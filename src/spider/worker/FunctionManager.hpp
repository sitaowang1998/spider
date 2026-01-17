#ifndef SPIDER_WORKER_FUNCTIONMANAGER_HPP
#define SPIDER_WORKER_FUNCTIONMANAGER_HPP

#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <fmt/format.h>

#include <spider/client/Data.hpp>
#include <spider/client/Receiver.hpp>
#include <spider/client/Sender.hpp>
#include <spider/client/task.hpp>
#include <spider/client/TaskContext.hpp>
#include <spider/core/DataImpl.hpp>
#include <spider/core/Error.hpp>
#include <spider/core/TaskContextImpl.hpp>
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/io/Serializer.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/worker/TaskExecutorMessage.hpp>

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define CONCAT_DIRECT(s1, s2) s1##s2
#define CONCAT(s1, s2) CONCAT_DIRECT(s1, s2)
#define ANONYMOUS_VARIABLE(str) CONCAT(str, __COUNTER__)
// NOLINTEND(cppcoreguidelines-macro-usage)

#define SPIDER_WORKER_REGISTER_TASK(func) \
    inline const auto ANONYMOUS_VARIABLE(var) \
            = spider::core::FunctionManager::get_instance().register_function(#func, func);

namespace spider::core {
using ArgsBuffer = msgpack::sbuffer;

using ResultBuffer = msgpack::sbuffer;

using Function = std::
        function<ResultBuffer(TaskContext& context, boost::uuids::uuid task_id, ArgsBuffer const&)>;

using FunctionMap = std::vector<std::pair<std::string, Function>>;

template <class T>
struct TemplateParameter;

template <template <class...> class t, class Param>
struct TemplateParameter<t<Param>> {
    using Type = Param;
};

template <class T>
using TemplateParameterT = typename TemplateParameter<T>::Type;

template <class Sig>
struct signature;

template <class R, class... Args>
struct signature<R(Args...)> {
    using args_t = std::tuple<std::decay_t<Args>...>;
    using ret_t = R;
};

template <class R, class... Args>
struct signature<R (*)(Args...)> {
    using args_t = std::tuple<std::decay_t<Args>...>;
    using ret_t = R;
};

enum class FunctionInvokeError : std::uint8_t {
    Success = 0,
    WrongNumberOfArguments = 1,
    ArgumentParsingError = 2,
    ResultParsingError = 3,
    FunctionExecutionError = 4,
};
}  // namespace spider::core

// MSGPACK_ADD_ENUM must be called from global namespace
MSGPACK_ADD_ENUM(spider::core::FunctionInvokeError);

namespace spider::core {
auto response_get_error(msgpack::sbuffer const& buffer)
        -> std::optional<std::tuple<FunctionInvokeError, std::string>>;

auto create_error_response(FunctionInvokeError error, std::string const& message)
        -> msgpack::sbuffer;

void create_error_buffer(
        FunctionInvokeError error,
        std::string const& message,
        msgpack::sbuffer& buffer
);

template <Serializable T>
auto response_get_result(msgpack::sbuffer const& buffer) -> std::optional<T> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::ARRAY != object.type || 2 != object.via.array.size) {
            return std::nullopt;
        }

        if (worker::TaskExecutorResponseType::Result
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            return std::nullopt;
        }

        if constexpr (cIsSpecializationV<T, spider::Data>) {
            static_assert("Not implemented");
            return std::make_optional(object.via.array.ptr[1].as<T>().get_id());
        } else {
            return std::make_optional(object.via.array.ptr[1].as<T>());
        }
    } catch (msgpack::type_error& e) {
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

template <Serializable... Ts>
requires(sizeof...(Ts) > 1)
auto response_get_result(msgpack::sbuffer const& buffer) -> std::optional<std::tuple<Ts...>> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::ARRAY != object.type || sizeof...(Ts) + 1 != object.via.array.size) {
            return std::nullopt;
        }

        if (worker::TaskExecutorResponseType::Result
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            return std::nullopt;
        }

        std::tuple<Ts...> result;
        for_n<sizeof...(Ts)>([&](auto i) {
            using T = std::tuple_element_t<i.cValue, std::tuple<Ts...>>;
            if constexpr (cIsSpecializationV<T, spider::Data>) {
                static_assert("Not implemented");
            }
            object.via.array.ptr[i.cValue + 1].convert(std::get<i.cValue>(result));
        });
        return std::make_optional(result);
    } catch (msgpack::type_error& e) {
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

auto response_get_result_buffers(msgpack::sbuffer const& buffer)
        -> std::optional<std::vector<msgpack::sbuffer>>;

/**
 * Extracts channel items from the response buffer.
 *
 * The response format supports optional channel items:
 *   [Result, result_values..., [[channel_id, value], ...]]
 *
 * @param buffer The response buffer to parse.
 * @return A vector of (channel_id, serialized_value) pairs, or empty if no channel items.
 */
auto response_get_channel_items(msgpack::sbuffer const& buffer)
        -> std::vector<std::pair<boost::uuids::uuid, std::string>>;

/**
 * Represents a channel item to be committed when a task succeeds.
 */
struct ChannelItemBuffer {
    boost::uuids::uuid channel_id;
    msgpack::sbuffer value;
};

/**
 * Extracts channel items from Sender arguments in a tuple.
 *
 * @tparam ArgsTuple The argument tuple type.
 * @param args The argument tuple after function execution.
 * @return A vector of channel items from all Sender arguments.
 */
template <class ArgsTuple>
auto extract_sender_channel_items(ArgsTuple& args) -> std::vector<ChannelItemBuffer> {
    std::vector<ChannelItemBuffer> items;
    for_n<std::tuple_size_v<ArgsTuple>>([&](auto i) {
        using T = std::tuple_element_t<i.cValue, ArgsTuple>;
        if constexpr (cIsSpecializationV<T, spider::Sender>) {
            auto& sender = std::get<i.cValue>(args);
            boost::uuids::uuid const channel_id = sender.get_channel_id();
            for (auto const& item : sender.get_buffered_items()) {
                ChannelItemBuffer ci;
                ci.channel_id = channel_id;
                msgpack::pack(ci.value, item);
                items.push_back(std::move(ci));
            }
        }
    });
    return items;
}

/**
 * Creates a result response.
 *
 * Response format: [Result, [result_values...], [channel_items...]]
 *
 * For backward compatibility with non-channel functions, this also supports:
 * [Result, result_value] (legacy format, no channel items)
 *
 * @param t The function's return value.
 * @return Serialized response buffer.
 */
template <TaskIo T>
auto create_result_response(T const& t) -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(2);
    packer.pack(worker::TaskExecutorResponseType::Result);
    if constexpr (cIsSpecializationV<T, spider::Data>) {
        packer.pack(DataImpl::get_impl(t)->get_id());
    } else {
        packer.pack(t);
    }
    return buffer;
}

/**
 * Creates a result response with channel items.
 *
 * Response format: [Result, [result_values...], [channel_items...]]
 *
 * @param t The function's return value.
 * @param channel_items Channel items from Sender buffers.
 * @return Serialized response buffer.
 */
template <TaskIo T>
auto create_result_response_with_channels(
        T const& t,
        std::vector<ChannelItemBuffer> const& channel_items
) -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};

    // Format: [Result, [result_values...], [[channel_id, value], ...]]
    packer.pack_array(3);
    packer.pack(worker::TaskExecutorResponseType::Result);

    // Pack result values as an array (single element for non-tuple)
    packer.pack_array(1);
    if constexpr (cIsSpecializationV<T, spider::Data>) {
        packer.pack(DataImpl::get_impl(t)->get_id());
    } else {
        packer.pack(t);
    }

    // Pack channel items: [[channel_id, value], ...]
    packer.pack_array(channel_items.size());
    for (auto const& item : channel_items) {
        packer.pack_array(2);
        packer.pack(item.channel_id);
        // Write the raw value buffer
        buffer.write(item.value.data(), item.value.size());
    }

    return buffer;
}

/**
 * Creates a result response for tuple return types.
 *
 * Response format: [Result, val1, val2, ...] (legacy format)
 */
template <TaskIo... Values>
auto create_result_response(std::tuple<Values...> const& t) -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(sizeof...(Values) + 1);
    packer.pack(worker::TaskExecutorResponseType::Result);
    for_n<sizeof...(Values)>([&](auto i) {
        using T = std::tuple_element_t<i.cValue, std::tuple<Values...>>;
        if constexpr (cIsSpecializationV<T, spider::Data>) {
            T const& data = std::get<i.cValue>(t);
            packer.pack(DataImpl::get_impl(data)->get_id());
        } else {
            packer.pack(std::get<i.cValue>(t));
        }
    });
    return buffer;
}

// NOLINTBEGIN(cppcoreguidelines-missing-std-forward)
template <class... Args>
auto create_args_buffers(Args&&... args) -> ArgsBuffer {
    ArgsBuffer args_buffer;
    msgpack::packer packer(args_buffer);
    packer.pack_array(sizeof...(args));
    ([&] { packer.pack(args); }(), ...);
    return args_buffer;
}

template <class... Args>
auto create_args_request(Args&&... args) -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(2);
    packer.pack(worker::TaskExecutorRequestType::Arguments);
    packer.pack_array(sizeof...(args));
    ([&] { packer.pack(args); }(), ...);
    return buffer;
}

inline auto create_args_request(std::vector<msgpack::sbuffer> const& args_buffers)
        -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(2);
    packer.pack(worker::TaskExecutorRequestType::Arguments);
    packer.pack_array(args_buffers.size());
    for (msgpack::sbuffer const& args_buffer : args_buffers) {
        buffer.write(args_buffer.data(), args_buffer.size());
    }
    return buffer;
}

// NOLINTEND(cppcoreguidelines-missing-std-forward)

template <class F>
class FunctionInvoker {
public:
    static auto apply(
            F const& function,
            TaskContext& context,
            boost::uuids::uuid const task_id,
            ArgsBuffer const& args_buffer
    ) -> ResultBuffer {
        // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
        using ArgsTuple = signature<F>::args_t;
        using ReturnType = signature<F>::ret_t;

        static_assert(TaskIo<ReturnType>, "Return type must be TaskIo");
        static_assert(
                std::is_same_v<TaskContext, std::tuple_element_t<0, ArgsTuple>>,
                "First argument must be TaskContext"
        );
        for_n<std::tuple_size_v<ArgsTuple> - 1>([&](auto i) {
            static_assert(
                    TaskIo<std::tuple_element_t<i.cValue + 1, ArgsTuple>>,
                    "Other arguments must be TaskIo"
            );
        });

        std::shared_ptr<DataStorage> data_store = TaskContextImpl::get_data_store(context);

        ArgsTuple args_tuple;
        try {
            msgpack::object_handle const handle
                    = msgpack::unpack(args_buffer.data(), args_buffer.size());
            msgpack::object const object = handle.get();

            if (msgpack::type::ARRAY != object.type || object.via.array.size < 1) {
                return create_error_response(
                        FunctionInvokeError::ArgumentParsingError,
                        fmt::format("Cannot parse arguments.")
                );
            }

            if (std::tuple_size_v<ArgsTuple> - 1 != object.via.array.size) {
                return create_error_response(
                        FunctionInvokeError::WrongNumberOfArguments,
                        fmt::format(
                                "Wrong number of arguments. Expect {}. Get {}.",
                                std::tuple_size_v<ArgsTuple>,
                                object.via.array.size
                        )
                );
            }

            // Fill args_tuple
            StorageErr err;
            std::get<0>(args_tuple) = context;
            std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
                    = TaskContextImpl::get_storage_factory(context)->provide_storage_connection();
            if (std::holds_alternative<core::StorageErr>(conn_result)) {
                err = std::get<core::StorageErr>(conn_result);
                return create_error_response(
                        FunctionInvokeError::ArgumentParsingError,
                        fmt::format("Cannot parse arguments: {}.", err.description)
                );
            }
            auto conn = std::move(std::get<std::unique_ptr<core::StorageConnection>>(conn_result));
            for_n<std::tuple_size_v<ArgsTuple> - 1>([&](auto i) {
                if (!err.success()) {
                    return;
                }
                using T = std::tuple_element_t<i.cValue + 1, ArgsTuple>;
                msgpack::object arg = object.via.array.ptr[i.cValue];
                if constexpr (cIsSpecializationV<T, spider::Data>) {
                    boost::uuids::uuid const data_id = arg.as<boost::uuids::uuid>();
                    std::unique_ptr<Data> data = std::make_unique<Data>();
                    err = data_store->get_task_data(*conn, task_id, data_id, data.get());
                    if (!err.success()) {
                        return;
                    }

                    std::get<i.cValue + 1>(args_tuple)
                            = DataImpl::create_data<TemplateParameterT<T>>(
                                    std::move(data),
                                    Context{Context::Source::Task, task_id},
                                    data_store,
                                    TaskContextImpl::get_storage_factory(context)
                            );
                } else if constexpr (cIsSpecializationV<T, spider::Sender>) {
                    boost::uuids::uuid const channel_id = arg.as<boost::uuids::uuid>();
                    std::get<i.cValue + 1>(args_tuple) = T{channel_id};
                } else if constexpr (cIsSpecializationV<T, spider::Receiver>) {
                    boost::uuids::uuid const channel_id = arg.as<boost::uuids::uuid>();
                    std::get<i.cValue + 1>(args_tuple)
                            = T{channel_id,
                                task_id,
                                TaskContextImpl::get_metadata_store(context),
                                TaskContextImpl::get_storage_factory(context)};
                } else {
                    std::get<i.cValue + 1>(args_tuple)
                            = arg.as<std::tuple_element_t<i.cValue + 1, ArgsTuple>>();
                }
            });
            if (!err.success()) {
                return create_error_response(
                        FunctionInvokeError::ArgumentParsingError,
                        fmt::format("Cannot parse arguments: {}.", err.description)
                );
            }
        } catch (msgpack::type_error& e) {
            return create_error_response(
                    FunctionInvokeError::ArgumentParsingError,
                    fmt::format("Cannot parse arguments.")
            );
        }

        try {
            ReturnType result = std::apply(function, args_tuple);

            // Extract channel items from Sender arguments
            std::vector<ChannelItemBuffer> channel_items = extract_sender_channel_items(args_tuple);

            if (channel_items.empty()) {
                return create_result_response(result);
            }
            return create_result_response_with_channels(result, channel_items);
        } catch (msgpack::type_error& e) {
            return create_error_response(
                    FunctionInvokeError::ResultParsingError,
                    fmt::format("Cannot parse result.")
            );
        } catch (std::exception& e) {
            return create_error_response(
                    FunctionInvokeError::FunctionExecutionError,
                    "Function execution error"
            );
        }
        // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }
};

class FunctionManager {
public:
    FunctionManager(FunctionManager const&) = delete;

    auto operator=(FunctionManager const&) -> FunctionManager& = delete;

    FunctionManager(FunctionManager&&) = delete;

    auto operator=(FunctionManager&&) -> FunctionManager& = delete;

    static auto get_instance() -> FunctionManager&;

    template <class F>
    auto register_function(std::string const& name, F f) -> bool {
        if (m_function_map.cend() != get(name)) {
            return false;
        }

        m_function_map.emplace_back(
                name,
                std::bind(
                        &FunctionInvoker<F>::apply,
                        std::move(f),
                        std::placeholders::_1,
                        std::placeholders::_2,
                        std::placeholders::_3
                )
        );
        return true;
    }

    auto register_function_invoker(std::string const& name, Function f) -> bool {
        if (m_function_map.cend() != get(name)) {
            return false;
        }
        m_function_map.emplace_back(name, std::move(f));
        return true;
    }

    [[nodiscard]] auto get_function(std::string const& name) const -> Function const*;

    [[nodiscard]] auto get_function_map() const -> FunctionMap const& { return m_function_map; }

private:
    [[nodiscard]] auto get(std::string_view name) const -> FunctionMap::const_iterator;

    FunctionManager() = default;

    ~FunctionManager() = default;

    FunctionMap m_function_map;
};
}  // namespace spider::core

#endif  // SPIDER_WORKER_FUNCTIONMANAGER_HPP
