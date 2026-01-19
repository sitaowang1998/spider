#include "FunctionManager.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/dll/alias.hpp>
#include <boost/uuid/uuid.hpp>
#include <spdlog/spdlog.h>

#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/worker/TaskExecutorMessage.hpp>

namespace spider::core {
auto response_get_error(msgpack::sbuffer const& buffer)
        -> std::optional<std::tuple<FunctionInvokeError, std::string>> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::ARRAY != object.type || 3 != object.via.array.size) {
            return std::nullopt;
        }

        if (worker::TaskExecutorResponseType::Error
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            return std::nullopt;
        }

        return std::make_tuple(
                object.via.array.ptr[1].as<FunctionInvokeError>(),
                object.via.array.ptr[2].as<std::string>()
        );
    } catch (msgpack::type_error& e) {
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

auto create_error_response(FunctionInvokeError error, std::string const& message)
        -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(3);
    packer.pack(worker::TaskExecutorResponseType::Error);
    packer.pack(error);
    packer.pack(message);
    return buffer;
}

void create_error_buffer(
        FunctionInvokeError error,
        std::string const& message,
        msgpack::sbuffer& buffer
) {
    msgpack::packer packer{buffer};
    packer.pack_array(2);
    packer.pack(error);
    packer.pack(message);
}

namespace {
/**
 * Checks if a msgpack object is a channel item: [channel_id, value]
 * Channel items are 2-element arrays where the first element is a 16-byte UUID.
 */
auto is_channel_item(msgpack::object const& obj) -> bool {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    if (msgpack::type::ARRAY != obj.type || obj.via.array.size != 2) {
        return false;
    }
    // Check if first element is a 16-byte binary (UUID)
    msgpack::object const& first = obj.via.array.ptr[0];
    constexpr size_t cUuidSize = 16;
    if (msgpack::type::BIN == first.type && first.via.bin.size == cUuidSize) {
        return true;
    }
    if (msgpack::type::EXT == first.type && first.via.ext.size == cUuidSize) {
        return true;
    }
    return false;
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}
}  // namespace

auto response_parse(msgpack::sbuffer const& buffer) -> std::optional<TaskExecutionResponse> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        TaskExecutionResponse response;
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::ARRAY != object.type || object.via.array.size < 2) {
            spdlog::error("Cannot parse response: Wrong type");
            return std::nullopt;
        }

        if (worker::TaskExecutorResponseType::Result
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            spdlog::error(
                    "Cannot parse response: Wrong response type {}",
                    static_cast<std::underlying_type_t<worker::TaskExecutorResponseType>>(
                            object.via.array.ptr[0].as<worker::TaskExecutorResponseType>()
                    )
            );
            return std::nullopt;
        }

        // Protocol: [Result, output1, output2, ...]
        // Outputs are in the order the task returns them.
        // Each output is either a regular value or a channel item [channel_id, value].
        // Channel items and regular values can be interleaved.
        for (size_t i = 1; i < object.via.array.size; ++i) {
            msgpack::object const& obj = object.via.array.ptr[i];

            if (is_channel_item(obj)) {
                // Extract as channel item
                boost::uuids::uuid const channel_id = obj.via.array.ptr[0].as<boost::uuids::uuid>();
                msgpack::object const& value_obj = obj.via.array.ptr[1];

                msgpack::sbuffer value_buffer;
                msgpack::pack(value_buffer, value_obj);
                std::string const value{value_buffer.data(), value_buffer.size()};

                response.outputs.emplace_back(
                        ChannelOutput{.channel_id = channel_id, .value = value}
                );
            } else {
                // Extract as regular value buffer
                msgpack::sbuffer result_buffer;
                msgpack::pack(result_buffer, obj);
                response.outputs.emplace_back(std::move(result_buffer));
            }
        }
        return response;
    } catch (msgpack::type_error& e) {
        spdlog::error("Cannot parse response: {}", e.what());
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

auto FunctionManager::get_instance() -> FunctionManager& {
    static FunctionManager instance;
    return instance;
}

auto FunctionManager::get(std::string_view name) const -> FunctionMap::const_iterator {
    for (auto it = m_function_map.cbegin(); it != m_function_map.cend(); ++it) {
        if (it->first == name) {
            return it;
        }
    }
    return m_function_map.cend();
}

auto FunctionManager::get_function(std::string const& name) const -> Function const* {
    auto const it = get(name);
    if (it == m_function_map.cend()) {
        return nullptr;
    }
    return &it->second;
}
}  // namespace spider::core

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
BOOST_DLL_ALIAS(spider::core::FunctionManager::get_instance, g_function_manager_get_instance)
