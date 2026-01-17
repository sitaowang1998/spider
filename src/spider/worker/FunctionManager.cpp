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

auto response_get_result_buffers(msgpack::sbuffer const& buffer)
        -> std::optional<std::vector<msgpack::sbuffer>> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        std::vector<msgpack::sbuffer> result_buffers;
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::ARRAY != object.type || object.via.array.size < 2) {
            spdlog::error("Cannot split result into buffers: Wrong type");
            return std::nullopt;
        }

        if (worker::TaskExecutorResponseType::Result
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            spdlog::error(
                    "Cannot split result into buffers: Wrong response type {}",
                    static_cast<std::underlying_type_t<worker::TaskExecutorResponseType>>(
                            object.via.array.ptr[0].as<worker::TaskExecutorResponseType>()
                    )
            );
            return std::nullopt;
        }

        // Detect format:
        // - New format (with channels): [Result, [result_values...], [channel_items...]]
        //   Size is 3, both second and third elements are arrays
        // - Legacy format: [Result, val1, val2, ...]
        //   Size is >= 2, values after first are results
        bool const is_new_format = object.via.array.size == 3
                                   && msgpack::type::ARRAY == object.via.array.ptr[1].type
                                   && msgpack::type::ARRAY == object.via.array.ptr[2].type;

        if (is_new_format) {
            // New format: extract result values from the array at position 1
            msgpack::object const& results_array = object.via.array.ptr[1];
            for (size_t i = 0; i < results_array.via.array.size; ++i) {
                msgpack::object const& obj = results_array.via.array.ptr[i];
                result_buffers.emplace_back();
                msgpack::pack(result_buffers.back(), obj);
            }
        } else {
            // Legacy format: all elements after type are result values
            for (size_t i = 1; i < object.via.array.size; ++i) {
                msgpack::object const& obj = object.via.array.ptr[i];
                result_buffers.emplace_back();
                msgpack::pack(result_buffers.back(), obj);
            }
        }
        return result_buffers;
    } catch (msgpack::type_error& e) {
        spdlog::error("Cannot split result into buffers: {}", e.what());
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

auto response_get_channel_items(msgpack::sbuffer const& buffer)
        -> std::vector<std::pair<boost::uuids::uuid, std::string>> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    std::vector<std::pair<boost::uuids::uuid, std::string>> items;

    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        // New format with channels: [Result, [result_values...], [[channel_id, value], ...]]
        // Size must be 3, and both second and third elements must be arrays
        if (msgpack::type::ARRAY != object.type || object.via.array.size != 3) {
            return items;  // No channel items (legacy format)
        }

        if (worker::TaskExecutorResponseType::Result
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            return items;
        }

        // Check if this is the new format (both second and third are arrays)
        if (msgpack::type::ARRAY != object.via.array.ptr[1].type
            || msgpack::type::ARRAY != object.via.array.ptr[2].type)
        {
            return items;  // Legacy tuple format, no channel items
        }

        // The third element is the channel items array
        msgpack::object const& channel_items_obj = object.via.array.ptr[2];

        for (size_t i = 0; i < channel_items_obj.via.array.size; ++i) {
            msgpack::object const& item_obj = channel_items_obj.via.array.ptr[i];
            if (msgpack::type::ARRAY != item_obj.type || item_obj.via.array.size != 2) {
                continue;
            }

            boost::uuids::uuid const channel_id
                    = item_obj.via.array.ptr[0].as<boost::uuids::uuid>();
            msgpack::object const& value_obj = item_obj.via.array.ptr[1];

            // Serialize the value to a string
            msgpack::sbuffer value_buffer;
            msgpack::pack(value_buffer, value_obj);
            std::string const value{value_buffer.data(), value_buffer.size()};

            items.emplace_back(channel_id, value);
        }
    } catch (msgpack::type_error const& e) {
        spdlog::error("Cannot parse channel items: {}", e.what());
    }

    return items;
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
