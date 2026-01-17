#ifndef SPIDER_CLIENT_RECEIVER_HPP
#define SPIDER_CLIENT_RECEIVER_HPP

#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <variant>

#include <boost/uuid/uuid.hpp>

#include <spider/client/Exception.hpp>
#include <spider/client/task.hpp>
#include <spider/core/Error.hpp>
#include <spider/core/Task.hpp>
#include <spider/io/MsgPack.hpp>
#include <spider/io/Serializer.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/storage/StorageFactory.hpp>

namespace spider {
/**
 * A receiver handle for reading items from a channel.
 *
 * @tparam T The type of items to receive. Must satisfy Serializable and not be spider::Data.
 */
template <class T>
class Receiver {
public:
    static_assert(Serializable<T>, "Receiver item type must be Serializable.");
    static_assert(!cIsSpecializationV<T, spider::Data>, "Channels do not support spider::Data.");

    /**
     * Default constructor for use in tuple initialization.
     * Creates an invalid receiver that must be assigned before use.
     */
    Receiver() = default;

    /**
     * Creates a receiver for the given channel.
     * @param channel_id The ID of the channel to receive items from.
     * @param task_id The ID of the consumer task.
     * @param metadata_store The metadata storage instance.
     * @param storage_factory The storage factory for creating connections.
     */
    Receiver(
            boost::uuids::uuid channel_id,
            boost::uuids::uuid task_id,
            std::shared_ptr<core::MetadataStorage> metadata_store,
            std::shared_ptr<core::StorageFactory> storage_factory
    )
            : m_channel_id{channel_id},
              m_task_id{task_id},
              m_metadata_store{std::move(metadata_store)},
              m_storage_factory{std::move(storage_factory)} {}

    Receiver(Receiver const&) = delete;
    auto operator=(Receiver const&) -> Receiver& = delete;
    Receiver(Receiver&&) = default;
    auto operator=(Receiver&&) -> Receiver& = default;
    ~Receiver() = default;

    /**
     * Receives an item from the channel with polling and timeout.
     *
     * @param timeout Maximum time to wait for an item.
     * @param poll_interval Time to sleep between polling attempts.
     * @return A pair (item, drained):
     *   - (item, false): An item was received
     *   - (nullopt, true): The channel is drained (sender closed and empty)
     *   - (nullopt, false): Timeout reached without receiving an item
     * @throw spider::ConnectionException if storage operations fail.
     */
    auto recv(
            std::chrono::milliseconds timeout = std::chrono::milliseconds(30'000),
            std::chrono::milliseconds poll_interval = std::chrono::milliseconds(100)
    ) -> std::pair<std::optional<T>, bool> {
        auto const start_time = std::chrono::steady_clock::now();

        while (true) {
            // Try to get a connection
            std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
                    = m_storage_factory->provide_storage_connection();
            if (std::holds_alternative<core::StorageErr>(conn_result)) {
                throw ConnectionException(std::get<core::StorageErr>(conn_result).description);
            }
            auto conn = std::move(std::get<std::unique_ptr<core::StorageConnection>>(conn_result));

            // Try to dequeue an item
            std::optional<core::ChannelItem> item;
            bool drained = false;
            core::StorageErr const err = m_metadata_store->dequeue_channel_item(
                    *conn,
                    m_channel_id,
                    m_task_id,
                    &item,
                    &drained
            );

            if (!err.success()) {
                throw ConnectionException(err.description);
            }

            // If we got an item, return it
            if (item.has_value()) {
                if (!item->value.has_value()) {
                    throw std::runtime_error("Channel item missing value.");
                }
                msgpack::object_handle const handle
                        = msgpack::unpack(item->value->data(), item->value->size());
                return {std::make_optional(handle.get().as<T>()), false};
            }

            // If channel is drained, return that
            if (drained) {
                return {std::nullopt, true};
            }

            // Check if we've exceeded the timeout
            auto const elapsed = std::chrono::steady_clock::now() - start_time;
            if (elapsed >= timeout) {
                return {std::nullopt, false};
            }

            // Sleep before next poll
            auto const remaining = timeout - elapsed;
            auto const sleep_time = std::min(
                    poll_interval,
                    std::chrono::duration_cast<std::chrono::milliseconds>(remaining)
            );
            std::this_thread::sleep_for(sleep_time);
        }
    }

    /**
     * @return The channel ID this receiver is bound to.
     */
    [[nodiscard]] auto get_channel_id() const -> boost::uuids::uuid { return m_channel_id; }

private:
    boost::uuids::uuid m_channel_id;
    boost::uuids::uuid m_task_id;
    std::shared_ptr<core::MetadataStorage> m_metadata_store;
    std::shared_ptr<core::StorageFactory> m_storage_factory;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_RECEIVER_HPP
