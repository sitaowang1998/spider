#ifndef SPIDER_CLIENT_SENDER_HPP
#define SPIDER_CLIENT_SENDER_HPP

#include <utility>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include <spider/client/task.hpp>
#include <spider/io/Serializer.hpp>

namespace spider {
// Forward declaration for internal access
namespace core {
struct SenderAccess;
}  // namespace core

/**
 * A sender handle for writing items to a channel.
 *
 * Items are buffered in memory and committed atomically when the task succeeds.
 * If the task fails, buffered items are discarded.
 *
 * @tparam T The type of items to send. Must satisfy Serializable and not be spider::Data.
 */
template <class T>
class Sender {
public:
    static_assert(Serializable<T>, "Sender item type must be Serializable.");
    static_assert(!cIsSpecializationV<T, spider::Data>, "Channels do not support spider::Data.");

    // Default constructor creates an invalid sender. Only use for internal purposes.
    Sender() = default;
    Sender(Sender const&) = delete;
    auto operator=(Sender const&) -> Sender& = delete;
    Sender(Sender&&) = default;
    auto operator=(Sender&&) -> Sender& = default;
    ~Sender() = default;

    /**
     * Buffers an item to be sent to the channel.
     * The item will be committed when the task successfully completes.
     * @param item The item to send.
     */
    auto send(T const& item) -> void { m_buffer.emplace_back(item); }

    /**
     * Buffers an item to be sent to the channel (move version).
     * @param item The item to send.
     */
    auto send(T&& item) -> void { m_buffer.emplace_back(std::move(item)); }

    /**
     * @return The number of buffered items.
     */
    [[nodiscard]] auto size() const -> std::size_t { return m_buffer.size(); }

    /**
     * @return True if no items are buffered.
     */
    [[nodiscard]] auto empty() const -> bool { return m_buffer.empty(); }

private:
    // Allow internal access to channel_id and buffer
    friend struct core::SenderAccess;

    /**
     * Creates a sender for the given channel.
     * @param channel_id The ID of the channel to send items to.
     */
    explicit Sender(boost::uuids::uuid channel_id) : m_channel_id{channel_id} {}

    boost::uuids::uuid m_channel_id;
    std::vector<T> m_buffer;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_SENDER_HPP
