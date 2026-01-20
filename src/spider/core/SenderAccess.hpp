#ifndef SPIDER_CORE_SENDER_ACCESS_HPP
#define SPIDER_CORE_SENDER_ACCESS_HPP

#include <vector>

#include <boost/uuid/uuid.hpp>

#include <spider/client/Sender.hpp>

namespace spider::core {
/**
 * Internal accessor for Sender private members.
 * Used by FunctionManager to create and extract channel items from Senders.
 */
struct SenderAccess {
    template <class T>
    static auto create(boost::uuids::uuid channel_id) -> Sender<T> {
        return Sender<T>{channel_id};
    }

    template <class T>
    static auto get_channel_id(Sender<T> const& sender) -> boost::uuids::uuid {
        return sender.m_channel_id;
    }

    template <class T>
    static auto get_buffered_items(Sender<T> const& sender) -> std::vector<T> const& {
        return sender.m_buffer;
    }

    template <class T>
    static auto clear(Sender<T>& sender) -> void {
        sender.m_buffer.clear();
    }
};
}  // namespace spider::core

#endif  // SPIDER_CORE_SENDER_ACCESS_HPP
