#ifndef SPIDER_CORE_RECEIVER_ACCESS_HPP
#define SPIDER_CORE_RECEIVER_ACCESS_HPP

#include <memory>
#include <utility>

#include <boost/uuid/uuid.hpp>

#include <spider/client/Receiver.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/StorageFactory.hpp>

namespace spider::core {
/**
 * Internal accessor for Receiver construction.
 * Used by FunctionManager to create Receivers for tasks.
 */
struct ReceiverAccess {
    template <class T>
    static auto
    create(boost::uuids::uuid channel_id,
           boost::uuids::uuid task_id,
           std::shared_ptr<MetadataStorage> metadata_store,
           std::shared_ptr<StorageFactory> storage_factory) -> Receiver<T> {
        return Receiver<T>{
                channel_id,
                task_id,
                std::move(metadata_store),
                std::move(storage_factory)
        };
    }
};
}  // namespace spider::core

#endif  // SPIDER_CORE_RECEIVER_ACCESS_HPP
