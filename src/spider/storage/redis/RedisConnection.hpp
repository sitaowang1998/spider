#ifndef SPIDER_STORAGE_REDISCONNECTION_HPP
#define SPIDER_STORAGE_REDISCONNECTION_HPP

#include <memory>
#include <string>

#include <redis++/redis++.h>

#include "../StorageConnection.hpp"

namespace spider::core {
class RedisConnection : public StorageConnection {
public:
    RedisConnection(std::string const& host, int port, std::string const& password);

    // Delete copy constructor and copy assignment operator
    RedisConnection(RedisConnection const&) = delete;
    RedisConnection& operator=(RedisConnection const&) = delete;
    // Default move constructor and move assignment operator
    RedisConnection(RedisConnection&&) = default;
    RedisConnection& operator=(RedisConnection&&) = default;

    ~RedisConnection() = default;

    auto operator*() -> sw::redis::Redis&;
    auto operator->() -> sw::redis::Redis*;

private:
    std::unique_ptr<sw::redis::Redis> m_connection;
};
}  // namespace spider::core

#endif
