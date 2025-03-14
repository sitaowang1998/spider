#include "RedisConnection.hpp"

#include <string>

#include <redis++/connection.h>
#include <redis++/redis++.h>

namespace spider::core {

RedisConnection::RedisConnection(std::string const& host, int port, std::string const& password) {
    sw::redis::ConnectionOptions connection_options;
    connection_options.host = host;
    connection_options.port = port;
    connection_options.password = password;
    connection_options.resp = 3;

    m_connection = std::make_unique<sw::redis::Redis>(
            connection_options,
            sw::redis::ConnectionPoolOptions{}
    );
}

auto RedisConnection::operator*() -> sw::redis::Redis& {
    return *m_connection;
}

auto RedisConnection::operator->() -> sw::redis::Redis* {
    return &*m_connection;
}

}  // namespace spider::core
