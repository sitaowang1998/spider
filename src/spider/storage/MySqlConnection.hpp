#ifndef SPIDER_STORAGE_MYSQLCONNECTION_HPP
#define SPIDER_STORAGE_MYSQLCONNECTION_HPP

#include <memory>
#include <string>
#include <utility>
#include <variant>

#include <mariadb/conncpp/Connection.hpp>

#include "../core/Error.hpp"

namespace spider::core {

// RAII class for MySQL connection
class MySqlConnection {
public:
    static auto create(std::string const& url) -> std::variant<MySqlConnection, StorageErr>;

    // Delete copy constructor and copy assignment operator
    MySqlConnection(MySqlConnection const&) = delete;
    auto operator=(MySqlConnection const&) -> MySqlConnection& = delete;
    // Default move constructor and move assignment operator
    MySqlConnection(MySqlConnection&&) = default;
    auto operator=(MySqlConnection&&) -> MySqlConnection& = default;

    ~MySqlConnection();

    auto operator*() const -> sql::Connection&;
    auto operator->() const -> sql::Connection*;

private:
    explicit MySqlConnection(std::unique_ptr<sql::Connection> conn)
            : m_connection{std::move(conn)} {};
    std::unique_ptr<sql::Connection> m_connection;
};

}  // namespace spider::core

#endif