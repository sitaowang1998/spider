#include "MySqlConnection.hpp"

#include <memory>
#include <regex>
#include <string>
#include <utility>
#include <variant>

#include <mariadb/conncpp/Connection.hpp>
#include <mariadb/conncpp/DriverManager.hpp>
#include <mariadb/conncpp/Exception.hpp>
#include <mariadb/conncpp/Properties.hpp>
#include <spdlog/spdlog.h>

#include <spider/core/Error.hpp>
#include <spider/storage/StorageConnection.hpp>

namespace spider::core {
auto MySqlConnection::create(std::string const& url)
        -> std::variant<std::unique_ptr<StorageConnection>, StorageErr> {
    // Validate jdbc url
    std::regex const url_regex(R"(jdbc:mariadb://[^?]+(\?user=([^&]*)(&password=([^&]*))?)?)");
    std::smatch match;
    if (false == std::regex_match(url, match, url_regex)) {
        return StorageErr{StorageErrType::OtherErr, "Invalid url"};
    }
    try {
        sql::Properties const properties{{{"useBulkStmts", "true"}}};
        std::unique_ptr<sql::Connection> conn{sql::DriverManager::getConnection(url, properties)};
        conn->setAutoCommit(false);
        return std::unique_ptr<StorageConnection>(new MySqlConnection{std::move(conn)});
    } catch (sql::SQLException& e) {
        return StorageErr{StorageErrType::ConnectionErr, e.what()};
    }
}

MySqlConnection::~MySqlConnection() {
    if (m_connection) {
        try {
            m_connection->close();
        } catch (sql::SQLException& e) {
            spdlog::warn("Failed to close connection: {}", e.what());
        }
        m_connection.reset();
    }
}

auto MySqlConnection::operator*() const -> sql::Connection& {
    return *m_connection;
}

auto MySqlConnection::operator->() const -> sql::Connection* {
    return &*m_connection;
}
}  // namespace spider::core
