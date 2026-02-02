#include "WorkerClient.hpp"

#include <algorithm>
#include <chrono>
#include <iterator>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <spdlog/spdlog.h>

#include <spider/core/Driver.hpp>
#include <spider/core/Error.hpp>
#include <spider/core/Task.hpp>
#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/io/msgpack_message.hpp>
#include <spider/scheduler/SchedulerMessage.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/storage/StorageFactory.hpp>

namespace spider::worker {
namespace {
/**
 * Resolves hostname and port to a list of TCP endpoints.
 *
 * @param context Boost ASIO context
 * @param hostname
 * @param port
 * @return A vector of resolved TCP endpoints. Empty if resolution fails.
 */
[[nodiscard]] auto
resolve_hostname(boost::asio::io_context& context, std::string_view hostname, int port)
        -> std::vector<boost::asio::ip::tcp::endpoint>;

auto resolve_hostname(boost::asio::io_context& context, std::string_view const hostname, int port)
        -> std::vector<boost::asio::ip::tcp::endpoint> {
    try {
        boost::asio::ip::tcp::resolver resolver{context};
        auto const results{resolver.resolve(hostname, fmt::format("{}", port))};
        std::vector<boost::asio::ip::tcp::endpoint> endpoints;
        std::ranges::copy(results, std::back_inserter(endpoints));
        return endpoints;
    } catch (boost::system::system_error const& e) {
        spdlog::warn("Failed to resolve hostname {}:{}: {}.", hostname, port, e.what());
        return {};
    }
}
}  // namespace

WorkerClient::WorkerClient(
        boost::uuids::uuid const worker_id,
        std::string worker_addr,
        std::shared_ptr<core::DataStorage> data_store,
        std::shared_ptr<core::MetadataStorage> metadata_store,
        std::shared_ptr<core::StorageFactory> storage_factory
)
        : m_worker_id{worker_id},
          m_worker_addr{std::move(worker_addr)},
          m_data_store(std::move(data_store)),
          m_metadata_store(std::move(metadata_store)),
          m_storage_factory(std::move(storage_factory)) {}

auto WorkerClient::get_next_task(std::optional<boost::uuids::uuid> const& fail_task_id)
        -> std::optional<std::tuple<boost::uuids::uuid, boost::uuids::uuid>> {
    // Get schedulers
    std::vector<core::Scheduler> schedulers;

    {  // Keep the scope for RAII storage connection
        std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
                = m_storage_factory->provide_storage_connection();
        if (std::holds_alternative<core::StorageErr>(conn_result)) {
            spdlog::error(
                    "Failed to connect to storage: {}",
                    std::get<core::StorageErr>(conn_result).description
            );
            return std::nullopt;
        }
        auto conn = std::move(std::get<std::unique_ptr<core::StorageConnection>>(conn_result));
        if (!m_metadata_store->get_active_scheduler(*conn, &schedulers).success()) {
            return std::nullopt;
        }
    }
    if (schedulers.empty()) {
        return std::nullopt;
    }

    std::random_device random_device;
    std::default_random_engine rng{random_device()};
    std::ranges::shuffle(schedulers, rng);

    try {
        auto const request_start = std::chrono::steady_clock::now();

        // Create socket to scheduler
        boost::asio::io_context context;
        boost::asio::ip::tcp::socket socket(context);

        std::vector<boost::asio::ip::tcp::endpoint> endpoints;
        for (auto const& scheduler : schedulers) {
            auto const resolved_endpoints{
                    resolve_hostname(context, scheduler.get_addr(), scheduler.get_port())
            };
            endpoints.insert(
                    endpoints.cend(),
                    resolved_endpoints.cbegin(),
                    resolved_endpoints.cend()
            );
        }
        if (endpoints.empty()) {
            spdlog::error("Failed to resolve any scheduler addresses.");
            return std::nullopt;
        }

        boost::asio::connect(socket, endpoints);
        auto const connect_end = std::chrono::steady_clock::now();

        scheduler::ScheduleTaskRequest request{m_worker_id, m_worker_addr};
        if (fail_task_id.has_value()) {
            request = scheduler::ScheduleTaskRequest{
                    m_worker_id,
                    m_worker_addr,
                    fail_task_id.value()
            };
        }
        msgpack::sbuffer request_buffer;
        msgpack::pack(request_buffer, request);

        core::send_message(socket, request_buffer);
        auto const send_end = std::chrono::steady_clock::now();

        // Receive response
        std::optional<msgpack::sbuffer> const optional_response_buffer
                = core::receive_message(socket);
        auto const receive_end = std::chrono::steady_clock::now();

        if (!optional_response_buffer.has_value()) {
            return std::nullopt;
        }
        msgpack::sbuffer const& response_buffer = optional_response_buffer.value();

        scheduler::ScheduleTaskResponse response;
        msgpack::object_handle const response_handle
                = msgpack::unpack(response_buffer.data(), response_buffer.size());
        response_handle.get().convert(response);

        auto const epoch_ms = [](auto tp) {
            return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch())
                    .count();
        };

        if (!response.has_task_id()) {
            spdlog::info(
                    "[TIMING] worker_id={} task_id=none get_next_task "
                    "request_start={} connect_end={} send_end={} receive_end={} "
                    "connect_duration_ms={} send_duration_ms={} receive_duration_ms={} "
                    "total_duration_ms={}",
                    boost::uuids::to_string(m_worker_id),
                    epoch_ms(request_start),
                    epoch_ms(connect_end),
                    epoch_ms(send_end),
                    epoch_ms(receive_end),
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                            connect_end - request_start
                    )
                            .count(),
                    std::chrono::duration_cast<std::chrono::milliseconds>(send_end - connect_end)
                            .count(),
                    std::chrono::duration_cast<std::chrono::milliseconds>(receive_end - send_end)
                            .count(),
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                            receive_end - request_start
                    )
                            .count()
            );
            return std::nullopt;
        }
        boost::uuids::uuid const task_id = response.get_task_id();

        std::variant<std::unique_ptr<core::StorageConnection>, core::StorageErr> conn_result
                = m_storage_factory->provide_storage_connection();
        if (std::holds_alternative<core::StorageErr>(conn_result)) {
            spdlog::error(
                    "Failed to connect to storage: {}",
                    std::get<core::StorageErr>(conn_result).description
            );
            return std::nullopt;
        }
        auto conn = std::move(std::get<std::unique_ptr<core::StorageConnection>>(conn_result));

        core::TaskInstance const instance{task_id};
        core::StorageErr const err = m_metadata_store->create_task_instance(*conn, instance);
        if (!err.success()) {
            return std::nullopt;
        }

        spdlog::info(
                "[TIMING] worker_id={} task_id={} get_next_task "
                "request_start={} connect_end={} send_end={} receive_end={} "
                "connect_duration_ms={} send_duration_ms={} receive_duration_ms={} "
                "total_duration_ms={}",
                boost::uuids::to_string(m_worker_id),
                boost::uuids::to_string(task_id),
                epoch_ms(request_start),
                epoch_ms(connect_end),
                epoch_ms(send_end),
                epoch_ms(receive_end),
                std::chrono::duration_cast<std::chrono::milliseconds>(connect_end - request_start)
                        .count(),
                std::chrono::duration_cast<std::chrono::milliseconds>(send_end - connect_end)
                        .count(),
                std::chrono::duration_cast<std::chrono::milliseconds>(receive_end - send_end)
                        .count(),
                std::chrono::duration_cast<std::chrono::milliseconds>(receive_end - request_start)
                        .count()
        );

        return std::make_tuple(task_id, instance.id);
    } catch (boost::system::system_error const& e) {
        return std::nullopt;
    } catch (std::runtime_error const& e) {
        return std::nullopt;
    }
}
}  // namespace spider::worker
