#include "SchedulerServer.hpp"

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <spdlog/spdlog.h>

#include <spider/core/Error.hpp>
#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/io/msgpack_message.hpp>
#include <spider/io/Serializer.hpp>  // IWYU pragma: keep
#include <spider/scheduler/SchedulerMessage.hpp>
#include <spider/scheduler/SchedulerPolicy.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/utils/StopFlag.hpp>

namespace spider::scheduler {
SchedulerServer::SchedulerServer(
        unsigned short const port,
        std::shared_ptr<SchedulerPolicy> policy,
        std::shared_ptr<core::MetadataStorage> metadata_store,
        std::shared_ptr<core::DataStorage> data_store,
        std::shared_ptr<core::StorageConnection> conn
)
        : m_port{port},
          m_policy{std::move(policy)},
          m_metadata_store{std::move(metadata_store)},
          m_data_store{std::move(data_store)},
          m_conn{std::move(conn)} {
    boost::asio::co_spawn(m_context, receive_message(), boost::asio::detached);
    std::lock_guard const lock{m_mutex};
    m_thread = std::make_unique<std::thread>([&] { m_context.run(); });
}

auto SchedulerServer::pause() -> void {
    std::lock_guard const lock{m_mutex};
    if (m_thread == nullptr) {
        return;
    }
    m_context.stop();
    m_thread->join();
    m_thread = nullptr;
}

auto SchedulerServer::resume() -> void {
    std::lock_guard const lock{m_mutex};
    if (m_thread != nullptr) {
        return;
    }
    m_thread = std::make_unique<std::thread>([&] {
        m_context.restart();
        m_context.run();
    });
}

auto SchedulerServer::stop() -> void {
    std::lock_guard const lock{m_mutex};
    if (m_thread == nullptr) {
        return;
    }
    m_context.stop();
    m_thread->join();
    m_thread = nullptr;
}

auto SchedulerServer::receive_message() -> boost::asio::awaitable<void> {
    try {
        boost::asio::ip::tcp::acceptor acceptor{m_context, {boost::asio::ip::tcp::v4(), m_port}};
        while (true) {
            boost::asio::ip::tcp::socket socket{m_context};
            auto const& [ec] = co_await acceptor.async_accept(
                    socket,
                    boost::asio::as_tuple(boost::asio::use_awaitable)
            );
            if (ec) {
                spdlog::error("Cannot accept connection {}: {}", ec.value(), ec.what());
                continue;
            }
            boost::asio::co_spawn(
                    m_context,
                    process_message(std::move(socket)),
                    boost::asio::detached
            );
        }
        co_return;
    } catch (boost::system::system_error& e) {
        spdlog::error("Fail to accept connection: {}", e.what());
        spider::core::StopFlag::request_stop();
        co_return;
    }
}

namespace {
auto deserialize_message(msgpack::sbuffer const& buffer) -> std::optional<ScheduleTaskRequest> {
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();
        return object.as<ScheduleTaskRequest>();
    } catch (std::runtime_error& e) {
        spdlog::error("Cannot unpack message to ScheduleTaskRequest: {}", e.what());
        return std::nullopt;
    }
}
}  // namespace

auto SchedulerServer::process_message(boost::asio::ip::tcp::socket socket)
        -> boost::asio::awaitable<void> {
    auto const request_start = std::chrono::steady_clock::now();

    // Phase 1: Receive message
    // NOLINTBEGIN(clang-analyzer-core.CallAndMessage)
    std::optional<msgpack::sbuffer> const& optional_message_buffer
            = co_await core::receive_message_async(socket);
    // NOLINTEND(clang-analyzer-core.CallAndMessage)
    auto const receive_end = std::chrono::steady_clock::now();

    if (false == optional_message_buffer.has_value()) {
        spdlog::error("Cannot receive message from worker");
        co_return;
    }

    // Phase 2: Deserialize
    msgpack::sbuffer const& message_buffer = optional_message_buffer.value();
    std::optional<ScheduleTaskRequest> const& optional_request
            = deserialize_message(message_buffer);
    auto const deserialize_end = std::chrono::steady_clock::now();

    if (false == optional_request.has_value()) {
        spdlog::error("Cannot parse message into schedule task request");
        co_return;
    }
    ScheduleTaskRequest const& request = optional_request.value();

    // Reset the whole job if the task fails
    if (request.has_task_id()) {
        boost::uuids::uuid job_id;
        core::StorageErr err
                = m_metadata_store->get_task_job_id(*m_conn, request.get_task_id(), &job_id);
        // It is possible the job is deleted, so we don't need to reset it
        if (!err.success()) {
            spdlog::error(
                    "Cannot get job id for task {}",
                    boost::uuids::to_string(request.get_task_id())
            );
        } else {
            err = m_metadata_store->reset_job(*m_conn, job_id);
            if (!err.success()) {
                spdlog::error("Cannot reset job {}", boost::uuids::to_string(job_id));
                co_return;
            }
        }
    }

    // Phase 3: Schedule
    auto const schedule_start = std::chrono::steady_clock::now();
    std::optional<boost::uuids::uuid> const task_id
            = m_policy->schedule_next(request.get_worker_id(), request.get_worker_addr());
    auto const schedule_end = std::chrono::steady_clock::now();

    // Phase 4: Serialize response
    ScheduleTaskResponse response{};
    if (task_id.has_value()) {
        response = ScheduleTaskResponse{task_id.value()};
    }
    msgpack::sbuffer response_buffer;
    msgpack::pack(response_buffer, response);
    auto const serialize_end = std::chrono::steady_clock::now();

    // Phase 5: Send response
    bool const success = co_await core::send_message_async(socket, response_buffer);
    auto const send_end = std::chrono::steady_clock::now();

    // Log timing
    auto const epoch_ms = [](auto tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch())
                .count();
    };
    auto const duration_ms = [](auto start, auto end) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    };

    std::string const task_id_str
            = task_id.has_value() ? boost::uuids::to_string(task_id.value()) : "none";

    spdlog::info(
            "[TIMING] worker_id={} schedule_request receive_start={} receive_end={} "
            "receive_duration_ms={}",
            boost::uuids::to_string(request.get_worker_id()),
            epoch_ms(request_start),
            epoch_ms(receive_end),
            duration_ms(request_start, receive_end)
    );

    spdlog::info(
            "[TIMING] worker_id={} schedule_request deserialize_start={} deserialize_end={} "
            "deserialize_duration_ms={}",
            boost::uuids::to_string(request.get_worker_id()),
            epoch_ms(receive_end),
            epoch_ms(deserialize_end),
            duration_ms(receive_end, deserialize_end)
    );

    spdlog::info(
            "[TIMING] worker_id={} task_id={} schedule_request schedule_start={} schedule_end={} "
            "schedule_duration_ms={}",
            boost::uuids::to_string(request.get_worker_id()),
            task_id_str,
            epoch_ms(schedule_start),
            epoch_ms(schedule_end),
            duration_ms(schedule_start, schedule_end)
    );

    spdlog::info(
            "[TIMING] worker_id={} task_id={} schedule_request send_start={} send_end={} "
            "send_duration_ms={}",
            boost::uuids::to_string(request.get_worker_id()),
            task_id_str,
            epoch_ms(serialize_end),
            epoch_ms(send_end),
            duration_ms(serialize_end, send_end)
    );

    spdlog::info(
            "[TIMING] worker_id={} task_id={} schedule_request total_start={} total_end={} "
            "total_duration_ms={}",
            boost::uuids::to_string(request.get_worker_id()),
            task_id_str,
            epoch_ms(request_start),
            epoch_ms(send_end),
            duration_ms(request_start, send_end)
    );

    if (!success) {
        spdlog::error(
                "Cannot send message to worker {} at {}",
                boost::uuids::to_string(request.get_worker_id()),
                request.get_worker_addr()
        );
    }
    co_return;
}
}  // namespace spider::scheduler
