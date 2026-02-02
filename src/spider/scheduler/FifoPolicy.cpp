#include "FifoPolicy.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <spdlog/spdlog.h>

#include <spider/core/Task.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>

namespace spider::scheduler {
FifoPolicy::FifoPolicy(
        boost::uuids::uuid const scheduler_id,
        std::shared_ptr<core::MetadataStorage> const& metadata_store,
        std::shared_ptr<core::DataStorage> const& data_store,
        std::shared_ptr<core::StorageConnection> const& conn
)
        : m_scheduler_id{scheduler_id},
          m_metadata_store{metadata_store},
          m_data_store{data_store},
          m_conn{conn} {}

auto FifoPolicy::schedule_next(
        boost::uuids::uuid const /*worker_id*/,
        std::string const& worker_addr
) -> std::optional<boost::uuids::uuid> {
    auto const start = std::chrono::steady_clock::now();
    bool cache_hit = true;

    std::optional<boost::uuids::uuid> next_task = pop_next_task(worker_addr);
    if (!next_task.has_value()) {
        cache_hit = false;
        size_t const num_tasks_before = m_tasks.size();

        auto const fetch_start = std::chrono::steady_clock::now();
        fetch_tasks();
        auto const fetch_end = std::chrono::steady_clock::now();

        // Log fetch_tasks timing
        auto const epoch_ms = [](auto tp) {
            return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch())
                    .count();
        };
        spdlog::info(
                "[TIMING] scheduler_id={} fetch_tasks_start={} fetch_tasks_end={} "
                "fetch_tasks_duration_ms={} tasks_before={} tasks_after={}",
                boost::uuids::to_string(m_scheduler_id),
                epoch_ms(fetch_start),
                epoch_ms(fetch_end),
                std::chrono::duration_cast<std::chrono::milliseconds>(fetch_end - fetch_start)
                        .count(),
                num_tasks_before,
                m_tasks.size()
        );

        if (m_tasks.size() == num_tasks_before) {
            auto const end = std::chrono::steady_clock::now();
            spdlog::info(
                    "[TIMING] scheduler_id={} task_id=none schedule_next_start={} "
                    "schedule_next_end={} schedule_next_duration_ms={} cache_hit={}",
                    boost::uuids::to_string(m_scheduler_id),
                    epoch_ms(start),
                    epoch_ms(end),
                    std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
                    cache_hit
            );
            return std::nullopt;
        }
        next_task = pop_next_task(worker_addr);
    }

    auto const end = std::chrono::steady_clock::now();

    // Log schedule_next overall timing with task_id
    auto const epoch_ms = [](auto tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    };
    std::string const task_id_str
            = next_task.has_value() ? boost::uuids::to_string(next_task.value()) : "none";
    spdlog::info(
            "[TIMING] scheduler_id={} task_id={} schedule_next_start={} schedule_next_end={} "
            "schedule_next_duration_ms={} cache_hit={}",
            boost::uuids::to_string(m_scheduler_id),
            task_id_str,
            epoch_ms(start),
            epoch_ms(end),
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
            cache_hit
    );

    return next_task;
}

auto FifoPolicy::pop_next_task(std::string const& worker_addr)
        -> std::optional<boost::uuids::uuid> {
    auto const reverse_begin = std::reverse_iterator(m_tasks.end());
    auto const reverse_end = std::reverse_iterator(m_tasks.begin());
    auto const it
            = std::find_if(reverse_begin, reverse_end, [&](core::ScheduleTaskMetadata const& task) {
                  std::vector<std::string> const& hard_localities = task.get_hard_localities();
                  if (hard_localities.empty()) {
                      return true;
                  }
                  // If the worker address is in the hard localities, then the task can be
                  // scheduled.
                  return std::ranges::find(hard_localities, worker_addr) != hard_localities.end();
              });
    if (it == reverse_end) {
        return std::nullopt;
    }
    boost::uuids::uuid const task_id = it->get_id();
    m_tasks.erase(std::next(it).base());
    return task_id;
}

auto FifoPolicy::fetch_tasks() -> void {
    m_metadata_store->get_ready_tasks(*m_conn, m_scheduler_id, &m_tasks);
    m_metadata_store->get_task_timeout(*m_conn, &m_tasks);

    // Sort tasks based on job creation time in descending order.
    std::ranges::sort(
            m_tasks,
            [&](core::ScheduleTaskMetadata const& a, core::ScheduleTaskMetadata const& b) {
                return a.get_job_creation_time() > b.get_job_creation_time();
            }
    );
}
}  // namespace spider::scheduler
