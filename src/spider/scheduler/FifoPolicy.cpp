#include "FifoPolicy.hpp"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>

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
    std::optional<boost::uuids::uuid> next_task = pop_next_task(worker_addr);
    if (!next_task.has_value()) {
        size_t const num_tasks_before = m_tasks.size();
        fetch_tasks();
        if (m_tasks.size() == num_tasks_before) {
            return std::nullopt;
        }
        next_task = pop_next_task(worker_addr);
    }

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

    // Sort tasks based on job creation time in descending order, with channel consumers
    // prioritized.
    std::ranges::sort(
            m_tasks,
            [&](core::ScheduleTaskMetadata const& a, core::ScheduleTaskMetadata const& b) {
                // Primary: job creation time descending (earlier jobs at end, popped first)
                if (a.get_job_creation_time() != b.get_job_creation_time()) {
                    return a.get_job_creation_time() > b.get_job_creation_time();
                }
                // Secondary: channel consumers should be scheduled first within same job
                // Since we iterate in reverse, put consumers AFTER non-consumers
                // so they get popped first
                return a.is_channel_consumer() < b.is_channel_consumer();
            }
    );
}
}  // namespace spider::scheduler
