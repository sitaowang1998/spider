#include "FifoPolicy.hpp"

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <fmt/format.h>

#include "../core/Task.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/StorageConnection.hpp"

namespace spider::scheduler {

FifoPolicy::FifoPolicy(
        std::shared_ptr<core::MetadataStorage> const& metadata_store,
        std::shared_ptr<core::DataStorage> const& data_store,
        std::shared_ptr<core::StorageConnection> const& conn
)
        : m_metadata_store{metadata_store},
          m_data_store{data_store},
          m_conn{conn} {}

auto FifoPolicy::schedule_next(
        boost::uuids::uuid const /*worker_id*/,
        std::string const& worker_addr
) -> std::optional<boost::uuids::uuid> {
    if (m_tasks.empty()) {
        auto start_time = std::chrono::system_clock::now();
        fetch_tasks();
        if (m_tasks.empty()) {
            return std::nullopt;
        }
        auto end_time = std::chrono::system_clock::now();
        std::cerr << fmt::format(
                "Fetch task from {} to {}\n",
                std::chrono::duration_cast<std::chrono::milliseconds>(start_time.time_since_epoch())
                        .count(),
                std::chrono::duration_cast<std::chrono::milliseconds>(end_time.time_since_epoch())
                        .count()
        );
    }
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
    m_metadata_store->get_ready_tasks(*m_conn, &m_tasks);
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
