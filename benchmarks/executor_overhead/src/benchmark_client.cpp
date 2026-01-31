#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <spider/client/spider.hpp>

#include "sleep_tasks.hpp"

namespace {
constexpr int cNumTasks = 160;
}  // namespace

// NOLINTBEGIN(bugprone-exception-escape)
auto main(int argc, char const* argv[]) -> int {
    if (argc < 2) {
        std::cerr << "Usage: ./benchmark_client <storage-backend-url> [task_type]" << '\n';
        std::cerr << "  task_type: 'cpp' (default) or 'python'" << '\n';
        return 1;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    std::string const storage_url{argv[1]};
    if (storage_url.empty()) {
        std::cerr << "storage-backend-url cannot be empty." << '\n';
        return 1;
    }

    std::string task_type = "cpp";
    if (argc >= 3) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        task_type = argv[2];
    }

    std::cout << "Starting executor overhead benchmark\n";
    std::cout << "  Storage URL: " << storage_url << '\n';
    std::cout << "  Task type: " << task_type << '\n';
    std::cout << "  Number of tasks: " << cNumTasks << '\n';

    // Create a driver that connects to the Spider cluster
    spider::Driver driver{storage_url};

    std::vector<spider::Job<int>> jobs;
    jobs.reserve(cNumTasks);

    auto const start_time = std::chrono::steady_clock::now();

    // Submit all tasks using batch mode
    driver.begin_batch_start();
    for (int i = 0; i < cNumTasks; ++i) {
        // Note: For Python tasks, the Python benchmark client should be used instead.
        // This client only supports C++ tasks.
        jobs.push_back(driver.start(&cpp_sleep_10ms, i));
    }
    driver.end_batch_start();

    auto const submit_time = std::chrono::steady_clock::now();
    auto const submit_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            submit_time - start_time
    ).count();
    std::cout << "All " << cNumTasks << " tasks submitted in " << submit_duration_ms << " ms\n";

    // Wait for all jobs to complete
    int succeeded = 0;
    int failed = 0;
    for (auto& job : jobs) {
        job.wait_complete();
        if (spider::JobStatus::Succeeded == job.get_status()) {
            ++succeeded;
        } else {
            ++failed;
        }
    }

    auto const end_time = std::chrono::steady_clock::now();
    auto const total_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time
    ).count();
    auto const execution_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - submit_time
    ).count();

    std::cout << "\n=== Benchmark Results ===\n";
    std::cout << "Task type: " << task_type << '\n';
    std::cout << "Total tasks: " << cNumTasks << '\n';
    std::cout << "Succeeded: " << succeeded << '\n';
    std::cout << "Failed: " << failed << '\n';
    std::cout << "Submit time: " << submit_duration_ms << " ms\n";
    std::cout << "Execution time: " << execution_duration_ms << " ms\n";
    std::cout << "Total time: " << total_duration_ms << " ms\n";
    std::cout << "Average per-task time: "
              << (static_cast<double>(total_duration_ms) / static_cast<double>(cNumTasks)) << " ms\n";

    if (failed > 0) {
        return 1;
    }
    return 0;
}
// NOLINTEND(bugprone-exception-escape)
