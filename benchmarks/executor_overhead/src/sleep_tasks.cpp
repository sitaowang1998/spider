#include "sleep_tasks.hpp"

#include <chrono>
#include <cstdint>
#include <thread>

#include <spdlog/spdlog.h>

#include <spider/client/spider.hpp>

namespace {
auto get_epoch_ms() -> int64_t {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
    ).count();
}
}  // namespace

auto cpp_sleep_10ms(spider::TaskContext& /*context*/, int task_id) -> int {
    auto const func_entry = get_epoch_ms();
    spdlog::info("[TIMING] task_id={} func=cpp_sleep_10ms func_entry={}", task_id, func_entry);

    // Sleep for 10ms
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto const func_exit = get_epoch_ms();
    spdlog::info(
            "[TIMING] task_id={} func=cpp_sleep_10ms func_entry={} func_exit={} "
            "func_duration_ms={}",
            task_id,
            func_entry,
            func_exit,
            func_exit - func_entry
    );

    return task_id;
}

// Register the task with Spider
// NOLINTNEXTLINE(cert-err58-cpp)
SPIDER_REGISTER_TASK(cpp_sleep_10ms);
