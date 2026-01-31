#ifndef SPIDER_BENCHMARK_SLEEP_TASKS_HPP
#define SPIDER_BENCHMARK_SLEEP_TASKS_HPP

#include <spider/client/TaskContext.hpp>

/**
 * A simple sleep task for benchmarking executor overhead.
 * Sleeps for 10ms and logs timing information.
 * @param context The task context.
 * @param task_id An identifier for this task instance.
 * @return The task_id that was passed in.
 */
auto cpp_sleep_10ms(spider::TaskContext& context, int task_id) -> int;

#endif  // SPIDER_BENCHMARK_SLEEP_TASKS_HPP
