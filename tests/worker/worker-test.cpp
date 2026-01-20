#include "worker-test.hpp"

#include <iostream>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <variant>

#include <spider/client/Data.hpp>
#include <spider/client/Driver.hpp>
#include <spider/client/Job.hpp>
#include <spider/client/TaskContext.hpp>
#include <spider/client/TaskGraph.hpp>
#include <spider/core/Error.hpp>

auto sum_test(spider::TaskContext& /*context*/, int const x, int const y) -> int {
    std::cerr << x << " + " << y << " = " << x + y << "\n";
    return x + y;
}

auto swap_test(spider::TaskContext& /*context*/, int const x, int const y) -> std::tuple<int, int> {
    return std::make_tuple(y, x);
}

auto error_test(spider::TaskContext& /*context*/, int const /*x*/) -> int {
    throw std::runtime_error("Simulated error in worker");
}

auto data_test(spider::TaskContext& /*context*/, spider::Data<int>& data) -> int {
    return data.get();
}

auto random_fail_test(spider::TaskContext& /*context*/, int fail_rate) -> int {
    std::random_device rd;
    std::mt19937 gen{rd()};
    constexpr int cMaxFailRate = 100;
    std::uniform_int_distribution dis{1, cMaxFailRate};
    int const random_number = dis(gen);
    std::cerr << "Fail rate: " << fail_rate << "\n";
    std::cerr << "Random number: " << random_number << "\n";
    if (random_number < fail_rate) {
        throw std::runtime_error("Simulated error in worker");
    }
    return 0;
}

auto create_data_test(spider::TaskContext& context, int x) -> spider::Data<int> {
    spider::Data<int> data = context.get_data_builder<int>().build(x);
    return data;
}

auto create_task_test(spider::TaskContext& context, int x, int y) -> int {
    spider::TaskGraph const graph = context.bind(&sum_test, &sum_test, 0);
    std::cerr << "Create task test\n";
    spider::Job job = context.start(graph, x, y);
    std::cerr << "Job started\n";
    job.wait_complete();
    std::cerr << "Job completed\n";
    if (job.get_status() != spider::JobStatus::Succeeded) {
        std::cerr << "Job failed\n";
        throw std::runtime_error("Job failed");
    }
    return job.get_result();
}

auto join_string_test(
        spider::TaskContext& /*context*/,
        std::string const& input_1,
        std::string const& input_2
) -> std::string {
    return input_1 + input_2;
}

auto channel_producer_test(
        spider::TaskContext& /*context*/,
        spider::Sender<int>& sender,
        int const count
) -> int {
    std::cerr << "Producer: sending " << count << " items\n";
    for (int i = 0; i < count; ++i) {
        sender.send(i);
    }
    std::cerr << "Producer: sent " << count << " items\n";
    return count;
}

auto channel_consumer_test(spider::TaskContext& /*context*/, spider::Receiver<int>& receiver)
        -> int {
    std::cerr << "Consumer: starting to receive\n";
    int total = 0;
    while (true) {
        auto result = receiver.recv();
        if (result.has_error()) {
            std::cerr << "Consumer: storage error\n";
            throw std::runtime_error("Storage error in channel consumer");
        }
        auto [item, drained] = result.value();
        if (item.has_value()) {
            total += item.value();
            std::cerr << "Consumer: received " << item.value() << ", total = " << total << "\n";
        }
        if (drained) {
            std::cerr << "Consumer: channel drained\n";
            break;
        }
    }
    std::cerr << "Consumer: finished with total = " << total << "\n";
    return total;
}

auto multi_channel_producer_test(
        spider::TaskContext& /*context*/,
        spider::Sender<int>& sender1,
        spider::Sender<int>& sender2,
        int const count
) -> int {
    std::cerr << "MultiProducer: sending " << count << " items to each channel\n";
    for (int i = 0; i < count; ++i) {
        sender1.send(i);
        sender2.send(i * 2);
    }
    std::cerr << "MultiProducer: done\n";
    return count;
}

auto channel_passthrough_test(
        spider::TaskContext& /*context*/,
        spider::Receiver<int>& receiver,
        spider::Sender<int>& sender
) -> int {
    std::cerr << "Passthrough: starting\n";
    int count = 0;
    while (true) {
        auto result = receiver.recv();
        if (result.has_error()) {
            std::cerr << "Passthrough: storage error\n";
            throw std::runtime_error("Storage error in passthrough");
        }
        auto [item, drained] = result.value();
        if (item.has_value()) {
            sender.send(item.value() * 2);
            count++;
            std::cerr << "Passthrough: forwarded " << item.value() << " as " << item.value() * 2
                      << "\n";
        }
        if (drained) {
            std::cerr << "Passthrough: done, forwarded " << count << " items\n";
            break;
        }
    }
    return count;
}

auto mixed_output_tuple_with_sender_test(
        spider::TaskContext& /*context*/,
        spider::Sender<int>& sender,
        int const count
) -> std::tuple<int, int> {
    std::cerr << "MixedTupleWithSender: sending " << count << " items through sender\n";
    int sum = 0;
    for (int i = 0; i < count; ++i) {
        sender.send(i);
        sum += i;
    }
    std::cerr << "MixedTupleWithSender: returning tuple(" << count << ", " << sum << ")\n";
    return {count, sum};
}

auto mixed_output_multi_sender_test(
        spider::TaskContext& /*context*/,
        spider::Sender<int>& sender1,
        spider::Sender<int>& sender2,
        int const count
) -> int {
    std::cerr << "MixedMultiSender: sending " << count << " items to each sender\n";
    for (int i = 0; i < count; ++i) {
        sender1.send(i);
        sender2.send(i * 2);
    }
    std::cerr << "MixedMultiSender: returning count=" << count << "\n";
    return count;
}

// NOLINTBEGIN(cert-err58-cpp)
SPIDER_REGISTER_TASK(sum_test);
SPIDER_REGISTER_TASK(swap_test);
SPIDER_REGISTER_TASK(error_test);
SPIDER_REGISTER_TASK(data_test);
SPIDER_REGISTER_TASK(random_fail_test);
SPIDER_REGISTER_TASK(create_data_test);
SPIDER_REGISTER_TASK(create_task_test);
SPIDER_REGISTER_TASK(join_string_test);
SPIDER_REGISTER_TASK(channel_producer_test);
SPIDER_REGISTER_TASK(channel_consumer_test);
SPIDER_REGISTER_TASK(multi_channel_producer_test);
SPIDER_REGISTER_TASK(channel_passthrough_test);
SPIDER_REGISTER_TASK(mixed_output_tuple_with_sender_test);
SPIDER_REGISTER_TASK(mixed_output_multi_sender_test);
// NOLINTEND(cert-err58-cpp)
