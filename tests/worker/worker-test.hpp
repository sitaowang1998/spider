#ifndef SPIDER_TEST_WORKER_TEST_HPP
#define SPIDER_TEST_WORKER_TEST_HPP

#include <string>
#include <tuple>

#include <spider/client/Data.hpp>
#include <spider/client/Receiver.hpp>
#include <spider/client/Sender.hpp>
#include <spider/client/TaskContext.hpp>

auto sum_test(spider::TaskContext& /*context*/, int x, int y) -> int;

auto swap_test(spider::TaskContext& /*context*/, int x, int y) -> std::tuple<int, int>;

auto error_test(spider::TaskContext& /*context*/, int /*x*/) -> int;

auto data_test(spider::TaskContext& /*context*/, spider::Data<int>& data) -> int;

auto random_fail_test(spider::TaskContext& /*context*/, int fail_rate) -> int;

auto create_data_test(spider::TaskContext& context, int x) -> spider::Data<int>;

auto create_task_test(spider::TaskContext& context, int x, int y) -> int;

auto join_string_test(
        spider::TaskContext& context,
        std::string const& input_1,
        std::string const& input_2
) -> std::string;

// Channel test functions - Senders are passed by reference, their items are extracted after
// execution
auto channel_producer_test(spider::TaskContext& context, spider::Sender<int>& sender, int count)
        -> int;

auto channel_consumer_test(spider::TaskContext& context, spider::Receiver<int>& receiver) -> int;

auto multi_channel_producer_test(
        spider::TaskContext& context,
        spider::Sender<int>& sender1,
        spider::Sender<int>& sender2,
        int count
) -> int;

auto channel_passthrough_test(
        spider::TaskContext& context,
        spider::Receiver<int>& receiver,
        spider::Sender<int>& sender
) -> int;

// Mixed output test functions - return both regular values and channel items from Sender arguments

/**
 * Takes a Sender argument and returns a tuple<int, int>.
 * Sends items through the Sender while returning two regular values.
 * Output order: [int, int, channel_items...]
 * Tests that regular return values come before channel items from Sender arguments.
 */
auto mixed_output_tuple_with_sender_test(
        spider::TaskContext& context,
        spider::Sender<int>& sender,
        int count
) -> std::tuple<int, int>;

/**
 * Takes two Sender arguments and returns an int.
 * Sends items to both Senders while returning a regular value.
 * Output order: [int, channel_items_1..., channel_items_2...]
 * Tests multiple Senders with a return value.
 */
auto mixed_output_multi_sender_test(
        spider::TaskContext& context,
        spider::Sender<int>& sender1,
        spider::Sender<int>& sender2,
        int count
) -> int;

#endif
