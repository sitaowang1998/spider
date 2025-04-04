#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/any/bad_any_cast.hpp>
#include <boost/dll/runtime_symbol_info.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>
#include <spdlog/sinks/stdout_color_sinks.h>  // IWYU pragma: keep
#include <spdlog/spdlog.h>

#include "../core/Data.hpp"
#include "../core/Driver.hpp"
#include "../core/Error.hpp"
#include "../core/Task.hpp"
#include "../io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/Serializer.hpp"  // IWYU pragma: keep
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"
#include "../storage/mysql/MySqlStorageFactory.hpp"
#include "../storage/StorageConnection.hpp"
#include "../storage/StorageFactory.hpp"
#include "../utils/StopToken.hpp"
#include "TaskExecutor.hpp"
#include "WorkerClient.hpp"

constexpr int cCmdArgParseErr = 1;
constexpr int cWorkerAddrErr = 2;
constexpr int cStorageConnectionErr = 3;
constexpr int cStorageErr = 4;
constexpr int cTaskErr = 5;

constexpr int cRetryCount = 5;

namespace {
auto parse_args(int const argc, char** argv) -> boost::program_options::variables_map {
    boost::program_options::options_description desc;
    desc.add_options()("help", "spider scheduler");
    desc.add_options()(
            "storage_url",
            boost::program_options::value<std::string>(),
            "storage server url"
    );
    desc.add_options()(
            "libs",
            boost::program_options::value<std::vector<std::string>>(),
            "dynamic libraries that include the spider tasks"
    );
    desc.add_options()("host", boost::program_options::value<std::string>(), "worker host address");

    boost::program_options::variables_map variables;
    boost::program_options::store(
            // NOLINTNEXTLINE(misc-include-cleaner)
            boost::program_options::parse_command_line(argc, argv, desc),
            variables
    );
    boost::program_options::notify(variables);
    return variables;
}

auto get_environment_variable() -> absl::flat_hash_map<
        boost::process::v2::environment::key,
        boost::process::v2::environment::value> {
    boost::filesystem::path const executable_dir = boost::dll::program_location().parent_path();

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    char const* path_env_str = std::getenv("PATH");
    std::string path_env = nullptr == path_env_str ? "" : path_env_str;
    path_env.append(":");
    path_env.append(executable_dir.string());

    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value>
            environment_variables;

    environment_variables.emplace("PATH", path_env);

    return environment_variables;
}

auto heartbeat_loop(
        std::shared_ptr<spider::core::StorageFactory> const& storage_factory,
        std::shared_ptr<spider::core::MetadataStorage> const& metadata_store,
        spider::core::Driver const& driver,
        spider::core::StopToken& stop_token
) -> void {
    int fail_count = 0;
    while (!stop_token.stop_requested()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        spdlog::debug("Updating heartbeat");
        std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
                conn_result = storage_factory->provide_storage_connection();
        if (std::holds_alternative<spider::core::StorageErr>(conn_result)) {
            spdlog::error(
                    "Failed to connect to storage: {}",
                    std::get<spider::core::StorageErr>(conn_result).description
            );
            fail_count++;
            continue;
        }
        auto conn = std::move(
                std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result)
        );

        spider::core::StorageErr const err
                = metadata_store->update_heartbeat(*conn, driver.get_id());
        if (!err.success()) {
            spdlog::error("Failed to update scheduler heartbeat: {}", err.description);
            fail_count++;
        } else {
            fail_count = 0;
        }
        if (fail_count >= cRetryCount - 1) {
            stop_token.request_stop();
            break;
        }
    }
}

constexpr int cFetchTaskTimeout = 100;

auto
fetch_task(spider::worker::WorkerClient& client, std::optional<boost::uuids::uuid> fail_task_id)
        -> std::tuple<boost::uuids::uuid, boost::uuids::uuid> {
    spdlog::debug("Fetching task");
    while (true) {
        std::optional<std::tuple<boost::uuids::uuid, boost::uuids::uuid>> const optional_task_ids
                = client.get_next_task(fail_task_id);
        if (optional_task_ids.has_value()) {
            return optional_task_ids.value();
        }
        // If the first request succeeds, later requests should not include the failed task id
        fail_task_id = std::nullopt;
        std::this_thread::sleep_for(std::chrono::milliseconds(cFetchTaskTimeout));
    }
}

auto get_args_buffers(spider::core::Task const& task)
        -> std::optional<std::vector<msgpack::sbuffer>> {
    std::vector<msgpack::sbuffer> args_buffers;
    size_t const num_inputs = task.get_num_inputs();
    for (size_t i = 0; i < num_inputs; ++i) {
        spider::core::TaskInput const& input = task.get_input(i);
        std::optional<std::string> const optional_value = input.get_value();
        if (optional_value.has_value()) {
            std::string const& value = optional_value.value();
            args_buffers.emplace_back();
            args_buffers.back().write(value.data(), value.size());
            continue;
        }
        std::optional<boost::uuids::uuid> const optional_data_id = input.get_data_id();
        if (optional_data_id.has_value()) {
            boost::uuids::uuid const data_id = optional_data_id.value();
            args_buffers.emplace_back();
            msgpack::pack(args_buffers.back(), data_id);
            continue;
        }
        spdlog::error(
                "Task {} {} input {} has no value or data id",
                task.get_function_name(),
                boost::uuids::to_string(task.get_id()),
                i
        );
        return std::nullopt;
    }
    return args_buffers;
}

auto
parse_outputs(spider::core::Task const& task, std::vector<msgpack::sbuffer> const& result_buffers)
        -> std::optional<std::vector<spider::core::TaskOutput>> {
    std::vector<spider::core::TaskOutput> outputs;
    outputs.reserve(task.get_num_outputs());
    for (size_t i = 0; i < task.get_num_outputs(); ++i) {
        std::string const type = task.get_output(i).get_type();
        if (type == typeid(spider::core::Data).name()) {
            try {
                msgpack::object_handle const handle
                        = msgpack::unpack(result_buffers[i].data(), result_buffers[i].size());
                msgpack::object const obj = handle.get();
                boost::uuids::uuid data_id;
                obj.convert(data_id);
                outputs.emplace_back(data_id);
            } catch (std::runtime_error const& e) {
                spdlog::error(
                        "Task {} failed to parse result as data id",
                        task.get_function_name()
                );
                return std::nullopt;
            }
        } else {
            msgpack::sbuffer const& buffer = result_buffers[i];
            std::string const value{buffer.data(), buffer.size()};
            outputs.emplace_back(value, type);
        }
    }
    return outputs;
}

// NOLINTBEGIN(clang-analyzer-unix.BlockInCriticalSection)
auto task_loop(
        std::shared_ptr<spider::core::StorageFactory> const& storage_factory,
        std::shared_ptr<spider::core::MetadataStorage> const& metadata_store,
        spider::worker::WorkerClient& client,
        std::string const& storage_url,
        std::vector<std::string> const& libs,
        absl::flat_hash_map<
                boost::process::v2::environment::key,
                boost::process::v2::environment::value> const& environment,
        spider::core::StopToken const& stop_token
) -> void {
    std::optional<boost::uuids::uuid> fail_task_id = std::nullopt;
    while (!stop_token.stop_requested()) {
        boost::asio::io_context context;

        auto const [task_id, task_instance_id] = fetch_task(client, fail_task_id);
        spider::core::TaskInstance const instance{task_instance_id, task_id};
        spdlog::debug("Fetched task {}", boost::uuids::to_string(task_id));
        fail_task_id = std::nullopt;
        // Fetch task detail from metadata storage
        spider::core::Task task{""};
        std::optional<std::vector<msgpack::sbuffer>> optional_args_buffers;
        spider::core::StorageErr err;

        {
            // Keep the scope of RAII storage connection
            std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
                    conn_result = storage_factory->provide_storage_connection();
            if (std::holds_alternative<spider::core::StorageErr>(conn_result)) {
                spdlog::error(
                        "Failed to connect to storage: {}",
                        std::get<spider::core::StorageErr>(conn_result).description
                );
                continue;
            }
            auto conn = std::move(
                    std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result)
            );

            err = metadata_store->get_task(*conn, task_id, &task);
            if (!err.success()) {
                spdlog::error("Failed to fetch task detail: {}", err.description);
                continue;
            }

            // Set up arguments
            optional_args_buffers = get_args_buffers(task);
            if (!optional_args_buffers.has_value()) {
                metadata_store->task_fail(
                        *conn,
                        instance,
                        fmt::format("Task {} failed to parse arguments", task.get_function_name())
                );
                continue;
            }
        }
        std::vector<msgpack::sbuffer> const& args_buffers = optional_args_buffers.value();

        // Execute task
        spider::worker::TaskExecutor executor{
                context,
                task.get_function_name(),
                task.get_id(),
                storage_url,
                libs,
                environment,
                args_buffers
        };

        context.run();
        executor.wait();

        std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
                conn_result = storage_factory->provide_storage_connection();
        if (std::holds_alternative<spider::core::StorageErr>(conn_result)) {
            spdlog::error(
                    "Failed to connect to storage: {}",
                    std::get<spider::core::StorageErr>(conn_result).description
            );
            continue;
        }
        auto conn = std::move(
                std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result)
        );

        if (!executor.succeed()) {
            spdlog::warn("Task {} failed", task.get_function_name());
            metadata_store->task_fail(
                    *conn,
                    instance,
                    fmt::format("Task {} failed", task.get_function_name())
            );
            fail_task_id = task_id;
            continue;
        }

        // Parse result
        std::optional<std::vector<msgpack::sbuffer>> const optional_result_buffers
                = executor.get_result_buffers();
        if (!optional_result_buffers.has_value()) {
            spdlog::error("Task {} failed to parse result into buffers", task.get_function_name());
            metadata_store->task_fail(
                    *conn,
                    instance,
                    fmt::format(
                            "Task {} failed to parse result into buffers",
                            task.get_function_name()
                    )
            );
            fail_task_id = task_id;
            continue;
        }
        std::vector<msgpack::sbuffer> const& result_buffers = optional_result_buffers.value();
        std::optional<std::vector<spider::core::TaskOutput>> const optional_outputs
                = parse_outputs(task, result_buffers);
        if (!optional_outputs.has_value()) {
            metadata_store->task_fail(
                    *conn,
                    instance,
                    fmt::format(
                            "Task {} failed to parse result into TaskOutput",
                            task.get_function_name()
                    )
            );
            fail_task_id = task_id;
            continue;
        }
        std::vector<spider::core::TaskOutput> const& outputs = optional_outputs.value();
        // Submit result
        spdlog::debug("Submitting result for task {}", boost::uuids::to_string(task_id));
        for (int i = 0; i < cRetryCount; ++i) {
            err = metadata_store->task_finish(*conn, instance, outputs);
            if (err.success()) {
                break;
            }
            if (spider::core::StorageErrType::DeadLockErr != err.type) {
                spdlog::error(
                        "Submit task {} fails: {}",
                        task.get_function_name(),
                        err.description
                );
                break;
            }
        }
        fail_task_id = std::nullopt;
        if (!err.success()) {
            spdlog::error("Submit task {} fails: {}", task.get_function_name(), err.description);
        }
    }
}

// NOLINTEND(clang-analyzer-unix.BlockInCriticalSection)
}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
auto main(int argc, char** argv) -> int {
    // Set up spdlog to write to stderr
    // NOLINTNEXTLINE(misc-include-cleaner)
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [spider.worker] %v");
#ifndef NDEBUG
    spdlog::set_level(spdlog::level::trace);
#endif

    boost::program_options::variables_map const args = parse_args(argc, argv);

    std::string storage_url;
    std::vector<std::string> libs;
    std::string worker_addr;
    try {
        if (!args.contains("storage_url")) {
            spdlog::error("Missing storage_url");
            return cCmdArgParseErr;
        }
        storage_url = args["storage_url"].as<std::string>();
        if (!args.contains("host")) {
            spdlog::error("Missing host");
            return cCmdArgParseErr;
        }
        worker_addr = args["host"].as<std::string>();
        if (!args.contains("libs") || args["libs"].empty()) {
            spdlog::error("Missing libs");
            return cCmdArgParseErr;
        }
        libs = args["libs"].as<std::vector<std::string>>();
    } catch (boost::bad_any_cast const& e) {
        spdlog::error("Error: {}", e.what());
        return cCmdArgParseErr;
    } catch (boost::program_options::error const& e) {
        spdlog::error("Error: {}", e.what());
        return cCmdArgParseErr;
    }

    // Create storage
    std::shared_ptr<spider::core::StorageFactory> const storage_factory
            = std::make_shared<spider::core::MySqlStorageFactory>(storage_url);
    std::shared_ptr<spider::core::MetadataStorage> const metadata_store
            = storage_factory->provide_metadata_storage();
    std::shared_ptr<spider::core::DataStorage> const data_store
            = storage_factory->provide_data_storage();

    boost::uuids::random_generator gen;
    boost::uuids::uuid const worker_id = gen();
    spider::core::Driver driver{worker_id};

    {  // Keep the scope of RAII storage connection
        std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
                conn_result = storage_factory->provide_storage_connection();
        if (std::holds_alternative<spider::core::StorageErr>(conn_result)) {
            spdlog::error(
                    "Failed to connect to storage: {}",
                    std::get<spider::core::StorageErr>(conn_result).description
            );
            return cStorageErr;
        }
        auto conn = std::move(
                std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result)
        );

        spider::core::StorageErr const err = metadata_store->add_driver(*conn, driver);
        if (!err.success()) {
            spdlog::error("Cannot add driver to metadata storage: {}", err.description);
            return cStorageErr;
        }
    }

    spider::core::StopToken stop_token;

    // Start client
    spider::worker::WorkerClient
            client{worker_id, worker_addr, data_store, metadata_store, storage_factory};

    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variables
            = get_environment_variable();

    // Start a thread that periodically updates the scheduler's heartbeat
    std::thread heartbeat_thread{
            heartbeat_loop,
            std::cref(storage_factory),
            std::cref(metadata_store),
            std::ref(driver),
            std::ref(stop_token)
    };

    // Start a thread that processes tasks
    std::thread task_thread{
            task_loop,
            std::cref(storage_factory),
            std::cref(metadata_store),
            std::ref(client),
            std::cref(storage_url),
            std::cref(libs),
            std::cref(environment_variables),
            std::cref(stop_token),
    };

    heartbeat_thread.join();
    task_thread.join();

    return 0;
}
