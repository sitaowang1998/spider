#ifndef SPIDER_UTILS_PROGRAMOPTIONS_HPP
#define SPIDER_UTILS_PROGRAMOPTIONS_HPP

#include <string_view>

namespace spider::core {

constexpr std::string_view cSchedulerUsage
        = {"Usage: spider_scheduler --host <host> --port <port> --storage_url <url>"};

constexpr std::string_view cSchedulerHelpMessage
        = {"Try 'spider_scheduler --help' for detailed usage instructions.\n"};

constexpr std::string_view cWorkerUsage
        = {"Usage: spider_worker --host <host> --storage_url <storage_url> --libs <libs>"};

constexpr std::string_view cWorkerHelpMessage
        = {"Try 'spider_worker --help' for detailed usage instructions.\n"};

constexpr std::string_view cTaskExecutorUsage
        = {"Usage: spider_task_executor --func <function> --task_id <task_id> --storage_url "
           "<storage_url> --libs <libs>"};

constexpr std::string_view cTaskExecutorHelpMessage
        = {"Try 'spider_task_executor --help' for detailed usage instructions.\n"};

constexpr std::string_view cHelpOption = {"help"};

constexpr std::string_view cHelpMessage = {"Print this help text."};

constexpr std::string_view cHostOption = {"host"};

constexpr std::string_view cHostMessage = {"The host address to bind to"};

constexpr std::string_view cPortOption = {"port"};

constexpr std::string_view cPortMessage = {"The port to listen on"};

constexpr std::string_view cStorageUrlOption = {"storage_url"};

constexpr std::string_view cStorageUrlMessage = {"The storage server's URL"};

constexpr std::string_view cLibsOption = {"libs"};

constexpr std::string_view cLibsMessage = {"The tasks libraries to load"};

constexpr std::string_view cFunctionOption = {"func"};

constexpr std::string_view cFunctionMessage = {"The function to execute"};

constexpr std::string_view cTaskIdOption = {"task_id"};

constexpr std::string_view cTaskIdMessage = {"The id of the task to execute"};

}  // namespace spider::core

#endif
