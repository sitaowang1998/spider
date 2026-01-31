cmake_minimum_required(VERSION 3.22.1)
project(executor_overhead_benchmark)

if(PROJECT_IS_TOP_LEVEL)
    # Include dependency settings if the project isn't being included as a subproject.
    include("${CMAKE_SOURCE_DIR}/../../build/deps/cmake-settings/all.cmake")
endif()

# Add the Spider library
add_subdirectory(../.. spider EXCLUDE_FROM_ALL)

# Add the sleep tasks library
add_library(
    sleep_tasks
    SHARED
    src/sleep_tasks.cpp
    src/sleep_tasks.hpp
)

# Link the Spider library to the tasks library
target_link_libraries(sleep_tasks PRIVATE spider::spider)

# Add the benchmark client
add_executable(benchmark_client src/benchmark_client.cpp)

# Link the Spider and tasks library to the client
target_link_libraries(
    benchmark_client
    PRIVATE
        spider::spider
        sleep_tasks
)
