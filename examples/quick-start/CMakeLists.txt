cmake_minimum_required(VERSION 3.22.1)
project(spider_getting_started)

if(PROJECT_IS_TOP_LEVEL)
    # Include dependency settings if the project isn't being included as a subproject.
    include("${CMAKE_SOURCE_DIR}/../../build/deps/cmake-settings/all.cmake")
endif()

# Add the Spider library
add_subdirectory(../../ spider EXCLUDE_FROM_ALL)

# Add the tasks library
add_library(
    tasks
    SHARED
    src/tasks.cpp
    src/tasks.hpp
)

# Link the Spider library to the tasks library
target_link_libraries(tasks PRIVATE spider::spider)

# Add the client
add_executable(client src/client.cpp)

# Link the Spider and tasks library to the client
target_link_libraries(
    client
    PRIVATE
        spider::spider
        tasks
)
