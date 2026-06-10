cmake_minimum_required(VERSION 3.21)

project(moqtopus LANGUAGES CXX)

find_package(msquic CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

file(GLOB_RECURSE MOQ_ALL_SRC CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

add_library(moq_subscriber ${MOQ_ALL_SRC})
target_compile_features(moq_subscriber PUBLIC cxx_std_17)
target_include_directories(moq_subscriber
  PUBLIC include
  PRIVATE src
)
target_link_libraries(moq_subscriber PRIVATE msquic)
target_link_libraries(moq_subscriber PUBLIC spdlog::spdlog)
target_compile_definitions(moq_subscriber PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)

# moq-interop-runner compatible test client
add_executable(moqtopus_interop_client tools/moqtopus_interop_client.cpp)
target_link_libraries(moqtopus_interop_client PRIVATE moq_subscriber)

# example executables
add_executable(subscriber example/subscriber.cpp)
target_link_libraries(subscriber PRIVATE moq_subscriber)
