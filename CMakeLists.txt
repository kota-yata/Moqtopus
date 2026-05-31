cmake_minimum_required(VERSION 3.21)

project(moqtopus LANGUAGES CXX)

find_package(msquic CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

file(GLOB_RECURSE MOQ_ALL_SRC CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# library: all src/*.cpp except the main program
set(MOQ_LIBRARY_SRC ${MOQ_ALL_SRC})
list(REMOVE_ITEM MOQ_LIBRARY_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp")

add_library(moq_subscriber ${MOQ_LIBRARY_SRC})
target_compile_features(moq_subscriber PUBLIC cxx_std_17)
target_include_directories(moq_subscriber
  PUBLIC include
  PRIVATE src
)
target_link_libraries(moq_subscriber PRIVATE msquic)
target_link_libraries(moq_subscriber PUBLIC spdlog::spdlog)
target_compile_definitions(moq_subscriber PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)

# main executable (kept explicit so it's clear entrypoint)
add_executable(moqtopus src/main.cpp)
target_link_libraries(moqtopus PRIVATE moq_subscriber)

# example executable
add_executable(moqtail_video_subscriber example/moqtail_video_subscriber.cpp)
target_link_libraries(moqtail_video_subscriber PRIVATE moq_subscriber)
