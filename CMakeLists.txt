cmake_minimum_required(VERSION 3.21)

project(moqtopus LANGUAGES CXX)

find_package(msquic CONFIG REQUIRED)

add_library(moq_subscriber
  src/codec.cpp
  src/data_plane.cpp
  src/executor.cpp
  src/msquic_transport_adapter.cpp
  src/subscriber_session.cpp
)
target_compile_features(moq_subscriber PUBLIC cxx_std_17)
target_include_directories(moq_subscriber
  PUBLIC include
  PRIVATE src
)
target_link_libraries(moq_subscriber PRIVATE msquic)

add_executable(moqtopus src/main.cpp)
target_link_libraries(moqtopus PRIVATE moq_subscriber)

add_executable(moqtail_video_subscriber
  example/moqtail_video_subscriber.cpp
)
target_link_libraries(moqtail_video_subscriber PRIVATE moq_subscriber)
