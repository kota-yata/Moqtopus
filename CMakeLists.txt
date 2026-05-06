cmake_minimum_required(VERSION 3.21)

project(mosque LANGUAGES CXX)

find_package(msquic CONFIG REQUIRED)

add_executable(mosque src/main.cpp)
target_compile_features(mosque PRIVATE cxx_std_17)
target_link_libraries(mosque PRIVATE msquic)
