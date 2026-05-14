cmake_minimum_required(VERSION 3.21)

project(montique LANGUAGES CXX)

find_package(msquic CONFIG REQUIRED)

add_executable(montique src/main.cpp)
target_compile_features(montique PRIVATE cxx_std_17)
target_link_libraries(montique PRIVATE msquic)
