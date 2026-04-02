cmake_minimum_required(VERSION 3.20)
project(mochi-raft VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)

# Spack dependencies
list(APPEND CMAKE_PREFIX_PATH "$ENV{SPACK_ENV}/.spack-env/view")
find_package(thallium REQUIRED)
find_package(abt-io REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(ABT REQUIRED IMPORTED_TARGET argobots)

# ---------- c-raft via pkg-config ----------
pkg_check_modules(RAFT REQUIRED IMPORTED_TARGET raft)

# ---------- mochi-raft library ----------
add_library(mochi-raft
    src/storage.cpp
    src/network.cpp
    src/event_queue.cpp
    src/mochi_raft.cpp
    src/log_iterator.cpp
)
target_include_directories(mochi-raft
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_link_libraries(mochi-raft
    PUBLIC
        PkgConfig::RAFT
        thallium
    PRIVATE
        abt-io
        PkgConfig::ABT
)

# ---------- Tools ----------
add_subdirectory(tools)

# ---------- Tests ----------
option(ENABLE_TESTS "Build the test suite" ON)
if(ENABLE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ---------- Examples ----------
option(ENABLE_EXAMPLES "Build the documentation examples" OFF)
if(ENABLE_EXAMPLES)
    add_subdirectory(docs/examples/mochi-raft)
endif()
