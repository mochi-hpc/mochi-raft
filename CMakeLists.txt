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

# ---------- c-raft via FetchContent ----------
include(FetchContent)
FetchContent_Declare(
    raft
    GIT_REPOSITORY https://github.com/cowsql/raft.git
    GIT_TAG        main
)
FetchContent_Populate(raft)

# Generate raft.h from raft.h.in (non-legacy step-based API)
set(enable_v0 "no")
configure_file(
    ${raft_SOURCE_DIR}/include/raft.h.in
    ${CMAKE_BINARY_DIR}/raft/include/raft.h
    @ONLY
)

# Build c-raft core as a static library (excluding uv_*, legacy, fixture, log, snapshot)
set(RAFT_CORE_SOURCES
    ${raft_SOURCE_DIR}/src/byte.c
    ${raft_SOURCE_DIR}/src/client.c
    ${raft_SOURCE_DIR}/src/compress.c
    ${raft_SOURCE_DIR}/src/configuration.c
    ${raft_SOURCE_DIR}/src/convert.c
    ${raft_SOURCE_DIR}/src/election.c
    ${raft_SOURCE_DIR}/src/entry.c
    ${raft_SOURCE_DIR}/src/err.c
    ${raft_SOURCE_DIR}/src/heap.c
    ${raft_SOURCE_DIR}/src/membership.c
    ${raft_SOURCE_DIR}/src/message.c
    ${raft_SOURCE_DIR}/src/progress.c
    ${raft_SOURCE_DIR}/src/random.c
    ${raft_SOURCE_DIR}/src/raft.c
    ${raft_SOURCE_DIR}/src/recv.c
    ${raft_SOURCE_DIR}/src/recv_append_entries.c
    ${raft_SOURCE_DIR}/src/recv_append_entries_result.c
    ${raft_SOURCE_DIR}/src/recv_request_vote.c
    ${raft_SOURCE_DIR}/src/recv_request_vote_result.c
    ${raft_SOURCE_DIR}/src/recv_install_snapshot.c
    ${raft_SOURCE_DIR}/src/recv_timeout_now.c
    ${raft_SOURCE_DIR}/src/replication.c
    ${raft_SOURCE_DIR}/src/restore.c
    ${raft_SOURCE_DIR}/src/state.c
    ${raft_SOURCE_DIR}/src/syscall.c
    ${raft_SOURCE_DIR}/src/timeout.c
    ${raft_SOURCE_DIR}/src/tracing.c
    ${raft_SOURCE_DIR}/src/trail.c
)

# Copy generated raft.h to the location c-raft sources expect (../include/raft.h)
file(COPY ${CMAKE_BINARY_DIR}/raft/include/raft.h
     DESTINATION ${raft_SOURCE_DIR}/include/)

add_library(raft_core STATIC ${RAFT_CORE_SOURCES})
target_include_directories(raft_core
    PUBLIC
        ${raft_SOURCE_DIR}/include        # raft.h (generated, copied here)
)
# Use -iquote for src/ so that #include "assert.h" finds the local one
# but #include <assert.h> still finds the system one
target_compile_options(raft_core PRIVATE -iquote ${raft_SOURCE_DIR}/src)
target_compile_definitions(raft_core PRIVATE _GNU_SOURCE)
set_target_properties(raft_core PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# ---------- mochi-raft library ----------
add_library(mochi-raft
    src/storage.cpp
    src/network.cpp
    src/event_queue.cpp
    src/mochi_raft.cpp
)
target_include_directories(mochi-raft
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_link_libraries(mochi-raft
    PUBLIC
        raft_core
        thallium
    PRIVATE
        abt-io
        PkgConfig::ABT
)

# ---------- Tools ----------
add_subdirectory(tools)

# ---------- Tests ----------
enable_testing()
add_subdirectory(tests)
