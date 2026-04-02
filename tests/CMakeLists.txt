include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

function(mochi_raft_add_test name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name}
        PRIVATE
            mochi-raft
            GTest::gtest_main
            abt-io
            PkgConfig::ABT
            coverage_config
    )
    add_test(NAME ${name} COMMAND ${name})
endfunction()

mochi_raft_add_test(test_raft_init)
mochi_raft_add_test(test_storage)
mochi_raft_add_test(test_raft_step)
mochi_raft_add_test(test_serialization)
mochi_raft_add_test(test_network)
mochi_raft_add_test(test_event_queue)
mochi_raft_add_test(test_single_node)
mochi_raft_add_test(test_cluster)
mochi_raft_add_test(test_log_iterator)
mochi_raft_add_test(test_snapshot)
