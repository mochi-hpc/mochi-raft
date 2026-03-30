# Worker binary — links against mochi-raft and all Mochi deps
add_executable(mochi-raft-worker worker.cpp)
target_include_directories(mochi-raft-worker PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)
target_link_libraries(mochi-raft-worker
    PRIVATE
        mochi-raft
        abt-io
        PkgConfig::ABT
)

# Master CLI binary — NO Mochi dependencies, plain C++17
add_executable(mochi-raft-ctl ctl.cpp)
target_include_directories(mochi-raft-ctl PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)
