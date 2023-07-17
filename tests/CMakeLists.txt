enable_testing ()

find_package (MPI REQUIRED)
find_package (thallium REQUIRED)

add_executable (mraft_get_raft_id mraft_get_raft_id.c)
target_link_libraries (mraft_get_raft_id PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C)

add_executable (memory-log-simple-test memory-log-simple-test.c)
target_link_libraries (memory-log-simple-test PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C)

add_executable (abt-io-log-simple-test abt-io-log-simple-test.c)
target_link_libraries (abt-io-log-simple-test PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C)

add_executable (memory-log-error-recovery memory-log-error-recovery.c)
target_link_libraries (memory-log-error-recovery PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C)

add_executable (abt-io-log-error-recovery abt-io-log-error-recovery.c)
target_link_libraries (abt-io-log-error-recovery PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C)

add_executable (memory-log-generic-test memory-log-generic-test.cpp)
target_link_libraries (memory-log-generic-test PUBLIC mochi-raft thallium)

add_executable (abt-io-log-generic-test abt-io-log-generic-test.cpp)
target_link_libraries (abt-io-log-generic-test PUBLIC mochi-raft thallium)
