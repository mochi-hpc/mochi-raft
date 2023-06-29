enable_testing ()

find_package (MPI REQUIRED)

add_executable (simple-test simple-test.c)
target_link_libraries (simple-test PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C mochi-raft-memory-log)

add_executable (error-recovery error-recovery.c)
target_link_libraries (error-recovery PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C mochi-raft-memory-log)

add_executable (mraft_get_raft_id mraft_get_raft_id.c)
target_link_libraries (mraft_get_raft_id PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C mochi-raft-memory-log)
