enable_testing ()

add_library (memory-log memory-log.c)
target_link_libraries (memory-log PUBLIC mochi-raft)

find_package (MPI REQUIRED)

add_executable (simple-test simple-test.c)
target_link_libraries (simple-test PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C memory-log)

add_executable (error-recovery error-recovery.c)
target_link_libraries (error-recovery PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C memory-log)
