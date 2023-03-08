add_library (memory-log memory-log.c)
target_link_libraries (memory-log PUBLIC mochi-raft)
