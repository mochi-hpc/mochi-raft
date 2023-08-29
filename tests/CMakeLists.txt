enable_testing ()

find_package (MPI REQUIRED)
find_package (thallium REQUIRED)
find_package (sol2 REQUIRED)
find_package (Lua REQUIRED)
find_package (TCLAP REQUIRED)
find_package (readline REQUIRED)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories (${LUA_INCLUDE_DIRS})

add_executable (mraft-test test-framework.cpp)
target_link_libraries (mraft-test
    PRIVATE sol2::sol2 ${LUA_LIBRARIES} mochi-raft READLINE::READLINE)

#add_executable (mraft_get_raft_id mraft_get_raft_id.c)
#target_link_libraries (mraft_get_raft_id PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C)

#add_executable (memory-log-simple-test memory-log-simple-test.c)
#target_link_libraries (memory-log-simple-test PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C)

#add_executable (abt-io-log-simple-test abt-io-log-simple-test.c)
#target_link_libraries (abt-io-log-simple-test PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C)

#add_executable (memory-log-error-recovery memory-log-error-recovery.c)
#target_link_libraries (memory-log-error-recovery PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C)

#add_executable (abt-io-log-error-recovery abt-io-log-error-recovery.c)
#target_link_libraries (abt-io-log-error-recovery PUBLIC mochi-raft PkgConfig::SSG MPI::MPI_C)

#add_executable (memory-log-generic-test memory-log-generic-test.cpp)
#target_link_libraries (memory-log-generic-test PUBLIC mochi-raft thallium)

#add_executable (abt-io-log-generic-test abt-io-log-generic-test.cpp)
#target_link_libraries (abt-io-log-generic-test PUBLIC mochi-raft thallium)

file (GLOB scenarios ${CMAKE_CURRENT_SOURCE_DIR}/scenarios/*.lua)
foreach (scenario ${scenarios})
    get_filename_component (test-name ${scenario} NAME_WE)
    add_test (NAME ${test-name}
              COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run-test.sh ${scenario})
endforeach ()
add_custom_target (check COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS ${scenarios} mraft-test)
