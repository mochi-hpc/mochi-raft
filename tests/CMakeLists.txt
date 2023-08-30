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

file (GLOB scenarios ${CMAKE_CURRENT_SOURCE_DIR}/scenarios/*.lua)
foreach (scenario ${scenarios})
    get_filename_component (test-name ${scenario} NAME_WE)
    add_test (NAME ${test-name}-abt-io
              COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run-test.sh ${scenario} abt-io)
    add_test (NAME ${test-name}-memory
              COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run-test.sh ${scenario} memory)
endforeach ()
add_custom_target (check COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS ${scenarios} mraft-test)
