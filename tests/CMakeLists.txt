enable_testing ()

find_package (MPI REQUIRED)
find_package (thallium REQUIRED)
find_package (TCLAP REQUIRED)
find_package (readline REQUIRED)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable (mraft-py-test python-test-framework.cpp)
target_link_libraries (mraft-py-test
    PRIVATE pybind11::embed asan_config mochi-raft thallium READLINE::READLINE)

file (GLOB scenarios ${CMAKE_CURRENT_SOURCE_DIR}/scenarios/*.py)
foreach (scenario ${scenarios})
    get_filename_component (test-name ${scenario} NAME_WE)
    add_test (NAME ${test-name}-abt-io
              COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run-test.sh ${scenario} abt-io)
    add_test (NAME ${test-name}-memory
              COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run-test.sh ${scenario} memory)
endforeach ()
add_custom_target (check COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS ${scenarios} mraft-test)
