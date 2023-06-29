# (C) 2023 The University of Chicago
# See COPYRIGHT in top-level directory.
cmake_minimum_required (VERSION 3.8)
project (mochi-raft C CXX)
enable_testing ()

option (ENABLE_TESTS    "Build tests" OFF)
option (ENABLE_EXAMPLES "Build examples" OFF)
option (ENABLE_SSG      "Build SSG support" OFF)
option (ENABLE_BEDROCK  "Build bedrock module" OFF)
option (ENABLE_PYTHON   "Build the Python module" OFF)

# add our cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE RelWithDebInfo
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "ON" CACHE BOOL "Build a shared library")

find_package (PkgConfig REQUIRED)

if (${ENABLE_BEDROCK})
    find_package (bedrock REQUIRED)
endif ()

# search for margo
pkg_check_modules (MARGO REQUIRED IMPORTED_TARGET margo)
# search for c-raft
pkg_check_modules (RAFT REQUIRED IMPORTED_TARGET raft)
# search for uuid
pkg_check_modules (UUID REQUIRED IMPORTED_TARGET uuid)
# search for abt-io
pkg_check_modules (ABTIO REQUIRED IMPORTED_TARGET abt-io)
# search for json-c
pkg_check_modules (JSONC REQUIRED IMPORTED_TARGET json-c)

if (ENABLE_SSG)
    # search for ssg
    pkg_check_modules (SSG REQUIRED IMPORTED_TARGET ssg)
    set (PC_REQUIRES ssg)
endif ()

if (ENABLE_PYTHON)
    find_package (Python3 COMPONENTS Interpreter Development REQUIRED)
    find_package (pybind11 REQUIRED)
    add_subdirectory (python)
endif ()

# library version set here (e.g. for shared libs).
set (MRAFT_VERSION_MAJOR 0)
set (MRAFT_VERSION_MINOR 1)
set (MRAFT_VERSION_PATCH 0)
set (MRAFT_VERSION
     "${MRAFT_VERSION_MAJOR}.${MRAFT_VERSION_MINOR}.${MRAFT_VERSION_PATCH}")

add_subdirectory (src)
if (${ENABLE_TESTS})
    enable_testing ()
    find_package (Catch2 QUIET)
    if (NOT Catch2_FOUND)
        include (FetchContent)
        FetchContent_Declare (
            Catch2
            GIT_REPOSITORY https://github.com/catchorg/Catch2.git
            GIT_TAG        v3.0.1
        )
        FetchContent_MakeAvailable (Catch2)
    endif ()
    add_subdirectory (tests)
endif (${ENABLE_TESTS})
if (${ENABLE_EXAMPLES})
    add_subdirectory (examples)
endif (${ENABLE_EXAMPLES})
