# set source files
set (src-files
     mraft.c)

set (bedrock-module-src-files
     bedrock-module.c)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (mochi-raft-pkg "share/cmake/mochi-raft")

# set version fo shared library
set (mochi-raft-vers "${MRAFT_VERSION_MAJOR}.${MRAFT_VERSION_MINOR}")

# server library
add_library (mochi-raft ${src-files})
target_link_libraries (mochi-raft
    PkgConfig::MARGO
    PkgConfig::ABTIO
    PkgConfig::UUID
    PkgConfig::JSONC
    PkgConfig::SSG
    PkgConfig::RAFT)
target_include_directories (mochi-raft PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (mochi-raft BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (mochi-raft BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (mochi-raft
    PROPERTIES VERSION ${MRAFT_VERSION}
    SOVERSION ${MRAFT_VERSION_MAJOR})

if (${ENABLE_BEDROCK})
# bedrock-module library
add_library (mochi-raft-bedrock-module ${bedrock-module-src-files})
target_link_libraries (mochi-raft-bedrock-module mochi-raft bedrock-server)
target_include_directories (mochi-raft-bedrock-module PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (mochi-raft-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (mochi-raft-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (mochi-raft-bedrock-module
    PROPERTIES VERSION ${MRAFT_VERSION}
    SOVERSION ${MRAFT_VERSION_MAJOR})
endif ()

# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "mochi-raft-config-version.cmake"
    VERSION ${MRAFT_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (mochi-raft-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/mochi-raft-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/mochi-raft/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (PRIVATE_LIBS "-lmochi-raft")
configure_file ("mochi-raft.pc.in" "mochi-raft.pc" @ONLY)

# configure config.h
configure_file ("config.h.in" "config.h" @ONLY)

# "make install" rules
install (TARGETS mochi-raft
         EXPORT mochi-raft-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
if (${ENABLE_BEDROCK})
    install (TARGETS mochi-raft-bedrock-module
             ARCHIVE DESTINATION lib
             LIBRARY DESTINATION lib)
endif ()
install (EXPORT mochi-raft-targets
         DESTINATION ${mochi-raft-pkg}
         FILE "mochi-raft-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/mochi-raft-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/mochi-raft-config-version.cmake"
         DESTINATION ${mochi-raft-pkg} )
install (DIRECTORY ../include/mochi-raft
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/mochi-raft.pc"
         DESTINATION "lib/pkgconfig/")
