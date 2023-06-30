add_library (pymraft MODULE py-mochi-raft.cpp)
target_link_libraries (pymraft PRIVATE pybind11::module mochi-raft)
pybind11_extension (pymraft)
pybind11_strip (pymraft)

set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})

install (TARGETS pymraft EXPORT mochi-raft-targets
         ARCHIVE DESTINATION lib/python${PY_VERSION}/site-packages
         LIBRARY DESTINATION lib/python${PY_VERSION}/site-packages)
