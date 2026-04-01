mochi-raft (Raft consensus for the Mochi stack)
================================================

mochi-raft is a C++ library that implements the Raft consensus protocol on top
of the Mochi stack. It uses Thallium for RPC and RDMA network
transport, and ABT-IO for durable log and metadata persistence.

The library wraps `c-raft <https://github.com/cowsql/raft>`_'s step-based API
behind a clean C++ interface, making it straightforward to add strongly
consistent, fault-tolerant state to any Mochi service.

.. important::
   mochi-raft's event loop and RPC handlers run as Argobots user-level threads
   (ULTs). Any synchronization primitives used inside callbacks or the
   :code:`Fsm::apply()` method **must** be Thallium (Argobots) wrappers, not
   their :code:`std::` counterparts. Use :code:`thallium::mutex` instead of
   :code:`std::mutex`, and :code:`thallium::condition_variable` instead of
   :code:`std::condition_variable`. Using :code:`std::` synchronization
   primitives in a ULT context is a common source of deadlocks and hangs.

.. toctree::
   :maxdepth: 1

   mochi-raft/01_overview.rst
   mochi-raft/02_fsm.rst
   mochi-raft/03_single_node.rst
   mochi-raft/04_submit.rst
   mochi-raft/05_cluster.rst
   mochi-raft/06_callbacks.rst
   mochi-raft/07_forward.rst
   mochi-raft/08_rdma.rst
   mochi-raft/09_isolation.rst
   mochi-raft/10_leadership.rst
   mochi-raft/11_snapshot.rst
