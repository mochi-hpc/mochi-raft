Single-Node Server
==================

The simplest mochi-raft deployment is a single-node cluster. A single node
forms a quorum by itself: it elects itself leader immediately and can commit
entries without waiting for any peer.

This is useful for development, unit testing, and any application that needs
durability and log replay via Raft without fault tolerance.

Initialization
--------------

Two Mochi objects must be initialized before creating a
:code:`MochiRaftServer`:

* **Thallium engine** — initialized in server mode.
* **ABT-IO instance** — for all log and metadata I/O.

.. code-block:: cpp

   tl::engine engine("na+sm", THALLIUM_SERVER_MODE, true, 1);
   abt_io_instance_id abt_io = abt_io_init(1);

Constructing the server
-----------------------

:code:`MochiRaftServer` takes the engine, ABT-IO instance, a unique ID,
two Argobots pools (event loop pool and RPC pool), a data
directory, and your FSM.

.. code-block:: cpp

   auto pool = engine.get_handler_pool();
   mraft::MochiRaftServer server(engine, abt_io, /*id=*/1,
                                 pool, pool, "/tmp/my-raft", fsm);

The data directory is created if absent. Both the engine and the FSM must
outlive the server.

Bootstrap and start
-------------------

:code:`bootstrap()` records the initial cluster configuration as the first log
entry. Call it on every node **before** :code:`start()`, and only once per
cluster (it is a no-op on restart if a log already exists).

.. code-block:: cpp

   std::string address = static_cast<std::string>(engine.self());
   server.bootstrap({{1, address}});
   server.start();

:code:`start()` loads any persisted state from the data directory, then
launches the event loop ULT. After this call the server participates in
elections, replicates entries, and applies committed entries to the FSM.

Waiting for leadership
-----------------------

:code:`state()` returns the current Raft state as one of :code:`RAFT_FOLLOWER`,
:code:`RAFT_CANDIDATE`, or :code:`RAFT_LEADER`. In a single-node cluster the
server self-elects within one election timeout (typically a few hundred
milliseconds):

.. code-block:: cpp

   while (server.state() != RAFT_LEADER)
       ABT_thread_yield();

Shutdown
--------

:code:`shutdown()` stops the event loop and blocks until the ULT has exited.
Always call it before destroying the engine.

.. code-block:: cpp

   server.shutdown();
   engine.finalize();
   abt_io_finalize(abt_io);
   ABT_finalize();

Complete example
----------------

.. literalinclude:: ../../examples/mochi-raft/01_single_node/single_node.cpp
   :language: cpp
