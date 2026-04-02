Multi-Node Cluster
==================

A production Raft cluster consists of an odd number of servers, each running
in its own process. mochi-raft servers discover each other through the
Thallium address passed to :code:`bootstrap()`; no additional service
discovery is needed.

Bootstrapping
-------------

Every server in the initial cluster must call :code:`bootstrap()` with the
**same** cluster configuration: a list of :code:`(server_id, thallium_address)`
pairs. Server IDs must be positive integers, unique within the cluster.

.. code-block:: cpp

   std::vector<std::pair<raft_id, std::string>> config = {
       {1, "na+sm://..."},
       {2, "na+sm://..."},
       {3, "na+sm://..."},
   };
   server.bootstrap(config);

:code:`bootstrap()` writes the configuration as the first log entry. On a node
restart it detects that a log already exists and is a no-op — it is safe to
call unconditionally in your startup code.

Starting and leader election
----------------------------

All servers must call :code:`start()` before the cluster can elect a leader.
After each server starts, they exchange RequestVote RPCs. The first server to
time out and collect a majority of votes becomes leader. On a fresh cluster
this typically happens within one election timeout (≈ 300 ms by default in
c-raft).

.. code-block:: cpp

   server.start();

   // Wait for this server to become leader.  In practice you may not care
   // which server wins; the leader() query below works on any server.
   while (server.state() != RAFT_LEADER)
       ABT_thread_yield();

Querying the cluster
--------------------

Several methods return information about the current cluster state:

.. code-block:: cpp

   // Which server is the current leader?
   auto [leader_id, leader_addr] = server.leader();

   // All servers in the committed configuration.
   auto members = server.members();  // vector of (id, address) pairs

   raft_term  term = server.current_term();
   raft_index idx  = server.commit_index();

All of these are best-effort snapshots of the server's local Raft state. They
are not synchronized with the event loop and may be transiently stale. Use
them for monitoring and client routing, not for strong consistency guarantees.

Complete example
----------------

The following example runs three servers in a single process using the
shared-memory transport (:code:`na+sm`). In production each :code:`tl::engine`
and :code:`MochiRaftServer` would live in a separate process on a separate
machine.

.. literalinclude:: ../../examples/mochi-raft/02_cluster/cluster.cpp
   :language: cpp
