Network Partition Simulation
=============================

mochi-raft provides built-in network isolation for testing partition tolerance
and leader re-election behavior. Isolation is applied at the transport layer
before any Thallium RPC is processed or dispatched.

IsolationMode
-------------

Four modes are defined in :code:`mraft::IsolationMode`:

+----------+--------------------------------------------+
| Mode     | Effect                                     |
+==========+============================================+
| NONE     | No isolation (normal operation).           |
+----------+--------------------------------------------+
| INBOUND  | Drop all incoming messages.                |
+----------+--------------------------------------------+
| OUTBOUND | Drop all outgoing messages.                |
+----------+--------------------------------------------+
| BOTH     | Drop all messages in both directions.      |
+----------+--------------------------------------------+

:code:`set_isolation(mode)` takes effect immediately and is thread-safe.

Simulating a partition
-----------------------

The following pattern isolates the current leader, waits for a new leader to
emerge among the remaining nodes, then heals the partition and verifies the
old leader rejoins as a follower.

.. code-block:: cpp

   // Isolate the leader — it can no longer send or receive messages.
   servers[old_leader]->set_isolation(mraft::IsolationMode::BOTH);

   // The remaining two servers stop receiving heartbeats, time out,
   // and elect a new leader among themselves.
   int new_leader = -1;
   while (new_leader < 0) {
       for (int i = 0; i < N; i++) {
           if (i != old_leader && servers[i]->state() == RAFT_LEADER)
               new_leader = i;
       }
       yield_ms(10);
   }

   // Heal the partition.
   servers[old_leader]->set_isolation(mraft::IsolationMode::NONE);

   // The old leader receives a heartbeat with a higher term and steps down.
   while (servers[old_leader]->state() != RAFT_FOLLOWER)
       yield_ms(10);

.. note::
   Network isolation only affects the transport layer; it does not corrupt or
   truncate the persistent log. A node that was isolated and then rejoins will
   catch up from its last committed index via AppendEntries or InstallSnapshot.

Inbound vs. outbound isolation
-------------------------------

:code:`INBOUND` isolation makes the node deaf: it stops receiving heartbeats
and will eventually start a new election (which it cannot win, since it cannot
receive votes either). From the other nodes' perspective the isolated node has
simply disappeared.

:code:`OUTBOUND` isolation makes the node mute: it can still receive messages
but cannot respond, so peers time out waiting for replies. This is useful for
testing slow-follower scenarios.

:code:`BOTH` combines both effects and is the most faithful simulation of a
complete network partition.
