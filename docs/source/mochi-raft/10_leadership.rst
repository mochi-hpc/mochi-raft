Leadership Transfer
===================

Leadership transfer allows a cluster administrator to move the leader role to
a specific node — for example, before taking the current leader offline for
maintenance or load balancing.

Querying the leader
--------------------

Any server can report who it currently believes to be the leader:

.. code-block:: cpp

   auto [leader_id, leader_addr] = server.leader();
   if (leader_id == 0) {
       // No leader is currently known (election may be in progress).
   }

This is a best-effort read from the server's local Raft state. The result may
be transiently stale immediately after a leader change.

Initiating a transfer
----------------------

Call :code:`transfer()` on the **current leader**, passing the Raft ID of the
intended new leader:

.. code-block:: cpp

   raft_id target_id = 3;
   int rv = servers[leader]->transfer(target_id);

:code:`transfer()` is asynchronous. It pushes a :code:`RAFT_TRANSFER` event
into the event loop queue. The event loop then sends a :code:`TimeoutNow` RPC
to the target, which triggers an immediate election on that node (bypassing
the normal election timeout). The target wins the election and becomes leader
within one round-trip.

Wait for the transfer to complete by polling the target's state:

.. code-block:: cpp

   while (servers[target]->state() != RAFT_LEADER)
       ABT_thread_yield();

.. note::
   :code:`transfer()` must be called on the **current leader**. If the target
   is not fully caught up with the leader's log at the time of the call, the
   :code:`TimeoutNow` RPC is delayed until the target catches up. If the
   transfer does not complete within one Raft election timeout, the leader
   retakes leadership automatically to avoid an availability gap.
