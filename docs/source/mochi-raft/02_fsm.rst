Implementing the FSM
====================

The :code:`mraft::Fsm` interface defines what happens to committed log entries.
Implement it by subclassing :code:`mraft::Fsm` and overriding the methods
relevant to your application.

The Fsm interface
-----------------

.. code-block:: cpp

   struct Fsm {
       virtual ~Fsm() = default;

       // Required: apply a committed log entry.
       virtual int apply(std::string_view data) = 0;

       // Optional: serialise the FSM state for snapshotting.
       virtual int snapshot(std::string& data) { return RAFT_NOTFOUND; }

       // Optional: restore the FSM state from a snapshot.
       virtual int restore(std::string_view data) { return RAFT_NOTFOUND; }
   };

apply()
-------

:code:`apply()` is called exactly once per committed :code:`RAFT_COMMAND`
entry, in log order. Configuration-change entries (:code:`RAFT_CHANGE`) and
barrier entries (:code:`RAFT_BARRIER`) are filtered out by the server and
never reach :code:`apply()`.

The method receives the raw payload submitted by the client as a
:code:`std::string_view`. The byte format is entirely up to the application:
you may use plain-text commands, Protocol Buffers, MessagePack, or any other
encoding, as long as :code:`submit()` and :code:`apply()` agree on the format.

Return 0 on success or a non-zero error code on failure.

.. important::
   :code:`apply()` is called from inside the event loop ULT. Keep it fast.
   Avoid blocking calls, heavy computation, or locking with :code:`std::mutex`
   (use :code:`thallium::mutex` instead). If your FSM needs to do significant
   I/O, offload the work to a separate Argobots pool.

snapshot() and restore()
------------------------

These two methods are optional. They enable log compaction: instead of
replaying the entire log from the beginning on every restart, the server
periodically captures the full FSM state in a snapshot and discards the log
entries the snapshot supersedes.

:code:`snapshot(data)` is called from the event loop ULT when a snapshot is
due. The implementation should serialise its complete state into :code:`data`.
Returning :code:`RAFT_NOTFOUND` (the default) opts the FSM out of snapshotting
entirely — the log will grow without bound, but the server will still function
correctly.

:code:`restore(data)` is called in two situations:

* At startup, if a snapshot file is found in the data directory, before any
  log entries are replayed.
* On a follower, when the leader sends an :code:`InstallSnapshot` RPC because
  the follower has fallen too far behind to be caught up via normal
  AppendEntries.

The implementation must replace the entire FSM state with the one encoded in
:code:`data`.

See :doc:`11_snapshot` for the complete guide to snapshotting, including how
to configure the snapshot threshold and interpret :code:`log_start_index()`.

A key-value store example
--------------------------

The following header defines a minimal key-value FSM that understands two
plain-text commands — :code:`SET key value` and :code:`DEL key`.

.. literalinclude:: ../../examples/mochi-raft/03_fsm/kv_fsm.hpp
   :language: cpp

This FSM stores all state in a :code:`std::map<std::string, std::string>`.
The :code:`get()` helper is not thread-safe with :code:`apply()`; read from
the FSM only when no entries are being applied — for example, after
:code:`shutdown()` returns, or from a callback that runs after the last
submitted entry has been applied.
