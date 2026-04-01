Snapshots and Log Compaction
============================

Without log compaction, the Raft log grows indefinitely. Every restart
replays the entire log to rebuild the FSM, and a follower that has been
offline for a long time must receive every entry since the beginning of
time before it can rejoin the cluster. Snapshots solve both problems.

A snapshot captures the complete FSM state at a particular log index. Once
persisted, all log entries up to that index can be discarded. A follower
that is too far behind to be caught up by normal AppendEntries will receive
the snapshot directly via an :code:`InstallSnapshot` RPC.

How snapshots work in mochi-raft
---------------------------------

Snapshot support is built into the server; the application only needs to
implement two methods on its :code:`Fsm` subclass. The server calls them at
the right time:

* **:code:`snapshot(data)`** — called when a snapshot is due. Fill
  :code:`data` with a complete serialisation of the FSM state.
* **:code:`restore(data)`** — called on startup (if a snapshot exists on
  disk) or when the server installs a snapshot received from the leader.
  Replace the entire FSM state with what is encoded in :code:`data`.

The snapshot is written atomically to a :code:`snapshot` file in the data
directory (via a write-then-rename sequence). Log segment files whose entries
are fully covered by the snapshot are deleted automatically, keeping only a
configurable trailing window of recent entries.

Implementing snapshot and restore
----------------------------------

Both methods work with a plain byte string, so the encoding is entirely up to
the application. The example below serialises a 32-bit integer counter:

.. code-block:: cpp

   class CountingFsm : public mraft::Fsm {
       int count_ = 0;
   public:
       int apply(std::string_view) override {
           ++count_;
           return 0;
       }

       int snapshot(std::string& out) override {
           out.resize(sizeof(int));
           std::memcpy(out.data(), &count_, sizeof(int));
           return 0;
       }

       int restore(std::string_view in) override {
           if (in.size() < sizeof(int)) return -1;
           std::memcpy(&count_, in.data(), sizeof(int));
           return 0;
       }
   };

Return 0 from both methods on success. Returning :code:`RAFT_NOTFOUND` from
:code:`snapshot()` disables snapshotting for this FSM — log compaction will
not occur and :code:`restore()` will never be called.

Configuring the snapshot frequency
------------------------------------

Two knobs control when snapshots are taken. Call them before :code:`start()`.

.. code-block:: cpp

   // Take a snapshot every 512 committed entries (default: 1024).
   server.set_snapshot_threshold(512);

   // Keep the 64 most recent entries in the log after a snapshot (default: 128).
   // These trailing entries allow lagging followers to catch up without needing
   // a full InstallSnapshot in common cases.
   server.set_snapshot_trailing(64);

A snapshot is taken automatically inside the event loop after
:code:`apply()` has been called enough times to cross the threshold since the
last snapshot. The leader's snapshot is then available to send to any follower
that falls behind by more than the trailing window.

To disable automatic snapshotting entirely, set the threshold to zero:

.. code-block:: cpp

   server.set_snapshot_threshold(0);  // disable auto-snapshots

Restart and recovery
---------------------

No extra startup code is needed. :code:`start()` looks for a :code:`snapshot`
file in the data directory. If found, it calls :code:`fsm_.restore()` before
replaying any subsequent log entries, so the FSM is always in a consistent
state by the time :code:`start()` returns.

.. code-block:: cpp

   CountingFsm fsm;
   mraft::MochiRaftServer server(engine, abt_io, 1, pool, pool, data_dir, fsm);
   server.set_snapshot_threshold(512);
   // No bootstrap() call on restart — just start().
   server.start();
   // fsm.count() now reflects the persisted snapshot plus any log entries
   // that were committed after the snapshot was taken.

Log availability and entries()
--------------------------------

After a snapshot, the log entries that the snapshot supersedes are deleted
from disk. Calling :code:`entries()` with a :code:`from` index that falls
before the snapshot base will silently skip the unavailable history.

Use :code:`log_start_index()` to find the first index still present in
storage before creating an iterator:

.. code-block:: cpp

   raft_index safe_from = std::max(from, server.log_start_index());
   auto it = server.entries(safe_from);

:code:`log_start_index()` returns 1 if no snapshot has ever been taken.

Complete example
-----------------

.. literalinclude:: ../../examples/mochi-raft/07_snapshot/snapshot.cpp
   :language: cpp
