Submitting Log Entries
======================

Entries are written to the Raft log by calling :code:`submit()`. The library
provides :code:`MochiRaftBuffer`, a lightweight RAII wrapper that manages the
memory region passed to :code:`submit()`.

MochiRaftBuffer
---------------

:code:`MochiRaftBuffer` wraps a :code:`raft_malloc` allocation so that the
Raft library can steal the buffer without copying it. Three constructors are
available:

.. code-block:: cpp

   // Allocate an uninitialized buffer of the given size; fill it yourself.
   MochiRaftBuffer buf(1024);
   memcpy(buf.data(), payload, 1024);

   // Copy from a raw pointer + length.
   MochiRaftBuffer buf(data_ptr, data_size);

   // Copy from a std::string_view.
   MochiRaftBuffer buf(std::string_view{"hello"});

:code:`MochiRaftBuffer` is move-only. Once moved into :code:`submit()`, the
caller's object is left empty and must not be used.

submit()
--------

:code:`submit()` enqueues the entry asynchronously and returns immediately.
The entry will be committed once a majority of the cluster has persisted it,
then applied to each server's FSM.

.. code-block:: cpp

   const char* data = "SET x 42";
   int rv = server.submit(mraft::MochiRaftBuffer{data, strlen(data)});
   // rv == 0            on success
   // rv == RAFT_NOTLEADER  if this server is not the leader

:code:`submit()` returns :code:`RAFT_NOTLEADER` immediately if the server is
not currently the leader and :code:`forward=false` (see :doc:`07_forward` for
transparent forwarding to the leader).

Tracking commitment
-------------------

:code:`commit_index()` returns the highest log index known to be committed.
Poll it to detect when a submitted entry has been replicated across a majority:

.. code-block:: cpp

   raft_index before = server.commit_index();
   server.submit(mraft::MochiRaftBuffer{data, strlen(data)});

   while (server.commit_index() <= before)
       ABT_thread_yield();
   // The entry is committed; the FSM will apply it shortly after.

For precise, per-entry notification use a completion callback instead
(see :doc:`06_callbacks`).

.. note::
   There is a small window between commitment and FSM application. If you need
   to read state from the FSM right after a commit, wait a few more scheduler
   yields or use a callback that fires after :code:`apply()` returns.
