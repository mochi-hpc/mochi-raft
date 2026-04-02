Forward-Submit from Followers
==============================

By default, :code:`submit()` forwards the entry to the current leader when
called on a follower. This allows clients to contact any cluster member
without needing to track which server is the leader.

The :code:`forward` parameter
------------------------------

:code:`submit()` accepts a :code:`bool forward` parameter (second argument,
default :code:`true`) that controls this behavior:

.. code-block:: cpp

   // Forward to the leader transparently (default behavior).
   server.submit(buf, /*forward=*/true, callback);

   // Fail immediately if this server is not the leader.
   int rv = server.submit(buf, /*forward=*/false);
   if (rv == RAFT_NOTLEADER) { /* retry on a different server */ }

When :code:`forward=true` and the server is not the leader:

1. :code:`submit()` returns :code:`0` immediately.
2. The entry is queued for forwarding in the event loop.
3. The event loop sends the entry to the leader via a Thallium RPC.
4. The leader commits the entry and replies with the result code.
5. The callback (if any) fires on the originating follower with the result.

If no leader is currently known (for example during an election in progress),
:code:`submit()` returns :code:`RAFT_NOTLEADER` even when :code:`forward=true`.

.. warning::
   The completion callback fires from an RPC handler ULT on the follower, not
   from the event loop ULT. The same thread-safety rules apply: use
   :code:`std::atomic` or :code:`thallium::mutex` to protect shared state.

Complete example
----------------

.. literalinclude:: ../../examples/mochi-raft/05_forward/forward.cpp
   :language: cpp
