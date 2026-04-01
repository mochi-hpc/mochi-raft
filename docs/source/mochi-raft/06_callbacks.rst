Completion Callbacks
====================

:code:`submit()` accepts an optional :code:`std::function<void(int)>` as its
third argument. The callback is invoked exactly once when the outcome of the
submitted entry is known.

.. code-block:: cpp

   server.submit(
       mraft::MochiRaftBuffer{data, strlen(data)},
       /*forward=*/true,
       [](int rv) {
           if (rv == 0)
               std::cout << "Entry applied successfully." << std::endl;
           else
               std::cout << "Entry failed: " << rv << std::endl;
       });

The callback receives:

* :code:`0` — the entry has been applied to the FSM on this server.
* :code:`RAFT_NOTLEADER` — the server lost leadership before the entry could be
  committed; the entry has been rolled back and will not be applied.

.. important::
   The callback is invoked from the event loop ULT, **not** from the thread
   that called :code:`submit()`. Any shared state accessed from the callback
   must be protected with :code:`std::atomic` or :code:`thallium::mutex`.
   Never use :code:`std::mutex` inside a callback — it is not safe in an
   Argobots ULT context.

Multiple callbacks
------------------

Callbacks for different entries fire in log order as the commit index advances.
Each callback fires at most once. If the server shuts down with pending entries,
those callbacks are *not* fired — the entries are simply not committed. Design
your application accordingly, for example by retrying submissions that do not
produce a callback within a reasonable deadline.

Complete example
----------------

.. literalinclude:: ../../examples/mochi-raft/04_callbacks/callbacks.cpp
   :language: cpp
