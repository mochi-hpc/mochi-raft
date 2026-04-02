Architecture Overview
=====================

Raft is a consensus algorithm designed to be understandable and correct. A
Raft cluster consists of an odd number of servers (typically 3 or 5). One
server is elected *leader* and handles all log entries submitted by clients.
The leader replicates entries to a majority of the cluster before committing
them; committed entries are then applied to each server's finite state machine
(FSM) in log order. If the leader fails, the remaining servers elect a new one
within a configurable timeout.

mochi-raft wraps the `c-raft <https://raft.readthedocs.io/>`_ library's
step-based API with Mochi middleware components, giving Raft the same
high-performance networking and I/O capabilities used throughout the Mochi
ecosystem.

Three-layer architecture
------------------------

**MochiRaftServer**
  The public C++ class that users instantiate. It owns the event loop (an
  Argobots ULT running on the caller-supplied pool) and wires the three layers
  together. The event loop drives :code:`raft_step()` with timer and network
  events, processes the resulting updates in strict order, and invokes the FSM.

**Network (Thallium)**
  A :code:`tl::provider<Network>` that registers Thallium RPCs for each c-raft
  message type: RequestVote, AppendEntries, InstallSnapshot, and TimeoutNow.
  Large AppendEntries payloads can optionally be delivered via RDMA bulk
  transfer, avoiding an extra copy through Mercury's eager send buffer.

**Storage (ABT-IO)**
  Provides durable persistence of the Raft metadata (current term, voted-for)
  and the log. Metadata uses a double-buffering scheme — two 32-byte slots
  written alternately — to guarantee crash-safe atomic updates. Log entries
  are written to rotating segment files, with an in-memory cache to assemble
  AppendEntries payloads quickly.

Event loop
----------

The event loop ULT calls :code:`queue_->pop(timeout_ms)` to block until an
event arrives or a Raft timer fires. On return it calls :code:`raft_step()`
with the event, then processes the resulting :code:`raft_update` in strict
order:

1. Persist metadata changes (current term, voted-for server).
2. Persist new log entries to the segment file.
3. Send outgoing network messages to peers.
4. Apply newly committed entries to the FSM.
5. If enough entries have accumulated since the last snapshot, take a new
   snapshot and compact the log (see :doc:`11_snapshot`).

This ordering guarantees that the persistent log is always consistent before
any peer receives a message or the FSM is updated — a key safety property of
the Raft protocol.

**Storage** is also extended beyond the log: when a snapshot is taken it is
written atomically to a :code:`snapshot` file alongside the segment files.
On restart, if a snapshot is present it is loaded and passed to c-raft before
any log entries are replayed, so the FSM starts from the correct base state.
