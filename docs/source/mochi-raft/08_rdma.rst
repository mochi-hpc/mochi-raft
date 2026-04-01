RDMA Bulk Transfer
==================

For large log entries (e.g. database pages or checkpoint data), copying the
payload through Mercury's eager send buffer adds latency and memory pressure.
mochi-raft can instead transfer AppendEntries payloads via RDMA bulk transfer,
pulling data directly from the leader's memory into each follower's buffer
without an intermediate copy.

set_rdma_config()
-----------------

RDMA is disabled by default. Enable it with :code:`set_rdma_config()` before
calling :code:`start()`:

.. code-block:: cpp

   server.set_rdma_config(
       /*rdma_threshold=*/65536,   // use RDMA for entries >= 64 KiB
       /*min_timeout_ms=*/5000.0,  // minimum 5 seconds per transfer
       /*timeout_factor=*/0.001);  // +1 ms per KiB of payload

The three parameters are:

* **rdma_threshold** — entries whose total payload (in bytes) equals or exceeds
  this value are sent via RDMA; smaller entries use inline RPC. Default:
  :code:`SIZE_MAX` (RDMA disabled).
* **min_timeout_ms** — the minimum timeout for a single RDMA transfer, in
  milliseconds. Guards against very small payloads getting an unreasonably
  short deadline.
* **timeout_factor** — additional milliseconds of timeout budget granted per
  byte of payload. The effective timeout per transfer is:

  .. code-block:: text

     timeout_s = max(min_timeout_ms, timeout_factor * payload_bytes) / 1000

Choosing a threshold
--------------------

A threshold of 64–256 KiB is a good starting point for most workloads. Below
this size the RPC round-trip overhead dominates and the added complexity of
RDMA is not worth it; above it, RDMA's zero-copy advantage becomes meaningful.

Set :code:`rdma_threshold=0` to route every non-empty entry through RDMA,
which is useful for benchmarking and testing the RDMA path.

Complete example
----------------

.. literalinclude:: ../../examples/mochi-raft/06_rdma/rdma_config.cpp
   :language: cpp
