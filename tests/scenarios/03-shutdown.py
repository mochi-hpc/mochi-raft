# This test applies 3 entries to the state machine,
# performs a barrier, adds a new process, adds more
# entries, removes the added process, adds more entries,
# then check that the content of the state machine is
# as expected in all remaining workers.
import raft
from raft import cluster
import time

print("[python] Applying 3 commands")
expected = ""
for i in range(0, 3):
    entry = f"entry_{i}"
    cluster[i+1].apply(entry)
    expected = expected + entry

print("[python] Issuing barrier")
cluster[1].barrier()

print("[python] Creating new worker")
new_worker = cluster.spawn()
assert new_worker.raft_id == 4, "New worker's raft ID should be 4"

print("[python] Starting new worker")
new_worker.start()

print("[python] Adding the new worker to the cluster")
cluster[1].add(new_worker)

print("[python] Assigning the new worker as voter")
cluster[1].assign(new_worker, raft.VOTER)

print("[python] Applying 4 more commands")
for i in range(0, 4):
    entry = f"entry_{i+3}"
    cluster[i+1].apply(entry)
    expected = expected + entry

print("[python] Issuing barrier")
cluster[1].barrier()

print("[python] Shutting down worker")
new_worker.shutdown()
assert len(cluster) == 3, "Cluster should have 3 workers"

print("[python] Applying 3 commands")
for i in range(0, 3):
    entry = f"entry_{i+7}"
    cluster[i+1].apply(entry)
    expected = expected + entry

print("[python] Issuing barrier")
cluster[1].barrier()

print("[python] Checking the content of the state machines")
print(f"[python] Expected content: {expected}")
for i in range(0, 3):
  content = cluster[i+1].get_fsm_content()
  print(f"[python] Content in worker {i+1} is {content}")
  assert content == expected, f"in worker {i+1} content: {content} differs from {expected}"
