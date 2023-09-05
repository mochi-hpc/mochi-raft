# This test applies 3 entries to the state machine,
# performs a barrier, then removes worker 3, applies 2
# more entries, and adds back worker 3, waits a bit,
# then check that the worker has the right content.

import raft
from raft import cluster
import time

print("[python] Make sure worker 1 is the leader")
cluster[1].transfer(cluster[1])
while (cluster[1].get_leader().raft_id != 1):
  time.sleep(1)

print("[python] Applying 3 commands")
expected = ""
for i in range(0, 3):
    entry = f"entry_{i}"
    cluster[i+1].apply(entry)
    expected = expected + entry

print("[python] Barrier")
cluster[1].barrier()

print("[python] Kill worker 3")
cluster[3].kill()

print("[python] Applying 2 more commands")
for i in range(0, 2):
    entry = f"entry_{i+3}"
    cluster[i+1].apply(entry)
    expected = expected + entry

print("[python] Barrier")
cluster[1].barrier()

print("[python] Officially removing worker 3")
cluster[1].remove(3)

print("[python] Applying 2 more commands")
for i in range(0, 2):
    entry = f"entry_{i+5}"
    cluster[i+1].apply(entry)
    expected = expected + entry

print("[python] Barrier")
cluster[1].barrier()

print("[python] Creating new worker with id 3")
new_worker3 = cluster.spawn(3)
assert new_worker3.raft_id == 3, "New worker's raft ID should be 3"

print("[python] Starting new worker")
new_worker3.start()

print("[python] Adding the new worker 3 to the cluster")
cluster[1].add(new_worker3)

print("[python] Assigning the new worker 3 as voter")
cluster[1].assign(new_worker, raft.VOTER)

print("[python] Sleeping for 2 seconds")
time.sleep(2)

print("[python] Checking state machine in worker 3")
content = cluster[3].get_fsm_content()
assert content == expected, f"content in worker 3 ({content}) should be {expected}"
