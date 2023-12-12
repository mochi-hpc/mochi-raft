# This test applies 3 entries to the state machine,
# performs a barrier, then changes the role of a worker
# to "spare", applies 3 more entries, and changes the
# role of the worker back to "voter", waits a bit,
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

print("[python] Changing worker 3 into a spare")
cluster[1].assign(cluster[3], raft.SPARE)

print("[python] Applying 2 more commands")
for i in range(0, 2):
  entry = f"entry_{i+3}"
  cluster[i+1].apply(entry)
  expected = expected + entry

print("[python] Barrier")
cluster[1].barrier()

print("[python] Changing worker 3 into a voter")
cluster[1].assign(cluster[3], raft.VOTER)

print("[python] Sleeping for 2 seconds")
time.sleep(2)

print("[python] Checking state machine in worker 3")
content = cluster[3].get_fsm_content()
assert content == expected, f"content in worker 3 ({content}) should be {expected}"

