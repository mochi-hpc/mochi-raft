# This test applies 3 entries to the state machine,
# performs a barrier, then checks that the content
# of the state machine is as expected in all workers.
from raft import cluster
import time

expected = ""
for i in range(0, 3):
    entry = f"entry_{i}"
    cluster[i+1].apply(entry)
    expected = expected + entry

cluster[1].barrier()

time.sleep(1)

for i in range(0,3):
  content = cluster[i+1].get_fsm_content()
  assert content == expected, f"in worker {i+1} content: {content} differs from {expected}"
