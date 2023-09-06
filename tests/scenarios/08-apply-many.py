# This test applies 10 entries to the state machine,
# performs a barrier, then checks the content of the
# state machine. This test is meant to force the abt-io
# log backend to create a second entry file (entry files
# are limited to 1MB by default).
from raft import cluster
import time
import random
import string

def generate_random_string(n):
    characters = string.printable
    random_string = ''.join(random.choice(characters) for _ in range(n))
    return random_string

expected = ""
for i in range(0, 10):
    entry = generate_random_string(200*1024) # 200 KB entry
    cluster[(i % 3) + 1].apply(entry)
    expected = expected + entry

cluster[1].barrier()

time.sleep(1)

for i in range(0,3):
  content = cluster[i+1].get_fsm_content()
  assert content == expected, f"in worker {i+1} content differs from expected"
