# This test applies 10 entries to the state machine,
# performs a barrier, then checks the content of the
# state machine. This test is meant to force the abt-io
# log backend to create a second entry file (entry files
# are limited to 1MB by default).
from raft import cluster
import time


def generate_random_string(n):
    import random
    letters = list('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
    random_string = ''.join(random.choice(letters) for _ in range(n))
    return random_string

expected = ""
for i in range(0, 200):
    s = 10*1024 # 10 KB entry
    print(f"[python] applying an entry of size {s} via worker {(i%3)+1}")
    entry = generate_random_string(s)
    while not cluster[(i % 3) + 1].apply(entry):
        time.sleep(1)
    expected = expected + entry
    time.sleep(0.2)

for i in range(0, 3):
    for j in range(0, 10):
        print(f"[python] executing barrier in worker {i+1} (attempt {j})")
        if cluster[i+1].barrier():
            break
        time.sleep(1)

print(f"[python] sleeping for 5 seconds")
time.sleep(30)

for i in range(0,3):
    content = cluster[i+1].get_fsm_content()
    if content is None:
        print(content)
    else:
        print(content[0:10])
    index = next((j for j, (char1, char2) in enumerate(zip(content, expected)) if char1 != char2), None)
    assert content == expected, f"in worker {i+1} content (len={len(content)} differs from expected (len={len(expected)}) at character {index}"
