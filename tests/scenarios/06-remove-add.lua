-- This test applies 3 entries to the state machine,
-- performs a barrier, then removes worker 3, applies 2
-- more entries, and adds back worker 3, waits a bit,
-- then check that the worker has the right content.

print("[lua] Make sure worker 1 is the leader")
cluster[1]:transfer(1)
while (cluster[1]:get_leader().raft_id ~= 1) do
  sleep(1000)
end

print("[lua] Applying 3 commands")
expected = ""
for i=1,3 do
  local entry = string.format("entry_%d", i)
  cluster[i]:apply(entry)
  expected = expected .. entry
end

print("[lua] Barrier")
cluster[1]:barrier()

print("[lua] Removing worker 3")
cluster[1]:remove(cluster[3])

print("[lua] Applying 2 more commands")
for i=1,2 do
  local entry = string.format("entry_%d", i+3)
  cluster[i]:apply(entry)
  expected = expected .. entry
end

print("[lua] Barrier")
cluster[1]:barrier()

print("[lua] Adding worker 3 back")
cluster[1]:add(cluster[3])

print("[lua] Making 3 a voter")
cluster[1]:assign(cluster[3], Role.VOTER)

print("[lua] Sleeping for 2 seconds")
sleep(2000)

print("[lua] Checking state machine in worker 3")
content = cluster[3]:get_fsm_content()
assert(content == expected)

