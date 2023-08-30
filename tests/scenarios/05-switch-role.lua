-- This test applies 3 entries to the state machine,
-- performs a barrier, then changes the role of a worker
-- to "spare", applies 3 more entries, and changes the
-- role of the worker back to "voter", waits a bit,
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

print("[lua] Changing worker 3 into a spare")
cluster[1]:assign(cluster[3], Role.SPARE)

print("[lua] Applying 2 more commands")
for i=1,2 do
  local entry = string.format("entry_%d", i+3)
  cluster[i]:apply(entry)
  expected = expected .. entry
end

print("[lua] Barrier")
cluster[1]:barrier()

print("[lua] Changing worker 3 into a voter")
cluster[1]:assign(cluster[3], Role.VOTER)

print("[lua] Sleeping for 2 seconds")
sleep(2000)

print("[lua] Checking state machine in worker 3")
content = cluster[3]:get_fsm_content()
assert(content == expected)

