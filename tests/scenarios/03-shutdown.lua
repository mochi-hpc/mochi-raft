-- This test applies 3 entries to the state machine,
-- performs a barrier, adds a new process, adds more
-- entries, removes the added process, adds more entries,
-- then check that the content of the state machine is
-- as expected in all remaining workers.

print("[lua] Applying 3 commands")
expected = ""
for i=1,3 do
  local entry = string.format("entry_%d", i)
  cluster[i]:apply(entry)
  expected = expected .. entry
end

print("[lua] Issuing barrier")
cluster[1]:barrier()

print("[lua] Creating new worker")
new_worker = cluster.spawn()
assert(new_worker.raft_id == 4)

print("[lua] Starting new worker")
new_worker:start()

print("[lua] Adding the new worker to the cluster")
cluster[1]:add(new_worker)

print("[lua] Assigning the new worker as voter")
cluster[1]:assign(new_worker, Role.VOTER)

print("[lua] Applying 4 more commands")
for i=1,4 do
  local entry = string.format("entry_%d", i+3)
  cluster[i]:apply(entry)
  expected = expected .. entry
end

print("[lua] Issuing barrier")
cluster[1]:barrier()

print("[lua] Shutting down worker")
new_worker:shutdown()
assert(#cluster == 3)

print("[lua] Applying 3 commands")
for i=1,3 do
  local entry = string.format("entry_%d", i+7)
  cluster[i]:apply(entry)
  expected = expected .. entry
end

print("[lua] Issuing barrier")
cluster[1]:barrier()

print("[lua] Checking the content of the state machines")
print("[lua] Expected content: " .. expected)
for i=1,3 do
  content = cluster[i]:get_fsm_content()
  print(string.format("[lua] Content in worker %d is %s (length %d)", i, content, #content))
  assert(content == expected)
end
