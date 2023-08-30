-- This test applies 3 entries to the state machine,
-- performs a barrier, kills a process, adds more
-- entries, then check that the content of the state
-- machine is as expected in all remaining workers.

print("[lua] Applying 3 commands")
expected = ""
for i=1,3 do
  local entry = string.format("entry_%d", i)
  cluster[i]:apply(entry)
  expected = expected .. entry
end

print("[lua] Issuing barrier")
cluster[1]:barrier()

print("[lua] Checking who the leader is")
leader = cluster[1]:get_leader()
leader_id = leader.raft_id

remove_id = leader_id + 1
if remove_id == 4 then
  remove_id = 1
end
print(string.format("[lua] Removing worker with id %d", remove_id))
to_remove = cluster[remove_id]
to_remove:kill()
assert(#cluster == 2)

print("[lua] Applying 2 more commands")
for i=1,2 do
  local entry = string.format("entry_%d", i+3)
  cluster[leader_id]:apply(entry)
  expected = expected .. entry
end

print("[lua] Issuing barrier")
cluster[1]:barrier()

print("[lua] Checking the content of the state machines")
print("[lua] Expected content: " .. expected)
for i=1,3 do
  if i == remove_id then
    print(string.format("[lua] Worker %d was removed, not checking its content", i))
  else
    content = cluster[i]:get_fsm_content()
    print(string.format("[lua] Content in worker %d is %s (length %d)", i, content, #content))
    assert(content == expected)
  end
end
