-- memory,abt-io
-- This test applies 3 entries to the state machine,
-- performs a barrier, then checks that the content
-- of the state machine is as expected in all workers.

expected = ""
for i=1,3 do
  local entry = string.format("entry_%d", i)
  cluster[i]:apply(entry)
  expected = expected .. entry
end
cluster[1]:barrier()
for i=1,3 do
  content = cluster[i]:get_fsm_content()
  assert(content == expected)
end
