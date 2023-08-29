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
