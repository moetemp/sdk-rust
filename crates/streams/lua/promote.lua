
local items = redis.call('LRANGE', KEYS[1], 0, -1)
local retention, grace = tonumber(ARGV[1]), tonumber(ARGV[2])
local slots = {}
for i = 4, #ARGV do
  slots[ARGV[i]] = {log = KEYS[2 * (i - 3) + 1], meta = KEYS[2 * (i - 3) + 2], added = 0}
end
for i = 1, #items, 2 do
  if not slots[items[i]] then
    return redis.error_reply('STREAMS_TOPIC the stage holds topic ' .. items[i] ..
      ', which the promotion did not name')
  end
end
local was_pending = redis.call('HDEL', KEYS[2], ARGV[3])
if #items == 0 then
  if was_pending == 1 then
    return -1
  end
  return 0
end
for _, slot in pairs(slots) do
  revive(slot.log, slot.meta)
end
for i = 1, #items, 2 do
  local slot = slots[items[i]]
  slot.last = redis.call('XADD', slot.log, '*', 'r', items[i + 1])
  slot.added = slot.added + 1
end
for _, slot in pairs(slots) do
  if slot.added > 0 then
    keep(slot.log, slot.meta, slot.added, slot.last, retention, grace)
  end
end
redis.call('DEL', KEYS[1])
return #items / 2
