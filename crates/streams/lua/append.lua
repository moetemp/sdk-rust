
local retention, grace = tonumber(ARGV[1]), tonumber(ARGV[2])
local field, digest_arg = ARGV[3], ARGV[5]
local held = redis.call('HGET', KEYS[2], field)
local sequence = tonumber(ARGV[4])
if held then
  local held_sequence, held_count, first, last, digest =
    string.match(held, '^(%d+)|(%d+)|([^|]+)|([^|]+)|(%x+)$')
  held_sequence = tonumber(held_sequence)
  local next_sequence = held_sequence + tonumber(held_count)
  if sequence == held_sequence then
    if digest == digest_arg then
      return {first, last}
    end
    return redis.error_reply('STREAMS_DIVERGENT sequence ' .. ARGV[4] ..
      ' was already written with different content')
  end
  if sequence < next_sequence then
    return redis.error_reply('STREAMS_STALE sequence ' .. ARGV[4] ..
      ' is below the next one expected, ' .. next_sequence)
  end
end
if redis.call('HGET', KEYS[3], 'closed') then
  return redis.error_reply('STREAMS_CLOSED the Workflow that owns this stream ' ..
    'has closed')
end
revive(KEYS[1], KEYS[2])
local first, last
for i = 6, #ARGV do
  last = redis.call('XADD', KEYS[1], '*', 'r', ARGV[i])
  if not first then
    first = last
  end
end
redis.call('HSET', KEYS[2], field,
  ARGV[4] .. '|' .. (#ARGV - 5) .. '|' .. first .. '|' .. last .. '|' .. digest_arg)
keep(KEYS[1], KEYS[2], #ARGV - 5, last, retention, grace)
return {first, last}
