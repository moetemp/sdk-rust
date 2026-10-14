local now = clock_ms()
local lease, grace = tonumber(ARGV[3]), tonumber(ARGV[4])
local id = ARGV[2]
local held_value = redis.call('HGET', KEYS[1], ARGV[1])
if held_value then
  local old = string.match(held_value, '^(%d+%-%d+)|')
  if old and id_below(old, id) then
    id = old
  end
end
redis.call('HSET', KEYS[1], ARGV[1], id .. '|' .. (now + lease))
local function extend(key, ms, create)
  local ttl = redis.call('PTTL', key)
  if (ttl >= 0 or (create and ttl == -1)) and ttl < ms then
    redis.call('PEXPIRE', key, ms)
  end
end
extend(KEYS[1], lease, true)
extend(KEYS[2], lease, false)
extend(KEYS[3], lease + grace, false)
return 1
