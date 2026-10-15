local function extend(key, ms, create)
  local ttl = redis.call('PTTL', key)
  if (ttl >= 0 or (create and ttl == -1)) and ttl < ms then
    redis.call('PEXPIRE', key, ms)
  end
end
local lease, grace = tonumber(ARGV[3]), tonumber(ARGV[4])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2] .. '|' .. (clock_ms() + lease))
if redis.call('EXISTS', KEYS[3]) == 1 then
  redis.call('HSET', KEYS[3], 'archived', ARGV[2])
  if KEYS[4] and redis.call('HGET', KEYS[3], 'last') == ARGV[2] then
    redis.call('ZREM', KEYS[4], KEYS[2])
  end
end
extend(KEYS[1], lease, true)
extend(KEYS[2], lease, false)
extend(KEYS[3], lease + grace, false)
return 1
