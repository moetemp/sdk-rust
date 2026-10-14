
local newest = redis.call('XREVRANGE', KEYS[1], '+', '(' .. ARGV[1], 'COUNT', 1)
if #newest > 0 then
  return newest[1][1]
end
local generation = redis.call('HINCRBY', KEYS[2], 'arm_seq', 1)
redis.call('HSET', KEYS[2], 'arm', generation)
if redis.call('PTTL', KEYS[2]) < 0 then
  redis.call('PEXPIRE', KEYS[2], ARGV[2])
end
return generation
