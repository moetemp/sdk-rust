
local chunk = 1000
for i = 4, #ARGV, chunk do
  redis.call('RPUSH', KEYS[1], unpack(ARGV, i, math.min(i + chunk - 1, #ARGV)))
end
redis.call('PEXPIRE', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
if redis.call('PTTL', KEYS[2]) < tonumber(ARGV[1]) then
  redis.call('PEXPIRE', KEYS[2], ARGV[1])
end
