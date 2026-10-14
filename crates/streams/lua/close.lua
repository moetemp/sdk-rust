
redis.call('HSET', KEYS[1], 'closed', '1')
if not redis.call('HGET', KEYS[1], 'close') then
  redis.call('HSET', KEYS[1], 'close', ARGV[1])
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
