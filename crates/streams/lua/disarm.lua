
if redis.call('HGET', KEYS[1], 'arm') == ARGV[1] then
  return redis.call('HDEL', KEYS[1], 'arm')
end
return 0
