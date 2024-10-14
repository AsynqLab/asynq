-- KEYS[1]  -> asynq:schedulers:{<schedulerID>}
-- ARGV[1]  -> TTL in seconds
-- ARGV[2:] -> schedler entries
redis.call("DEL", KEYS[1])
for i = 2, #ARGV do
	redis.call("LPUSH", KEYS[1], ARGV[i])
end
redis.call("EXPIRE", KEYS[1], ARGV[1])
return redis.status_reply("OK")
