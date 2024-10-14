-- KEYS[1]  -> asynq:servers:{<host:pid:sid>}
-- KEYS[2]  -> asynq:workers:{<host:pid:sid>}
-- ARGV[1]  -> TTL in seconds
-- ARGV[2]  -> server info
-- ARGV[3:] -> alternate key-value pair of (worker id, worker data)
-- Note: Add key to ZSET with expiration time as score.
-- ref: https:--github.com/antirez/redis/issues/135#issuecomment-2361996
redis.call("SETEX", KEYS[1], ARGV[1], ARGV[2])
redis.call("DEL", KEYS[2])
for i = 3, table.getn(ARGV)-1, 2 do
	redis.call("HSET", KEYS[2], ARGV[i], ARGV[i+1])
end
redis.call("EXPIRE", KEYS[2], ARGV[1])
return redis.status_reply("OK")
