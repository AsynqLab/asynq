-- KEYS[1] -> asynq:servers:{<host:pid:sid>}
-- KEYS[2] -> asynq:workers:{<host:pid:sid>}
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
return redis.status_reply("OK")
