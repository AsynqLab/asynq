-- KEYS[1] -> asynq:{<queueName>}:active
-- KEYS[2] -> asynq:{<queueName>}:lease
-- KEYS[3] -> asynq:{<queueName>}:t:<task_id>
-- KEYS[4] -> asynq:{<queueName>}:processed:<yyyy-mm-dd>
-- KEYS[5] -> asynq:{<queueName>}:processed
-- KEYS[6] -> unique key
-- -------
-- ARGV[1] -> task ID
-- ARGV[2] -> stats expiration timestamp
-- ARGV[3] -> max int64 value
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
    return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
    return redis.error_reply("NOT FOUND")
end
if redis.call("DEL", KEYS[3]) == 0 then
    return redis.error_reply("NOT FOUND")
end
local n = redis.call("INCR", KEYS[4])
if tonumber(n) == 1 then
    redis.call("EXPIREAT", KEYS[4], ARGV[2])
end
local total = redis.call("GET", KEYS[5])
if tonumber(total) == tonumber(ARGV[3]) then
    redis.call("SET", KEYS[5], 1)
else
    redis.call("INCR", KEYS[5])
end
if redis.call("GET", KEYS[6]) == ARGV[1] then
    redis.call("DEL", KEYS[6])
end
return redis.status_reply("OK")
