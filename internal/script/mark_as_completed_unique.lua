-- KEYS[1] -> asynq:{<queueName>}:active
-- KEYS[2] -> asynq:{<queueName>}:lease
-- KEYS[3] -> asynq:{<queueName>}:completed
-- KEYS[4] -> asynq:{<queueName>}:t:<task_id>
-- KEYS[5] -> asynq:{<queueName>}:processed:<yyyy-mm-dd>
-- KEYS[6] -> asynq:{<queueName>}:processed
-- KEYS[7] -> asynq:{<queueName>}:unique:{<checksum>}
--
-- ARGV[1] -> task ID
-- ARGV[2] -> stats expiration timestamp
-- ARGV[3] -> task expiration time in unix time
-- ARGV[4] -> task message data
-- ARGV[5] -> max int64 value
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
    return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
    return redis.error_reply("NOT FOUND")
end
if redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1]) ~= 1 then
    return redis.error_reply("INTERNAL")
end
redis.call("HSET", KEYS[4], "msg", ARGV[4], "state", "completed")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
    redis.call("EXPIREAT", KEYS[5], ARGV[2])
end
local total = redis.call("GET", KEYS[6])
if tonumber(total) == tonumber(ARGV[5]) then
    redis.call("SET", KEYS[6], 1)
else
    redis.call("INCR", KEYS[6])
end
if redis.call("GET", KEYS[7]) == ARGV[1] then
    redis.call("DEL", KEYS[7])
end
return redis.status_reply("OK")
