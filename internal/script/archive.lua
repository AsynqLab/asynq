-- KEYS[1] -> asynq:{<queueName>}:t:<task_id>
-- KEYS[2] -> asynq:{<queueName>}:active
-- KEYS[3] -> asynq:{<queueName>}:lease
-- KEYS[4] -> asynq:{<queueName>}:archived
-- KEYS[5] -> asynq:{<queueName>}:processed:<yyyy-mm-dd>
-- KEYS[6] -> asynq:{<queueName>}:failed:<yyyy-mm-dd>
-- KEYS[7] -> asynq:{<queueName>}:processed
-- KEYS[8] -> asynq:{<queueName>}:failed
-- -------
-- ARGV[1] -> task ID
-- ARGV[2] -> updated base.TaskMessage value
-- ARGV[3] -> died_at UNIX timestamp
-- ARGV[4] -> cutoff timestamp (e.g., 90 days ago)
-- ARGV[5] -> max number of tasks in archive (e.g., 100)
-- ARGV[6] -> stats expiration timestamp
-- ARGV[7] -> max int64 value
if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
    return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
    return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
redis.call("ZREMRANGEBYSCORE", KEYS[4], "-inf", ARGV[4])
redis.call("ZREMRANGEBYRANK", KEYS[4], 0, -ARGV[5])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "archived")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
    redis.call("EXPIREAT", KEYS[5], ARGV[6])
end
local m = redis.call("INCR", KEYS[6])
if tonumber(m) == 1 then
    redis.call("EXPIREAT", KEYS[6], ARGV[6])
end
local total = redis.call("GET", KEYS[7])
if tonumber(total) == tonumber(ARGV[7]) then
    redis.call("SET", KEYS[7], 1)
    redis.call("SET", KEYS[8], 1)
else
    redis.call("INCR", KEYS[7])
    redis.call("INCR", KEYS[8])
end
return redis.status_reply("OK")
