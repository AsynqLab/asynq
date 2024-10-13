-- KEYS[1] -> asynq:{<queueName>}:t:<task_id>
-- KEYS[2] -> asynq:{<queueName>}:active
-- KEYS[3] -> asynq:{<queueName>}:lease
-- KEYS[4] -> asynq:{<queueName>}:retry
-- KEYS[5] -> asynq:{<queueName>}:processed:<yyyy-mm-dd>
-- KEYS[6] -> asynq:{<queueName>}:failed:<yyyy-mm-dd>
-- KEYS[7] -> asynq:{<queueName>}:processed
-- KEYS[8] -> asynq:{<queueName>}:failed
-- -------
-- ARGV[1] -> task ID
-- ARGV[2] -> updated base.TaskMessage value
-- ARGV[3] -> retry_at UNIX timestamp
-- ARGV[4] -> stats expiration timestamp
-- ARGV[5] -> is_failure (bool)
-- ARGV[6] -> max int64 value
if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
    return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
    return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "retry")
if tonumber(ARGV[5]) == 1 then
    local n = redis.call("INCR", KEYS[5])
    if tonumber(n) == 1 then
        redis.call("EXPIREAT", KEYS[5], ARGV[4])
    end
    local m = redis.call("INCR", KEYS[6])
    if tonumber(m) == 1 then
        redis.call("EXPIREAT", KEYS[6], ARGV[4])
    end
    local total = redis.call("GET", KEYS[7])
    if tonumber(total) == tonumber(ARGV[6]) then
        redis.call("SET", KEYS[7], 1)
        redis.call("SET", KEYS[8], 1)
    else
        redis.call("INCR", KEYS[7])
        redis.call("INCR", KEYS[8])
    end
end
return redis.status_reply("OK")
