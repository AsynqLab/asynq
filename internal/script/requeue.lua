-- KEYS[1] -> asynq:{<queueName>}:active
-- KEYS[2] -> asynq:{<queueName>}:lease
-- KEYS[3] -> asynq:{<queueName>}:pending
-- KEYS[4] -> asynq:{<queueName>}:t:<task_id>
-- ARGV[1] -> task ID
-- Note: Use RPUSH to push to the head of the queue.
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
    return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
    return redis.error_reply("NOT FOUND")
end
redis.call("RPUSH", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[4], "state", "pending")
return redis.status_reply("OK")
