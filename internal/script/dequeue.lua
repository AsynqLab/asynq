-- Input:
-- KEYS[1] -> asynq:{<queueName>}:pending
-- KEYS[2] -> asynq:{<queueName>}:paused
-- KEYS[3] -> asynq:{<queueName>}:active
-- KEYS[4] -> asynq:{<queueName>}:lease
-- --
-- ARGV[1] -> initial lease expiration Unix time
-- ARGV[2] -> task key prefix
--
-- Output:
-- Returns nil if no processable task is found in the given queue.
-- Returns an encoded TaskMessage.
--
-- Note: dequeueCmd checks whether a queue is paused first, before
-- calling RPOPLPUSH to pop a task from the queue.
if redis.call("EXISTS", KEYS[2]) == 0 then
    local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
    if id then
        local key = ARGV[2] .. id
        redis.call("HSET", key, "state", "active")
        redis.call("HDEL", key, "pending_since")
        redis.call("ZADD", KEYS[4], ARGV[1], id)
        return redis.call("HGET", key, "msg")
    end
end
return nil
