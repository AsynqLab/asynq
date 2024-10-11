-- enqueueCmd enqueues a given task message.
--
-- Input:
-- KEYS[1] -> asynq:{<queueName>}:t:<task_id>
-- KEYS[2] -> asynq:{<queueName>}:pending
-- --
-- ARGV[1] -> task message data
-- ARGV[2] -> task ID
-- ARGV[3] -> current unix time in nsec
--
-- Output:
-- Returns 1 if successfully enqueued
-- Returns 0 if task ID already exists
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "pending", "pending_since", ARGV[3])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
