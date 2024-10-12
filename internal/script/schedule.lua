-- KEYS[1] -> asynq:{<queueName>}:t:<task_id>
-- KEYS[2] -> asynq:{<queueName>}:scheduled
-- -------
-- ARGV[1] -> task message data
-- ARGV[2] -> process_at time in Unix time
-- ARGV[3] -> task ID
--
-- Output:
-- Returns 1 if successfully enqueued
-- Returns 0 if task ID already exists
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "scheduled")
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
