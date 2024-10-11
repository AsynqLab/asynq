-- enqueueUniqueCmd enqueues the task message if the task is unique.
--
-- KEYS[1] -> unique key
-- KEYS[2] -> asynq:{<queueName>}:t:<taskid>
-- KEYS[3] -> asynq:{<queueName>}:pending
-- --
-- ARGV[1] -> task ID
-- ARGV[2] -> uniqueness lock TTL
-- ARGV[3] -> task message data
-- ARGV[4] -> current unix time in nsec
--
-- Output:
-- Returns 1 if successfully enqueued
-- Returns 0 if task ID conflicts with another task
-- Returns -1 if task unique key already exists
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
    return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
    return 0
end
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", "pending", "pending_since", ARGV[4], "unique_key", KEYS[1])
redis.call("LPUSH", KEYS[3], ARGV[1])
return 1
