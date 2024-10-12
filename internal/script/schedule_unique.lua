-- KEYS[1] -> unique key
-- KEYS[2] -> asynq:{<queueName>}:t:<task_id>
-- KEYS[3] -> asynq:{<queueName>}:scheduled
-- -------
-- ARGV[1] -> task ID
-- ARGV[2] -> uniqueness lock TTL
-- ARGV[3] -> score (process_at timestamp)
-- ARGV[4] -> task message
--
-- Output:
-- Returns 1 if successfully scheduled
-- Returns 0 if task ID already exists
-- Returns -1 if task unique key already exists
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
    return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
    return 0
end
redis.call("HSET", KEYS[2], "msg", ARGV[4], "state", "scheduled", "unique_key", KEYS[1])
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
return 1
