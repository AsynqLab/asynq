-- KEYS[1] -> asynq:{<queueName>}:t:<task_id>
-- KEYS[2] -> asynq:{<queueName>}:g:<group_key>
-- KEYS[3] -> asynq:{<queueName>}:groups
-- KEYS[4] -> unique key
-- -------
-- ARGV[1] -> task message data
-- ARGV[2] -> task ID
-- ARGV[3] -> current time in Unix time
-- ARGV[4] -> group key
-- ARGV[5] -> uniqueness lock TTL
--
-- Output:
-- Returns 1 if successfully added
-- Returns 0 if task ID already exists
-- Returns -1 if task unique key already exists
local ok = redis.call("SET", KEYS[4], ARGV[2], "NX", "EX", ARGV[5])
if not ok then
    return -1
end
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "aggregating", "group", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
redis.call("SADD", KEYS[3], ARGV[4])
return 1
