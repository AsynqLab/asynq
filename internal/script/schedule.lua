-- KEYS[1] -> asynq:{<queueName>}:t:<task_id>
-- KEYS[2] -> asynq:{<queueName>}:scheduled
-- KEYS[3] -> unique key (optional, only for unique scheduling)
-- -------
-- ARGV[1] -> task message data
-- ARGV[2] -> score (process_at timestamp)
-- ARGV[3] -> task ID
-- ARGV[4] -> uniqueness lock TTL (optional, only for unique scheduling)
--
-- Output:
-- Returns 1 if successfully scheduled
-- Returns 0 if task ID already exists
-- Returns -1 if task unique key already exists (only for unique scheduling)

local is_unique = (#KEYS == 3 and #ARGV == 4)

if is_unique then
    local ok = redis.call("SET", KEYS[3], ARGV[3], "NX", "EX", ARGV[4])
    if not ok then
        return -1
    end
end

if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end

if is_unique then
    redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "scheduled", "unique_key", KEYS[3])
else
    redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "scheduled")
end

redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
