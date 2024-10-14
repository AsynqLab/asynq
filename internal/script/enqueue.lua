-- KEYS[1] -> asynq:{<queueName>}:t:<task_id>
-- KEYS[2] -> asynq:{<queueName>}:pending
-- KEYS[3] -> unique key (optional, only for unique enqueue)
--
-- ARGV[1] -> task ID
-- ARGV[2] -> task message data
-- ARGV[3] -> current unix time in nsec
-- ARGV[4] -> uniqueness lock TTL (optional, only for unique enqueue)
--
-- Output:
-- Returns 1 if successfully enqueued
-- Returns 0 if task ID already exists
-- Returns -1 if task unique key already exists (only for unique enqueue)

local is_unique = (#KEYS == 3 and #ARGV == 4)

if is_unique then
    -- Unique enqueue logic
    local ok = redis.call("SET", KEYS[3], ARGV[1], "NX", "EX", ARGV[4])
    if not ok then
        return -1
    end
end

if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end

if is_unique then
    redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "pending", "pending_since", ARGV[3], "unique_key", KEYS[3])
else
    redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "pending", "pending_since", ARGV[3])
end

redis.call("LPUSH", KEYS[2], ARGV[1])
return 1
