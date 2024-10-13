-- KEYS[1] -> source queue (e.g. asynq:{<queueName>:scheduled or asynq:{<queueName>}:retry})
-- KEYS[2] -> asynq:{<queueName>}:pending
-- ARGV[1] -> current unix time in seconds
-- ARGV[2] -> task key prefix
-- ARGV[3] -> current unix time in nsec
-- ARGV[4] -> group key prefix
-- Note: Script moves tasks up to 100 at a time to keep the runtime of script short.
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, id in ipairs(ids) do
    local taskKey = ARGV[2] .. id
    local group = redis.call("HGET", taskKey, "group")
    if group and group ~= '' then
        redis.call("ZADD", ARGV[4] .. group, ARGV[1], id)
        redis.call("ZREM", KEYS[1], id)
        redis.call("HSET", taskKey,
            "state", "aggregating")
    else
        redis.call("LPUSH", KEYS[2], id)
        redis.call("ZREM", KEYS[1], id)
        redis.call("HSET", taskKey,
            "state", "pending",
            "pending_since", ARGV[3])
    end
end
return table.getn(ids)
