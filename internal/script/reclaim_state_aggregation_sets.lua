-- KEYS[1] -> asynq:{<queueName>}:aggregation_sets
-- -------
-- ARGV[1] -> current time in unix time
local staleSetKeys = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, key in ipairs(staleSetKeys) do
    local idx = string.find(key, ":[^:]*$")
    local groupKey = string.sub(key, 1, idx - 1)
    local res = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
    for i = 1, table.getn(res) - 1, 2 do
        redis.call("ZADD", groupKey, tonumber(res[i + 1]), res[i])
    end
    redis.call("DEL", key)
end
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
return redis.status_reply("OK")
