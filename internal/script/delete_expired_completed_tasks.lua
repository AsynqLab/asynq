-- KEYS[1] -> asynq:{<queueName>}:completed
-- ARGV[1] -> current time in unix time
-- ARGV[2] -> task key prefix
-- ARGV[3] -> batch size (i.e. maximum number of tasks to delete)
--
-- Returns the number of tasks deleted.
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[3]))
for _, id in ipairs(ids) do
    redis.call("DEL", ARGV[2] .. id)
    redis.call("ZREM", KEYS[1], id)
end
return table.getn(ids)
