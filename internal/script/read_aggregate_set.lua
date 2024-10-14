-- KEYS[1] -> asynq:{<queueName>}:g:<gname>:<aggregation_set_id>
-- ------
-- ARGV[1] -> task key prefix
--
-- Output:
-- Array of encoded task messages
--
-- Time Complexity:
-- O(N) with N being the number of tasks in the aggregation set.
local msgs = {}
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
    local key = ARGV[1] .. id
    table.insert(msgs, redis.call("HGET", key, "msg"))
end
return msgs
