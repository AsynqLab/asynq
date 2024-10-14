-- KEYS[1] -> asynq:{<queueName>}:g:<gname>:<aggregation_set_id>
-- KEYS[2] -> asynq:{<queueName>}:aggregation_sets
-- -------
-- ARGV[1] -> task key prefix
--
-- Output:
-- Redis status reply
--
-- Time Complexity:
-- max(O(N), O(log(M))) with N being the number of tasks in the aggregation set
-- and M being the number of elements in the all-aggregation-sets list.
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
    redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], KEYS[1])
return redis.status_reply("OK")
