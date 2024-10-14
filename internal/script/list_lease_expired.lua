-- KEYS[1] -> asynq:{<queueName>}:lease
-- ARGV[1] -> cutoff in unix time
-- ARGV[2] -> task key prefix
local res = {}
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, id in ipairs(ids) do
	local key = ARGV[2] .. id
	local v = redis.call("HGET", key, "msg")
	if v then
		table.insert(res, v)
	end
end
return res
