-- KEYS[1] -> asynq:scheduler_history:<entryID>
-- ARGV[1] -> enqueued_at timestamp
-- ARGV[2] -> serialized SchedulerEnqueueEvent data
-- ARGV[3] -> max number of events to be persisted
redis.call("ZREMRANGEBYRANK", KEYS[1], 0, -ARGV[3])
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
return redis.status_reply("OK")
