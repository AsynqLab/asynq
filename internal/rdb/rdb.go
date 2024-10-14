// Package rdb encapsulates the interactions with redis.
package rdb

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/AsynqLab/asynq/internal/base"
	"github.com/AsynqLab/asynq/internal/errors"
	"github.com/AsynqLab/asynq/internal/script"
	"github.com/AsynqLab/asynq/internal/timeutil"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

const statsTTL = 90 * 24 * time.Hour // 90 days

// LeaseDuration is the duration used to initially create a lease and to extend it thereafter.
const LeaseDuration = 30 * time.Second

// RDB is a client interface to query and mutate task queues.
type RDB struct {
	client redis.UniversalClient
	clock  timeutil.Clock
}

// NewRDB returns a new instance of RDB.
func NewRDB(client redis.UniversalClient) *RDB {
	return &RDB{
		client: client,
		clock:  timeutil.NewRealClock(),
	}
}

// Close closes the connection with redis server.
func (r *RDB) Close() error {
	return r.client.Close()
}

// Client returns the reference to underlying redis client.
func (r *RDB) Client() redis.UniversalClient {
	return r.client
}

// SetClock sets the clock used by RDB to the given clock.
//
// Use this function to set the clock to SimulatedClock in tests.
func (r *RDB) SetClock(c timeutil.Clock) {
	r.clock = c
}

// Ping checks the connection with redis server.
func (r *RDB) Ping() error {
	return r.client.Ping(context.Background()).Err()
}

func (r *RDB) runScript(ctx context.Context, op errors.Op, script *redis.Script, keys []string, args ...interface{}) error {
	if err := script.Run(ctx, r.client, keys, args...).Err(); err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}
	return nil
}

// Runs the given script with keys and args and returns the script's return value as int64.
func (r *RDB) runScriptWithErrorCode(ctx context.Context, op errors.Op, script *redis.Script, keys []string, args ...interface{}) (int64, error) {
	res, err := script.Run(ctx, r.client, keys, args...).Result()
	if err != nil {
		return 0, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}
	n, ok := res.(int64)
	if !ok {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from Lua script: %v", res))
	}
	return n, nil
}

// Enqueue adds the given task to the pending list of the queue.
func (r *RDB) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Enqueue"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.PendingKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		encoded,
		r.clock.Now().UnixNano(),
	}
	n, err := r.runScriptWithErrorCode(ctx, op, script.EnqueueCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// EnqueueUnique inserts the given task if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RDB) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration) error {
	var op errors.Op = "rdb.EnqueueUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, "cannot encode task message: %v", err)
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.PendingKey(msg.Queue),
		msg.UniqueKey,
	}
	argv := []interface{}{
		msg.ID,
		encoded,
		r.clock.Now().UnixNano(),
		int(ttl.Seconds()),
	}
	n, err := r.runScriptWithErrorCode(ctx, op, script.EnqueueCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == -1 {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// Dequeue queries given queues in order and pops a task message
// off a queue if one exists and returns the message and its lease expiration time.
// Dequeue skips a queue if the queue is paused.
// If all queues are empty, ErrNoProcessableTask error is returned.
func (r *RDB) Dequeue(ctx context.Context, queueNames ...string) (msg *base.TaskMessage, leaseExpirationTime time.Time, err error) {
	var op errors.Op = "rdb.Dequeue"
	for _, queueName := range queueNames {
		keys := []string{
			base.PendingKey(queueName),
			base.PausedKey(queueName),
			base.ActiveKey(queueName),
			base.LeaseKey(queueName),
		}
		leaseExpirationTime = r.clock.Now().Add(LeaseDuration)
		argv := []interface{}{
			leaseExpirationTime.Unix(),
			base.TaskKeyPrefix(queueName),
		}
		res, err := script.DequeueCmd.Run(ctx, r.client, keys, argv...).Result()
		if errors.Is(err, redis.Nil) {
			continue
		} else if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
		}
		encoded, err := cast.ToStringE(res)
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cast error: unexpected return value from Lua script: %v", res))
		}
		if msg, err = base.DecodeMessage([]byte(encoded)); err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
		}
		return msg, leaseExpirationTime, nil
	}
	return nil, time.Time{}, errors.E(op, errors.NotFound, errors.ErrNoProcessableTask)
}

// Done removes the task from active queue and deletes the task.
// It removes a uniqueness lock acquired by the task, if any.
func (r *RDB) Done(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Done"
	now := r.clock.Now()
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
		base.ProcessedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		expireAt.Unix(),
		int64(math.MaxInt64),
	}
	// Note: We cannot pass empty unique key when running this script in redis-cluster.
	if len(msg.UniqueKey) > 0 {
		keys = append(keys, msg.UniqueKey)
	}
	return r.runScript(ctx, op, script.DoneCmd, keys, argv...)
}

// MarkAsComplete removes the task from active queue to mark the task as completed.
// It removes a uniqueness lock acquired by the task, if any.
func (r *RDB) MarkAsComplete(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.MarkAsComplete"
	now := r.clock.Now()
	statsExpireAt := now.Add(statsTTL)
	msg.CompletedAt = now.Unix()
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.CompletedKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
		base.ProcessedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		statsExpireAt.Unix(),
		now.Unix() + msg.Retention,
		encoded,
		int64(math.MaxInt64),
	}
	// Note: We cannot pass empty unique key when running this script in redis-cluster.
	if len(msg.UniqueKey) > 0 {
		keys = append(keys, msg.UniqueKey)
	}
	return r.runScript(ctx, op, script.MarkAsCompleteCmd, keys, argv...)
}

// Requeue moves the task from active queue to the specified queue.
func (r *RDB) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Requeue"
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.PendingKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
	}
	return r.runScript(ctx, op, script.RequeueCmd, keys, msg.ID)
}

func (r *RDB) AddToGroup(ctx context.Context, msg *base.TaskMessage, groupKey string) error {
	var op errors.Op = "rdb.AddToGroup"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.GroupKey(msg.Queue, groupKey),
		base.AllGroups(msg.Queue),
	}
	argv := []interface{}{
		encoded,
		msg.ID,
		r.clock.Now().Unix(),
		groupKey,
	}
	n, err := r.runScriptWithErrorCode(ctx, op, script.AddToGroupCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

func (r *RDB) AddToGroupUnique(ctx context.Context, msg *base.TaskMessage, groupKey string, ttl time.Duration) error {
	var op errors.Op = "rdb.AddToGroupUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.GroupKey(msg.Queue, groupKey),
		base.AllGroups(msg.Queue),
		base.UniqueKey(msg.Queue, msg.Type, msg.Payload),
	}
	argv := []interface{}{
		encoded,
		msg.ID,
		r.clock.Now().Unix(),
		groupKey,
		int(ttl.Seconds()),
	}
	n, err := r.runScriptWithErrorCode(ctx, op, script.AddToGroupCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == -1 {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// Schedule adds the task to the scheduled set to be processed in the future.
func (r *RDB) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	var op errors.Op = "rdb.Schedule"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.ScheduledKey(msg.Queue),
	}
	argv := []interface{}{
		encoded,
		processAt.Unix(),
		msg.ID,
	}
	n, err := r.runScriptWithErrorCode(ctx, op, script.ScheduleCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// ScheduleUnique adds the task to the backlog queue to be processed in the future if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RDB) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	var op errors.Op = "rdb.ScheduleUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode task message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.ScheduledKey(msg.Queue),
		msg.UniqueKey,
	}
	argv := []interface{}{
		encoded,
		processAt.Unix(),
		msg.ID,
		int(ttl.Seconds()),
	}
	n, err := r.runScriptWithErrorCode(ctx, op, script.ScheduleCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == -1 {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// Retry moves the task from active to retry queue.
// It also annotates the message with the given error message and
// if isFailure is true increments the retried counter.
func (r *RDB) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	var op errors.Op = "rdb.Retry"
	now := r.clock.Now()
	modified := *msg
	if isFailure {
		modified.Retried++
	}
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := base.EncodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.RetryKey(msg.Queue),
		base.ProcessedKey(msg.Queue, now),
		base.FailedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
		base.FailedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		encoded,
		processAt.Unix(),
		expireAt.Unix(),
		isFailure,
		int64(math.MaxInt64),
	}
	return r.runScript(ctx, op, script.RetryCmd, keys, argv...)
}

const (
	maxArchiveSize           = 10000 // maximum number of tasks in archive
	archivedExpirationInDays = 90    // number of days before an archived task gets deleted permanently
)

// Archive sends the given task to archive, attaching the error message to the task.
// It also trims the archive by timestamp and set size.
func (r *RDB) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
	var op errors.Op = "rdb.Archive"
	now := r.clock.Now()
	modified := *msg
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := base.EncodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}
	cutoff := now.AddDate(0, 0, -archivedExpirationInDays)
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.ArchivedKey(msg.Queue),
		base.ProcessedKey(msg.Queue, now),
		base.FailedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
		base.FailedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		encoded,
		now.Unix(),
		cutoff.Unix(),
		maxArchiveSize,
		expireAt.Unix(),
		int64(math.MaxInt64),
	}
	return r.runScript(ctx, op, script.ArchiveCmd, keys, argv...)
}

// ForwardIfReady checks scheduled and retry sets of the given queues
// and move any tasks that are ready to be processed to the pending set.
func (r *RDB) ForwardIfReady(ctx context.Context, queueNames ...string) error {
	var op errors.Op = "rdb.ForwardIfReady"
	for _, queueName := range queueNames {
		if err := r.forwardAll(ctx, queueName); err != nil {
			return errors.E(op, errors.CanonicalCode(err), err)
		}
	}
	return nil
}

// forward moves tasks with a score less than the current unix time from the delayed (i.e., scheduled | retry) zset
// to the pending list or group set.
// It returns the number of tasks moved.
func (r *RDB) forward(ctx context.Context, delayedKey, pendingKey, taskKeyPrefix, groupKeyPrefix string) (int, error) {
	now := r.clock.Now()
	keys := []string{delayedKey, pendingKey}
	argv := []interface{}{
		now.Unix(),
		taskKeyPrefix,
		now.UnixNano(),
		groupKeyPrefix,
	}
	res, err := script.ForwardCmd.Run(ctx, r.client, keys, argv...).Result()
	if err != nil {
		return 0, errors.E(errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}
	n, err := cast.ToIntE(res)
	if err != nil {
		return 0, errors.E(errors.Internal, fmt.Sprintf("cast error: Lua script returned unexpected value: %v", res))
	}
	return n, nil
}

// forwardAll checks for tasks in scheduled/retry state that are ready to be run, and updates
// their state to "pending" or "aggregating".
func (r *RDB) forwardAll(ctx context.Context, queueName string) (err error) {
	delayedKeys := []string{base.ScheduledKey(queueName), base.RetryKey(queueName)}
	pendingKey := base.PendingKey(queueName)
	taskKeyPrefix := base.TaskKeyPrefix(queueName)
	groupKeyPrefix := base.GroupKeyPrefix(queueName)
	for _, delayedKey := range delayedKeys {
		n := 1
		for n != 0 {
			n, err = r.forward(ctx, delayedKey, pendingKey, taskKeyPrefix, groupKeyPrefix)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ListGroups returns a list of all known groups in the given queue.
func (r *RDB) ListGroups(queueName string) ([]string, error) {
	var op errors.Op = "RDB.ListGroups"
	groups, err := r.client.SMembers(context.Background(), base.AllGroups(queueName)).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "smembers", Err: err})
	}
	return groups, nil
}

// Task aggregation should finish within this timeout.
// Otherwise, an aggregation set should be reclaimed by the recoverer.
const aggregationTimeout = 2 * time.Minute

// AggregationCheck checks the group identified by the given queue and group name to see if the tasks in the
// group are ready to be aggregated. If so, it moves the tasks to be aggregated to a aggregation set and returns
// the set ID. If not, it returns an empty string for the set ID.
// The time for gracePeriod and maxDelay is computed relative to the time t.
//
// Note: It assumes that this function is called at frequency less than or equal to the gracePeriod. In other words,
// the function only checks the most recently added task against the given gracePeriod.
func (r *RDB) AggregationCheck(queueName, gname string, t time.Time, gracePeriod, maxDelay time.Duration, maxSize int) (string, error) {
	var op errors.Op = "RDB.AggregationCheck"
	aggregationSetID := uuid.NewString()
	expireTime := r.clock.Now().Add(aggregationTimeout)
	keys := []string{
		base.GroupKey(queueName, gname),
		base.AggregationSetKey(queueName, gname, aggregationSetID),
		base.AllAggregationSets(queueName),
		base.AllGroups(queueName),
	}
	argv := []interface{}{
		maxSize,
		int64(maxDelay.Seconds()),
		int64(gracePeriod.Seconds()),
		expireTime.Unix(),
		t.Unix(),
		gname,
	}
	n, err := r.runScriptWithErrorCode(context.Background(), op, script.AggregationCheckCmd, keys, argv...)
	if err != nil {
		return "", err
	}
	switch n {
	case 0:
		return "", nil
	case 1:
		return aggregationSetID, nil
	default:
		return "", errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from lua script: %d", n))
	}
}

// ReadAggregationSet retrieves members of an aggregation set and returns a list of tasks in the set and
// the deadline for aggregating those tasks.
func (r *RDB) ReadAggregationSet(queueName, gname, setID string) ([]*base.TaskMessage, time.Time, error) {
	var op errors.Op = "RDB.ReadAggregationSet"
	ctx := context.Background()
	aggSetKey := base.AggregationSetKey(queueName, gname, setID)
	res, err := script.ReadAggregationSetCmd.Run(ctx, r.client,
		[]string{aggSetKey}, base.TaskKeyPrefix(queueName)).Result()
	if err != nil {
		return nil, time.Time{}, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}
	data, err := cast.ToStringSliceE(res)
	if err != nil {
		return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cast error: Lua script returned unexpected value: %v", res))
	}
	var msgs []*base.TaskMessage
	for _, s := range data {
		msg, err := base.DecodeMessage([]byte(s))
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
		}
		msgs = append(msgs, msg)
	}
	deadlineUnix, err := r.client.ZScore(ctx, base.AllAggregationSets(queueName), aggSetKey).Result()
	if err != nil {
		return nil, time.Time{}, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zscore", Err: err})
	}
	return msgs, time.Unix(int64(deadlineUnix), 0), nil
}

// DeleteAggregationSet deletes the aggregation set and its members identified by the parameters.
func (r *RDB) DeleteAggregationSet(ctx context.Context, queueName, gname, setID string) error {
	var op errors.Op = "RDB.DeleteAggregationSet"
	keys := []string{
		base.AggregationSetKey(queueName, gname, setID),
		base.AllAggregationSets(queueName),
	}
	return r.runScript(ctx, op, script.DeleteAggregationSetCmd, keys, base.TaskKeyPrefix(queueName))
}

// ReclaimStaleAggregationSets checks for any stale aggregation sets in the given queue, and
// reclaim tasks in the stale aggregation set by putting them back in the group.
func (r *RDB) ReclaimStaleAggregationSets(ctx context.Context, queueName string) error {
	var op errors.Op = "RDB.ReclaimStaleAggregationSets"
	return r.runScript(ctx, op, script.ReclaimStateAggregationSetsCmd,
		[]string{base.AllAggregationSets(queueName)}, r.clock.Now().Unix())
}

// DeleteExpiredCompletedTasks checks for any expired tasks in the given queue's completed set,
// and delete all expired tasks.
func (r *RDB) DeleteExpiredCompletedTasks(ctx context.Context, queueName string, batchSize int) error {
	for {
		n, err := r.deleteExpiredCompletedTasks(ctx, queueName, batchSize)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// deleteExpiredCompletedTasks runs the lua script to delete expired deleted task with the specified
// batch size. It reports the number of tasks deleted.
func (r *RDB) deleteExpiredCompletedTasks(ctx context.Context, queueName string, batchSize int) (int64, error) {
	var op errors.Op = "rdb.DeleteExpiredCompletedTasks"
	keys := []string{base.CompletedKey(queueName)}
	argv := []interface{}{
		r.clock.Now().Unix(),
		base.TaskKeyPrefix(queueName),
		batchSize,
	}
	res, err := script.DeleteExpiredCompletedTasksCmd.Run(ctx, r.client, keys, argv...).Result()
	if err != nil {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}
	n, ok := res.(int64)
	if !ok {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from Lua script: %v", res))
	}
	return n, nil
}

// ListLeaseExpired returns a list of task messages with an expired lease from the given queues.
func (r *RDB) ListLeaseExpired(ctx context.Context, cutoff time.Time, queueNames ...string) ([]*base.TaskMessage, error) {
	var op errors.Op = "rdb.ListLeaseExpired"
	var msgs []*base.TaskMessage
	for _, queueName := range queueNames {
		res, err := script.ListLeaseExpiredCmd.Run(ctx, r.client,
			[]string{base.LeaseKey(queueName)},
			cutoff.Unix(), base.TaskKeyPrefix(queueName)).Result()
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
		}
		data, err := cast.ToStringSliceE(res)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("cast error: Lua script returned unexpected value: %v", res))
		}
		for _, s := range data {
			msg, err := base.DecodeMessage([]byte(s))
			if err != nil {
				return nil, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
			}
			msgs = append(msgs, msg)
		}
	}
	return msgs, nil
}

// ExtendLease extends the lease for the given tasks by LeaseDuration (30s).
// It returns a new expiration time if the operation was successful.
func (r *RDB) ExtendLease(ctx context.Context, queueName string, ids ...string) (expirationTime time.Time, err error) {
	expireAt := r.clock.Now().Add(LeaseDuration)
	var zs []redis.Z
	for _, id := range ids {
		zs = append(zs, redis.Z{Member: id, Score: float64(expireAt.Unix())})
	}
	// Use XX option to only update elements that already exist; Don't add new elements
	// TODO: Consider adding GT option to ensure we only "extend" the lease. Caveat is that GT is supported from redis v6.2.0 or above.
	err = r.client.ZAddXX(ctx, base.LeaseKey(queueName), zs...).Err()
	if err != nil {
		return time.Time{}, err
	}
	return expireAt, nil
}

// WriteServerState writes server state data to redis with expiration set to the value ttl.
func (r *RDB) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	var op errors.Op = "rdb.WriteServerState"
	ctx := context.Background()
	bytes, err := base.EncodeServerInfo(info)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode server info: %v", err))
	}
	exp := r.clock.Now().Add(ttl).UTC()
	args := []interface{}{ttl.Seconds(), bytes} // args to the lua script
	for _, w := range workers {
		bytes, err := base.EncodeWorkerInfo(w)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, w.ID, bytes)
	}
	skey := base.ServerInfoKey(info.Host, info.PID, info.ServerID)
	wkey := base.WorkersKey(info.Host, info.PID, info.ServerID)
	if err := r.client.ZAdd(ctx, base.AllServers, redis.Z{Score: float64(exp.Unix()), Member: skey}).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	if err := r.client.ZAdd(ctx, base.AllWorkers, redis.Z{Score: float64(exp.Unix()), Member: wkey}).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zadd", Err: err})
	}
	return r.runScript(ctx, op, script.WriteServerStateCmd, []string{skey, wkey}, args...)
}

// ClearServerState deletes server state data from redis.
func (r *RDB) ClearServerState(host string, pid int, serverID string) error {
	var op errors.Op = "rdb.ClearServerState"
	ctx := context.Background()
	skey := base.ServerInfoKey(host, pid, serverID)
	wkey := base.WorkersKey(host, pid, serverID)
	if err := r.client.ZRem(ctx, base.AllServers, skey).Err(); err != nil {
		return errors.E(op, errors.Internal, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	if err := r.client.ZRem(ctx, base.AllWorkers, wkey).Err(); err != nil {
		return errors.E(op, errors.Internal, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	return r.runScript(ctx, op, script.ClearServerStateCmd, []string{skey, wkey})
}

// WriteSchedulerEntries writes scheduler entries data to redis with expiration set to the value ttl.
func (r *RDB) WriteSchedulerEntries(schedulerID string, entries []*base.SchedulerEntry, ttl time.Duration) error {
	var op errors.Op = "rdb.WriteSchedulerEntries"
	ctx := context.Background()
	args := []interface{}{ttl.Seconds()}
	for _, e := range entries {
		bytes, err := base.EncodeSchedulerEntry(e)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, bytes)
	}
	exp := r.clock.Now().Add(ttl).UTC()
	key := base.SchedulerEntriesKey(schedulerID)
	err := r.client.ZAdd(ctx, base.AllSchedulers, redis.Z{Score: float64(exp.Unix()), Member: key}).Err()
	if err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zadd", Err: err})
	}
	return r.runScript(ctx, op, script.WriteSchedulerEntriesCmd, []string{key}, args...)
}

// ClearSchedulerEntries deletes scheduler entries data from redis.
func (r *RDB) ClearSchedulerEntries(scheduelrID string) error {
	var op errors.Op = "rdb.ClearSchedulerEntries"
	ctx := context.Background()
	key := base.SchedulerEntriesKey(scheduelrID)
	if err := r.client.ZRem(ctx, base.AllSchedulers, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}

// CancellationPubSub returns a pubsub for cancellation messages.
func (r *RDB) CancellationPubSub() (*redis.PubSub, error) {
	var op errors.Op = "rdb.CancellationPubSub"
	ctx := context.Background()
	pubSub := r.client.Subscribe(ctx, base.CancelChannel)
	_, err := pubSub.Receive(ctx)
	if err != nil {
		return nil, errors.E(op, errors.Unknown, fmt.Sprintf("redis pubSub receive error: %v", err))
	}
	return pubSub, nil
}

// PublishCancellation publish cancellation message to all subscribers.
// The message is the ID for the task to be canceled.
func (r *RDB) PublishCancellation(id string) error {
	var op errors.Op = "rdb.PublishCancellation"
	ctx := context.Background()
	if err := r.client.Publish(ctx, base.CancelChannel, id).Err(); err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("redis pubsub publish error: %v", err))
	}
	return nil
}

// Maximum number of enqueue events to store per entry.
const maxEvents = 1000

// RecordSchedulerEnqueueEvent records the time when the given task was enqueued.
func (r *RDB) RecordSchedulerEnqueueEvent(entryID string, event *base.SchedulerEnqueueEvent) error {
	var op errors.Op = "rdb.RecordSchedulerEnqueueEvent"
	ctx := context.Background()
	data, err := base.EncodeSchedulerEnqueueEvent(event)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode scheduler enqueue event: %v", err))
	}
	keys := []string{
		base.SchedulerHistoryKey(entryID),
	}
	argv := []interface{}{
		event.EnqueuedAt.Unix(),
		data,
		maxEvents,
	}
	return r.runScript(ctx, op, script.RecordSchedulerEnqueueEventCmd, keys, argv...)
}

// ClearSchedulerHistory deletes the enqueue event history for the given scheduler entry.
func (r *RDB) ClearSchedulerHistory(entryID string) error {
	var op errors.Op = "rdb.ClearSchedulerHistory"
	ctx := context.Background()
	key := base.SchedulerHistoryKey(entryID)
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}

// WriteResult writes the given result data for the specified task.
func (r *RDB) WriteResult(ctx context.Context, queueName, taskID string, data []byte) (int, error) {
	var op errors.Op = "rdb.WriteResult"
	taskKey := base.TaskKey(queueName, taskID)
	if err := r.client.HSet(ctx, taskKey, "result", data).Err(); err != nil {
		return 0, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hset", Err: err})
	}
	return len(data), nil
}
