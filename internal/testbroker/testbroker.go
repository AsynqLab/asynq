// Package testbroker exports a broker implementation that should be used in package testing.
package testbroker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/AsynqLab/asynq/internal/base"
	"github.com/redis/go-redis/v9"
)

var errRedisDown = errors.New("testutil: redis is down")

// TestBroker is a broker implementation which enables
// to simulate Redis failure in tests.
type TestBroker struct {
	mu       sync.Mutex
	sleeping bool

	// real broker
	real base.Broker
}

// Make sure TestBroker implements Broker interface at compile time.
var _ base.Broker = (*TestBroker)(nil)

func NewTestBroker(b base.Broker) *TestBroker {
	return &TestBroker{real: b}
}

func (tb *TestBroker) Sleep() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.sleeping = true
}

func (tb *TestBroker) Wakeup() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.sleeping = false
}

func (tb *TestBroker) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Enqueue(ctx, msg)
}

func (tb *TestBroker) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.EnqueueUnique(ctx, msg, ttl)
}

func (tb *TestBroker) Dequeue(ctx context.Context, queueNames ...string) (*base.TaskMessage, time.Time, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, time.Time{}, errRedisDown
	}
	return tb.real.Dequeue(ctx, queueNames...)
}

func (tb *TestBroker) Done(ctx context.Context, msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Done(ctx, msg)
}

func (tb *TestBroker) MarkAsComplete(ctx context.Context, msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.MarkAsComplete(ctx, msg)
}

func (tb *TestBroker) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Requeue(ctx, msg)
}

func (tb *TestBroker) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Schedule(ctx, msg, processAt)
}

func (tb *TestBroker) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ScheduleUnique(ctx, msg, processAt, ttl)
}

func (tb *TestBroker) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Retry(ctx, msg, processAt, errMsg, isFailure)
}

func (tb *TestBroker) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Archive(ctx, msg, errMsg)
}

func (tb *TestBroker) ForwardIfReady(ctx context.Context, queueNames ...string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ForwardIfReady(ctx, queueNames...)
}

func (tb *TestBroker) DeleteExpiredCompletedTasks(ctx context.Context, queueName string, batchSize int) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.DeleteExpiredCompletedTasks(ctx, queueName, batchSize)
}

func (tb *TestBroker) ListLeaseExpired(ctx context.Context, cutoff time.Time, queueNames ...string) ([]*base.TaskMessage, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.ListLeaseExpired(ctx, cutoff, queueNames...)
}

func (tb *TestBroker) ExtendLease(ctx context.Context, queueName string, ids ...string) (time.Time, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return time.Time{}, errRedisDown
	}
	return tb.real.ExtendLease(ctx, queueName, ids...)
}

func (tb *TestBroker) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.WriteServerState(info, workers, ttl)
}

func (tb *TestBroker) ClearServerState(host string, pid int, serverID string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ClearServerState(host, pid, serverID)
}

func (tb *TestBroker) CancellationPubSub() (*redis.PubSub, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.CancellationPubSub()
}

func (tb *TestBroker) PublishCancellation(id string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.PublishCancellation(id)
}

func (tb *TestBroker) WriteResult(ctx context.Context, queueName, id string, data []byte) (int, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return 0, errRedisDown
	}
	return tb.real.WriteResult(ctx, queueName, id, data)
}

func (tb *TestBroker) Ping() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Ping()
}

func (tb *TestBroker) Close() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Close()
}

func (tb *TestBroker) AddToGroup(ctx context.Context, msg *base.TaskMessage, gname string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.AddToGroup(ctx, msg, gname)
}

func (tb *TestBroker) AddToGroupUnique(ctx context.Context, msg *base.TaskMessage, gname string, ttl time.Duration) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.AddToGroupUnique(ctx, msg, gname, ttl)
}

func (tb *TestBroker) ListGroups(queueName string) ([]string, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.ListGroups(queueName)
}

func (tb *TestBroker) AggregationCheck(queueName, gname string, t time.Time, gracePeriod, maxDelay time.Duration, maxSize int) (aggregationSetID string, err error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return "", errRedisDown
	}
	return tb.real.AggregationCheck(queueName, gname, t, gracePeriod, maxDelay, maxSize)
}

func (tb *TestBroker) ReadAggregationSet(queueName, gname, aggregationSetID string) ([]*base.TaskMessage, time.Time, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, time.Time{}, errRedisDown
	}
	return tb.real.ReadAggregationSet(queueName, gname, aggregationSetID)
}

func (tb *TestBroker) DeleteAggregationSet(ctx context.Context, queueName, gname, aggregationSetID string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.DeleteAggregationSet(ctx, queueName, gname, aggregationSetID)
}

func (tb *TestBroker) ReclaimStaleAggregationSets(ctx context.Context, queueName string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ReclaimStaleAggregationSets(ctx, queueName)
}
