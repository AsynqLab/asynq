package asynq

import (
	"context"
	"sync"
	"time"

	"github.com/AsynqLab/asynq/internal/base"
	"github.com/AsynqLab/asynq/internal/log"
)

// A forwarder is responsible for moving scheduled and retry tasks to pending state
// so that the tasks get processed by the workers.
type forwarder struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "forwarder" goroutine.
	done chan struct{}

	// list of queue names to check and enqueue.
	queues []string

	// poll interval on average
	avgInterval time.Duration
}

type forwarderParams struct {
	logger   *log.Logger
	broker   base.Broker
	queues   []string
	interval time.Duration
}

func newForwarder(params forwarderParams) *forwarder {
	return &forwarder{
		logger:      params.logger,
		broker:      params.broker,
		done:        make(chan struct{}),
		queues:      params.queues,
		avgInterval: params.interval,
	}
}

func (f *forwarder) shutdown() {
	f.logger.Debug("Forwarder shutting down...")
	// Signal the forwarder goroutine to stop polling.
	f.done <- struct{}{}
}

// start starts the "forwarder" goroutine.
func (f *forwarder) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTimer(f.avgInterval)
		for {
			select {
			case <-f.done:
				f.logger.Debug("Forwarder done")
				return
			case <-timer.C:
				f.exec()
				timer.Reset(f.avgInterval)
			}
		}
	}()
}

func (f *forwarder) exec() {
	ctx := context.Background()
	if err := f.broker.ForwardIfReady(ctx, f.queues...); err != nil {
		f.logger.Errorf("Failed to forward scheduled tasks: %v", err)
	}
}
