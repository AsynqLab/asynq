package asynq

import (
	"sync"
	"time"

	"github.com/AsynqLab/asynq/internal/base"
	"github.com/AsynqLab/asynq/internal/log"
	"github.com/redis/go-redis/v9"
)

type subscriber struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "subscriber" goroutine.
	done chan struct{}

	// cancelations hold cancel functions for all active tasks.
	cancelations *base.Cancelations

	// time to wait before retrying to connect to redis.
	retryTimeout time.Duration
}

type subscriberParams struct {
	logger       *log.Logger
	broker       base.Broker
	cancelations *base.Cancelations
}

func newSubscriber(params subscriberParams) *subscriber {
	return &subscriber{
		logger:       params.logger,
		broker:       params.broker,
		done:         make(chan struct{}),
		cancelations: params.cancelations,
		retryTimeout: 5 * time.Second,
	}
}

func (s *subscriber) shutdown() {
	s.logger.Debug("Subscriber shutting down...")
	// Signal the subscriber goroutine to stop.
	s.done <- struct{}{}
}

func (s *subscriber) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		var (
			pubSub *redis.PubSub
			err    error
		)
		// Try until successfully connect to Redis.
		for {
			pubSub, err = s.broker.CancellationPubSub()
			if err != nil {
				s.logger.Errorf("cannot subscribe to cancelation channel: %v", err)
				select {
				case <-time.After(s.retryTimeout):
					continue
				case <-s.done:
					s.logger.Debug("Subscriber done")
					return
				}
			}
			break
		}
		cancelCh := pubSub.Channel()
		for {
			select {
			case <-s.done:
				_ = pubSub.Close()
				s.logger.Debug("Subscriber done")
				return
			case msg := <-cancelCh:
				cancel, ok := s.cancelations.Get(msg.Payload)
				if ok {
					cancel()
				}
			}
		}
	}()
}
