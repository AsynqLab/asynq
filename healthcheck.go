package asynq

import (
	"sync"
	"time"

	"github.com/AsynqLab/asynq/internal/base"
	"github.com/AsynqLab/asynq/internal/log"
)

// healthChecker is responsible for pinging broker periodically
// and call user provided HeathCheckFunc with the ping result.
type healthChecker struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "healthChecker" goroutine.
	done chan struct{}

	// interval between healthchecks.
	interval time.Duration

	// function to call periodically.
	healthcheckFunc func(error)
}

type healthcheckerParams struct {
	logger          *log.Logger
	broker          base.Broker
	interval        time.Duration
	healthcheckFunc func(error)
}

func newHealthChecker(params healthcheckerParams) *healthChecker {
	return &healthChecker{
		logger:          params.logger,
		broker:          params.broker,
		done:            make(chan struct{}),
		interval:        params.interval,
		healthcheckFunc: params.healthcheckFunc,
	}
}

func (hc *healthChecker) shutdown() {
	if hc.healthcheckFunc == nil {
		return
	}

	hc.logger.Debug("Healthchecker shutting down...")
	// Signal the healthChecker goroutine to stop.
	hc.done <- struct{}{}
}

func (hc *healthChecker) start(wg *sync.WaitGroup) {
	if hc.healthcheckFunc == nil {
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTimer(hc.interval)
		for {
			select {
			case <-hc.done:
				hc.logger.Debug("Healthchecker done")
				timer.Stop()
				return
			case <-timer.C:
				err := hc.broker.Ping()
				hc.healthcheckFunc(err)
				timer.Reset(hc.interval)
			}
		}
	}()
}
