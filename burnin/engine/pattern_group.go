package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-mqtt/burnin/config"
	"github.com/kubemq-io/kubemq-mqtt/burnin/worker"
)

// PatternGroup owns the workers for one pattern across all its channels.
type PatternGroup struct {
	pattern string
	workers []worker.Worker
}

// NewPatternGroup builds one Worker per channel for the pattern, injecting the
// shared MQTT transport factory so workers open paho connections without
// importing paho directly.
func NewPatternGroup(pattern string, cfg *config.Config, factory worker.TransportFactory, logger *slog.Logger) *PatternGroup {
	numChannels := cfg.GetPatternChannels(pattern)
	workers := make([]worker.Worker, numChannels)
	for i := 0; i < numChannels; i++ {
		switch pattern {
		case worker.PatternEvents:
			workers[i] = worker.NewEventsWorker(cfg, i+1, factory, logger)
		case worker.PatternEventsStore:
			workers[i] = worker.NewEventsStoreWorker(cfg, i+1, factory, logger)
		case worker.PatternQueues:
			workers[i] = worker.NewQueuesWorker(cfg, i+1, factory, logger)
		case worker.PatternCommands:
			workers[i] = worker.NewCommandsWorker(cfg, i+1, factory, logger)
		case worker.PatternQueries:
			workers[i] = worker.NewQueriesWorker(cfg, i+1, factory, logger)
		}
	}
	return &PatternGroup{pattern: pattern, workers: workers}
}

func (pg *PatternGroup) StartConsumers(ctx context.Context) error {
	for _, w := range pg.workers {
		if err := w.Start(ctx); err != nil {
			return fmt.Errorf("start consumer for %s/%s: %w", pg.pattern, w.ChannelName(), err)
		}
	}
	return nil
}

func (pg *PatternGroup) WaitForConsumerReady(timeout time.Duration) error {
	for _, w := range pg.workers {
		select {
		case <-w.ConsumerReady():
		case <-time.After(timeout):
			return fmt.Errorf("consumer ready timeout for %s/%s", pg.pattern, w.ChannelName())
		}
	}
	return nil
}

func (pg *PatternGroup) StartProducers() {
	for _, w := range pg.workers {
		w.StartProducers()
	}
}

func (pg *PatternGroup) StopProducers() {
	for _, w := range pg.workers {
		w.StopProducers()
	}
}

func (pg *PatternGroup) StopConsumers() {
	for _, w := range pg.workers {
		w.StopConsumers()
	}
}

func (pg *PatternGroup) DisconnectConsumers() {
	for _, w := range pg.workers {
		w.DisconnectConsumers()
	}
}

func (pg *PatternGroup) Workers() []worker.Worker {
	return pg.workers
}

func (pg *PatternGroup) Pattern() string {
	return pg.pattern
}
