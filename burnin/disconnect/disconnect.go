package disconnect

import (
	"context"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-mqtt/burnin/metrics"
	"github.com/kubemq-io/kubemq-mqtt/burnin/worker"
)

// Manager periodically force-drops worker consumer connections (no clean MQTT
// DISCONNECT) so autopaho reconnects, exercising the recovery path. Unacked
// QoS>0 messages are requeued by the broker on disconnect.
type Manager struct {
	interval time.Duration
	duration time.Duration
	workers  []worker.Worker
	logger   *slog.Logger
}

func New(interval, duration time.Duration, workers []worker.Worker, logger *slog.Logger) *Manager {
	return &Manager{
		interval: interval,
		duration: duration,
		workers:  workers,
		logger:   logger,
	}
}

func (m *Manager) Run(ctx context.Context) {
	if m.interval == 0 {
		return
	}
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.IncForcedDisconnect()
			m.logger.Info("forced disconnect: dropping MQTT consumer connections")
			for _, w := range m.workers {
				w.DisconnectConsumers()
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(m.duration):
			}
			m.logger.Info("forced disconnect: consumers will auto-reconnect")
		}
	}
}
