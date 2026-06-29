package api

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kubemq-io/kubemq-mqtt/burnin/config"
)

// RunConfig is the JSON request body for POST /run/start.
// Uses pointer fields so omitted fields don't zero-out the base config during overlay.
type RunConfig struct {
	Duration         string                      `json:"duration"`
	Mode             string                      `json:"mode,omitempty"`
	MQTT             *config.MQTTConfig          `json:"mqtt,omitempty"`
	GRPC             *config.GRPCConfig          `json:"grpc,omitempty"`
	Patterns         *config.PatternsConfig      `json:"patterns,omitempty"`
	Queue            *config.QueueConfig         `json:"queue,omitempty"`
	RPC              *config.RPCConfig           `json:"rpc,omitempty"`
	Message          *config.MessageConfig       `json:"message,omitempty"`
	Thresholds       *config.ThresholdsConfig    `json:"thresholds,omitempty"`
	ForcedDisconnect *config.ForcedDisconnConfig `json:"forced_disconnect,omitempty"`
}

// ToInternalConfig merges this RunConfig onto a copy of base, parses
// durations, validates, and returns the merged config.
func (rc *RunConfig) ToInternalConfig(base *config.Config) (*config.Config, error) {
	data, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("marshal base config: %w", err)
	}
	var cfg config.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal base config: %w", err)
	}

	overlay, err := json.Marshal(rc)
	if err != nil {
		return nil, fmt.Errorf("marshal run config: %w", err)
	}
	if err := json.Unmarshal(overlay, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal run config overlay: %w", err)
	}

	if err := config.ParseDurationsPublic(&cfg); err != nil {
		return nil, fmt.Errorf("parse durations: %w", err)
	}

	if cfg.RunID == "" {
		cfg.RunID = config.RandomRunID()
	}

	if errs := cfg.Validate(); len(errs) > 0 {
		for _, e := range errs {
			if !strings.HasPrefix(e.Error(), "WARNING:") {
				return nil, fmt.Errorf("config validation: %v", e)
			}
		}
	}

	return &cfg, nil
}

type RunStatus struct {
	State    string                   `json:"state"`
	RunID    string                   `json:"run_id,omitempty"`
	Uptime   float64                  `json:"uptime_seconds,omitempty"`
	Patterns map[string]PatternStatus `json:"patterns,omitempty"`
}

type PatternStatus struct {
	Sent     uint64  `json:"sent"`
	Received uint64  `json:"received"`
	Errors   uint64  `json:"errors"`
	Rate     float64 `json:"actual_rate"`
}

type RunSnapshot struct {
	State   string                      `json:"state"`
	RunID   string                      `json:"run_id"`
	Config  *config.Config              `json:"config"`
	Workers map[string][]WorkerSnapshot `json:"workers"`
	Metrics MetricsSnapshot             `json:"metrics"`
}

type WorkerSnapshot struct {
	ID       string  `json:"id"`
	Pattern  string  `json:"pattern"`
	Channel  string  `json:"channel"`
	Sent     uint64  `json:"sent"`
	Received uint64  `json:"received"`
	Errors   uint64  `json:"errors"`
	Rate     float64 `json:"rate"`
}

type MetricsSnapshot struct {
	TotalSent       uint64  `json:"total_sent"`
	TotalReceived   uint64  `json:"total_received"`
	TotalErrors     uint64  `json:"total_errors"`
	TotalLost       uint64  `json:"total_lost"`
	TotalCorrupted  uint64  `json:"total_corrupted"`
	TotalDuplicated uint64  `json:"total_duplicated"`
	UptimeSeconds   float64 `json:"uptime_seconds"`
}
