package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kubemq-io/kubemq-mqtt/burnin/api"
	"github.com/kubemq-io/kubemq-mqtt/burnin/config"
	"github.com/kubemq-io/kubemq-mqtt/burnin/engine"
	"github.com/kubemq-io/kubemq-mqtt/burnin/metrics"
	"github.com/kubemq-io/kubemq-mqtt/burnin/report"
	"github.com/kubemq-io/kubemq-mqtt/burnin/server"
)

var (
	configPath     = flag.String("config", "", "path to config YAML file")
	validateConfig = flag.Bool("validate-config", false, "validate config and exit")
	runFlag        = flag.Bool("run", false, "start run immediately from config")
)

func main() {
	flag.Parse()

	cfgFile := config.FindConfigFile(*configPath)

	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: load config: %v\n", err)
		os.Exit(1)
	}

	logger := setupLogger(cfg.Logging)

	if *validateConfig {
		errs := cfg.Validate()
		hasErrors := false
		for _, e := range errs {
			if strings.HasPrefix(e.Error(), "WARNING:") {
				logger.Warn(e.Error())
			} else {
				logger.Error(e.Error())
				hasErrors = true
			}
		}
		if hasErrors {
			os.Exit(1)
		}
		logger.Info("config valid")
		os.Exit(0)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	metrics.InitMetrics()

	eng := engine.New(cfg, logger)
	adapter := &engineAdapter{eng: eng, startupCfg: cfg, logger: logger}

	srv := server.New(cfg.Metrics.Port, adapter, cfg, logger)
	if err := srv.Start(); err != nil {
		logger.Error("failed to start server", "error", err)
		os.Exit(1)
	}

	printBanner(cfg, logger)

	if *runFlag {
		if err := eng.StartRunFromConfig(cfg); err != nil {
			logger.Error("failed to start run", "error", err)
			os.Exit(1)
		}
	}

	// Wait for either a signal or, when -run was given with a bounded duration,
	// the run completing on its own.
	waitForExit(eng, sigCh, cfg, *runFlag)

	logger.Info("shutting down")
	passed := eng.GracefulShutdown()

	summary, verdict := adapter.RunReport()
	if summary != nil && verdict != nil {
		if cfg.Output.ReportFile != "" {
			if err := report.WriteJSON(cfg.Output.ReportFile, summary, verdict); err != nil {
				logger.Error("failed to write report", "error", err)
			}
		}
		report.PrintConsole(summary, verdict)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Stop(shutdownCtx)

	if !passed {
		os.Exit(1)
	}
	if eng.HasWarnings() {
		os.Exit(2)
	}
	os.Exit(0)
}

// waitForExit blocks until a termination signal arrives, or — for a bounded
// -run invocation — until the engine reaches a terminal state on its own.
func waitForExit(eng *engine.Engine, sigCh <-chan os.Signal, cfg *config.Config, autoRun bool) {
	if autoRun && cfg.DurationParsed > 0 {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-sigCh:
				return
			case <-ticker.C:
				switch eng.State() {
				case engine.StateStopped, engine.StateError:
					return
				}
			}
		}
	}
	<-sigCh
}

// engineAdapter bridges engine.Engine to server.RunController.
type engineAdapter struct {
	eng        *engine.Engine
	startupCfg *config.Config
	logger     *slog.Logger
}

func (a *engineAdapter) State() string { return a.eng.State() }

func (a *engineAdapter) StartRun(rc *api.RunConfig) error {
	cfg, err := rc.ToInternalConfig(a.startupCfg)
	if err != nil {
		return fmt.Errorf("convert run config: %w", err)
	}
	return a.eng.StartRunFromConfig(cfg)
}

func (a *engineAdapter) StopRun() error { return a.eng.StopRun() }

func (a *engineAdapter) RunID() string { return a.eng.RunID() }

func (a *engineAdapter) RunConfig() *config.Config { return a.eng.RunConfig() }

func (a *engineAdapter) RunReport() (*report.Summary, *report.Verdict) {
	snapshots := a.eng.GetPatternSnapshots()
	if snapshots == nil {
		return nil, nil
	}

	cfg := a.eng.RunConfig()
	if cfg == nil {
		cfg = a.startupCfg
	}

	summary := &report.Summary{
		RunID:           a.eng.RunID(),
		SDK:             metrics.SDK(),
		SDKVersion:      cfg.Output.SDKVersion,
		Mode:            cfg.Mode,
		BrokerAddress:   cfg.Broker.Address,
		ProtocolVersion: cfg.MQTT.ProtocolVersion,
		Qos:             cfg.MQTT.Qos,
		StartedAt:       a.eng.RunStartedAt(),
		EndedAt:         a.eng.RunEndedAt(),
		Patterns:        make(map[string]*report.PatternStats),
	}

	if !summary.EndedAt.IsZero() && !summary.StartedAt.IsZero() {
		summary.DurationSeconds = summary.EndedAt.Sub(summary.StartedAt).Seconds()
	}

	enabledCount := 0
	for _, pattern := range config.AllPatternNames {
		snap := snapshots[pattern]
		if snap == nil {
			continue
		}
		pc := cfg.GetPatternConfig(pattern)
		if pc == nil || !pc.Enabled {
			continue
		}
		enabledCount++

		ps := &report.PatternStats{
			Enabled:         true,
			Sent:            snap.Sent,
			Received:        snap.Received,
			Lost:            snap.Lost,
			Duplicated:      snap.Duplicated,
			Corrupted:       snap.Corrupted,
			OutOfOrder:      snap.OutOfOrder,
			Errors:          snap.Errors,
			Reconnections:   snap.Reconnections,
			DowntimeSeconds: snap.DowntimeSeconds,
			LatencyP50MS:    snap.LatencyP50,
			LatencyP95MS:    snap.LatencyP95,
			LatencyP99MS:    snap.LatencyP99,
			LatencyP999MS:   snap.LatencyP999,
			AvgRate:         snap.AvgRate,
			PeakRate:        snap.PeakRate,
			TargetRate:      pc.Rate,
			Channels:        pc.Channels,
			ConsumerGroup:   pc.ConsumerGroup,
			RPCSuccess:      snap.RPCSuccess,
			RPCTimeout:      snap.RPCTimeout,
			RPCError:        snap.RPCError,
			RPCLatencyP50MS: snap.RPCLatencyP50,
			RPCLatencyP99MS: snap.RPCLatencyP99,
		}

		if snap.Sent > 0 {
			ps.LossPct = float64(snap.Lost) / float64(snap.Sent) * 100
		}

		if config.IsRPCPattern(pattern) {
			ps.ProducersPerChannel = pc.SendersPerChannel
			ps.ConsumersPerChannel = pc.RespondersPerChannel
		} else {
			ps.ProducersPerChannel = pc.ProducersPerChannel
			ps.ConsumersPerChannel = pc.ConsumersPerChannel
		}

		for _, ws := range snap.Producers {
			ps.Producers = append(ps.Producers, report.WorkerSnapshot{ID: ws.ID, Sent: ws.Sent, Recv: ws.Recv})
		}
		for _, ws := range snap.Consumers {
			ps.Consumers = append(ps.Consumers, report.WorkerSnapshot{ID: ws.ID, Sent: ws.Sent, Recv: ws.Recv})
		}

		summary.Patterns[pattern] = ps
	}

	summary.AllPatternsEnabled = enabledCount == len(config.AllPatternNames)

	baseline := a.eng.BaselineRSS()
	peak := a.eng.PeakRSS()
	summary.Resources = report.ResourceStats{
		PeakRSSMB:     float64(peak) / 1024 / 1024,
		BaselineRSSMB: float64(baseline) / 1024 / 1024,
	}
	if baseline > 0 {
		summary.Resources.MemoryGrowthFactor = float64(peak) / float64(baseline)
	}

	verdict := report.GenerateVerdict(summary, cfg)
	return summary, verdict
}

func (a *engineAdapter) RunStatus() api.RunStatus {
	status := api.RunStatus{
		State: a.eng.State(),
		RunID: a.eng.RunID(),
	}

	if !a.eng.RunStartedAt().IsZero() {
		status.Uptime = time.Since(a.eng.RunStartedAt()).Seconds()
	}

	groups := a.eng.PatternGroups()
	if groups != nil {
		status.Patterns = make(map[string]api.PatternStatus)
		for pattern, pg := range groups {
			var ps api.PatternStatus
			for _, w := range pg.Workers() {
				ps.Sent += w.SentCount()
				ps.Received += w.ReceivedCount()
				ps.Errors += w.ErrorCount()
				ps.Rate += w.RateWindow().Rate()
			}
			status.Patterns[pattern] = ps
		}
	}

	return status
}

func (a *engineAdapter) RunSnapshot() api.RunSnapshot {
	snap := api.RunSnapshot{
		State:  a.eng.State(),
		RunID:  a.eng.RunID(),
		Config: a.eng.RunConfig(),
	}

	groups := a.eng.PatternGroups()
	if groups == nil {
		return snap
	}

	snap.Workers = make(map[string][]api.WorkerSnapshot)
	var totalSent, totalRecv, totalErrors, totalLost, totalCorrupted, totalDup uint64

	for pattern, pg := range groups {
		var workers []api.WorkerSnapshot
		for _, w := range pg.Workers() {
			ws := api.WorkerSnapshot{
				ID:       fmt.Sprintf("%s/%s", w.Pattern(), w.ChannelName()),
				Pattern:  w.Pattern(),
				Channel:  w.ChannelName(),
				Sent:     w.SentCount(),
				Received: w.ReceivedCount(),
				Errors:   w.ErrorCount(),
				Rate:     w.RateWindow().Rate(),
			}
			workers = append(workers, ws)
			totalSent += w.SentCount()
			totalRecv += w.ReceivedCount()
			totalErrors += w.ErrorCount()
			totalLost += w.Tracker().TotalLost()
			totalCorrupted += w.CorruptedCount()
			totalDup += w.DuplicatedCount()
		}
		snap.Workers[pattern] = workers
	}

	var uptimeSeconds float64
	if !a.eng.RunStartedAt().IsZero() {
		uptimeSeconds = time.Since(a.eng.RunStartedAt()).Seconds()
	}
	snap.Metrics = api.MetricsSnapshot{
		TotalSent:       totalSent,
		TotalReceived:   totalRecv,
		TotalErrors:     totalErrors,
		TotalLost:       totalLost,
		TotalCorrupted:  totalCorrupted,
		TotalDuplicated: totalDup,
		UptimeSeconds:   uptimeSeconds,
	}

	return snap
}

func setupLogger(logCfg config.LoggingConfig) *slog.Logger {
	level := parseLogLevel(logCfg.Level)
	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if logCfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}

	return slog.New(handler)
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func printBanner(cfg *config.Config, logger *slog.Logger) {
	logger.Info("MQTT burn-in starting",
		"mode", cfg.Mode,
		"broker", cfg.Broker.Address,
		"grpc_responder", cfg.GRPC.ResponderAddress,
		"protocol_version", cfg.MQTT.ProtocolVersion,
		"qos", cfg.MQTT.Qos,
		"port", cfg.Metrics.Port,
		"duration", cfg.Duration,
		"channels", cfg.TotalChannelCount(),
	)
}
