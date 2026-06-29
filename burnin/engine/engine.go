package engine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kubemq-io/kubemq-mqtt/burnin/config"
	"github.com/kubemq-io/kubemq-mqtt/burnin/disconnect"
	"github.com/kubemq-io/kubemq-mqtt/burnin/metrics"
	"github.com/kubemq-io/kubemq-mqtt/burnin/responder"
	"github.com/kubemq-io/kubemq-mqtt/burnin/transport"
	"github.com/kubemq-io/kubemq-mqtt/burnin/worker"
)

const (
	StateIdle     = "idle"
	StateStarting = "starting"
	StateRunning  = "running"
	StateStopping = "stopping"
	StateStopped  = "stopped"
	StateError    = "error"
)

// PatternSnapshot holds frozen counter values captured at producer-stop time,
// used by the report package to generate verdicts without reading live counters.
type PatternSnapshot struct {
	Sent            uint64
	Received        uint64
	Errors          uint64
	Corrupted       uint64
	Reconnections   uint64
	DowntimeSeconds float64
	BytesSent       uint64
	BytesReceived   uint64
	Duplicated      uint64
	RPCSuccess      uint64
	RPCTimeout      uint64
	RPCError        uint64
	Lost            uint64
	OutOfOrder      uint64
	LatencyP50      float64
	LatencyP95      float64
	LatencyP99      float64
	LatencyP999     float64
	RPCLatencyP50   float64
	RPCLatencyP99   float64
	PeakRate        float64
	AvgRate         float64
	Producers       []worker.WorkerStatSnapshot
	Consumers       []worker.WorkerStatSnapshot
}

// VerdictResult holds the pass/fail outcome and any warnings from threshold checks.
type VerdictResult struct {
	Result   string // "PASSED", "PASSED_WITH_WARNINGS", "FAILED"
	Passed   bool
	Warnings []string
}

type Engine struct {
	startupCfg       *config.Config
	logger           *slog.Logger
	bootTime         time.Time
	transportFactory worker.TransportFactory

	mu                 sync.Mutex
	state              string
	runCfg             *config.Config
	runID              string
	runStartedAt       time.Time
	runEndedAt         time.Time
	producersStartedAt time.Time
	producersStoppedAt time.Time
	runError           string
	runCancel          context.CancelFunc
	runDone            chan struct{}

	patternGroups    map[string]*PatternGroup
	patternSnapshots map[string]*PatternSnapshot
	rpcResponder     *responder.GRPCResponder

	baselineRSS atomic.Uint64
	peakRSS     atomic.Uint64

	verdictResult *VerdictResult
}

// New builds the engine. The transport factory is the paho-backed MQTT factory
// (transport.NewPahoFactory) — injected so workers stay decoupled from paho and
// tests can substitute a fake.
func New(cfg *config.Config, logger *slog.Logger) *Engine {
	return &Engine{
		startupCfg:       cfg,
		logger:           logger,
		bootTime:         time.Now(),
		state:            StateIdle,
		transportFactory: transport.NewPahoFactory(logger),
	}
}

// --- Accessors ---

func (e *Engine) State() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state
}

func (e *Engine) RunID() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runID
}

func (e *Engine) RunConfig() *config.Config {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runCfg
}

func (e *Engine) StartupConfig() *config.Config { return e.startupCfg }
func (e *Engine) BootTime() time.Time           { return e.bootTime }

func (e *Engine) RunStartedAt() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runStartedAt
}

func (e *Engine) RunEndedAt() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runEndedAt
}

func (e *Engine) RunError() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runError
}

func (e *Engine) PatternGroups() map[string]*PatternGroup {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.patternGroups
}

func (e *Engine) GetPatternSnapshots() map[string]*PatternSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.patternSnapshots
}

func (e *Engine) BaselineRSS() uint64 { return e.baselineRSS.Load() }
func (e *Engine) PeakRSS() uint64     { return e.peakRSS.Load() }

func (e *Engine) Verdict() *VerdictResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.verdictResult
}

func (e *Engine) AllWorkers() []worker.Worker {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.allWorkers()
}

func (e *Engine) allWorkers() []worker.Worker {
	var workers []worker.Worker
	for _, pattern := range config.AllPatternNames {
		pg := e.patternGroups[pattern]
		if pg == nil {
			continue
		}
		workers = append(workers, pg.Workers()...)
	}
	return workers
}

// --- Lifecycle ---

// StartRunFromConfig transitions idle/stopped/error → starting and launches the
// run loop in the background.
func (e *Engine) StartRunFromConfig(cfg *config.Config) error {
	e.mu.Lock()
	if e.state != StateIdle && e.state != StateStopped && e.state != StateError {
		state := e.state
		e.mu.Unlock()
		return fmt.Errorf("cannot start run: engine in state %s", state)
	}
	e.state = StateStarting
	e.runCfg = cfg
	e.runID = cfg.RunID
	if e.runID == "" {
		e.runID = config.RandomRunID()
	}
	e.runStartedAt = time.Now()
	e.runEndedAt = time.Time{}
	e.runError = ""
	e.verdictResult = nil
	e.patternSnapshots = nil
	e.baselineRSS.Store(0)
	e.peakRSS.Store(0)

	ctx, cancel := context.WithCancel(context.Background())
	e.runCancel = cancel
	e.runDone = make(chan struct{})
	e.mu.Unlock()

	e.logger.Info("starting run", "run_id", e.runID)
	go e.runLoop(ctx, cfg)
	return nil
}

func (e *Engine) StopRun() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != StateStarting && e.state != StateRunning {
		return fmt.Errorf("cannot stop run: engine in state %s", e.state)
	}
	if e.runCancel != nil {
		e.runCancel()
	}
	return nil
}

func (e *Engine) GracefulShutdown() bool {
	e.mu.Lock()
	state := e.state
	cancel := e.runCancel
	done := e.runDone
	e.mu.Unlock()

	if state == StateRunning || state == StateStarting {
		if cancel != nil {
			cancel()
		}
		if done != nil {
			select {
			case <-done:
			case <-time.After(60 * time.Second):
				e.logger.Error("graceful shutdown timed out after 60s")
			}
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	return e.verdictResult != nil && e.verdictResult.Passed
}

func (e *Engine) HasWarnings() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.verdictResult != nil && e.verdictResult.Result == "PASSED_WITH_WARNINGS"
}

// --- Run loop ---

func (e *Engine) runLoop(ctx context.Context, cfg *config.Config) {
	defer func() {
		e.mu.Lock()
		if e.state != StateError {
			e.state = StateStopped
		}
		e.runEndedAt = time.Now()
		done := e.runDone
		e.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()

	// Build pattern groups for each enabled pattern.
	e.mu.Lock()
	e.patternGroups = make(map[string]*PatternGroup)
	for _, pattern := range config.AllPatternNames {
		pc := cfg.GetPatternConfig(pattern)
		if pc == nil || !pc.Enabled {
			continue
		}
		e.patternGroups[pattern] = NewPatternGroup(pattern, cfg, e.transportFactory, e.logger)
	}
	e.mu.Unlock()

	// Start the embedded gRPC RPC responder if commands/queries are enabled.
	if err := e.startResponder(ctx, cfg); err != nil {
		e.setError(fmt.Sprintf("start RPC responder: %v", err))
		return
	}

	// Start all consumers/subscriptions.
	for _, pattern := range config.AllPatternNames {
		pg := e.patternGroups[pattern]
		if pg == nil {
			continue
		}
		if err := pg.StartConsumers(ctx); err != nil {
			e.setError(fmt.Sprintf("start consumers: %v", err))
			return
		}
	}

	// Wait for all consumers to signal ready.
	readyTimeout := 30 * time.Second
	for _, pattern := range config.AllPatternNames {
		pg := e.patternGroups[pattern]
		if pg == nil {
			continue
		}
		if err := pg.WaitForConsumerReady(readyTimeout); err != nil {
			e.setError(fmt.Sprintf("consumer ready: %v", err))
			return
		}
	}

	if ctx.Err() != nil {
		return
	}

	// Warmup stabilization + counter reset.
	e.warmup(ctx, cfg)
	if ctx.Err() != nil {
		return
	}

	// T0: start producers — measurement window begins.
	e.mu.Lock()
	e.producersStartedAt = time.Now()
	e.mu.Unlock()

	for _, pattern := range config.AllPatternNames {
		pg := e.patternGroups[pattern]
		if pg == nil {
			continue
		}
		pg.StartProducers()
		metrics.SetTargetRate(pattern, float64(cfg.GetPatternRate(pattern)))
	}

	e.mu.Lock()
	e.state = StateRunning
	e.mu.Unlock()
	e.logger.Info("burn-in running", "run_id", e.runID, "duration", cfg.Duration)

	periodicCtx, periodicCancel := context.WithCancel(ctx)
	var periodicWG sync.WaitGroup
	e.startPeriodicTasks(periodicCtx, &periodicWG, cfg)

	var durationCh <-chan time.Time
	if cfg.DurationParsed > 0 {
		timer := time.NewTimer(cfg.DurationParsed)
		defer timer.Stop()
		durationCh = timer.C
	}

	select {
	case <-ctx.Done():
		e.logger.Info("run cancelled")
	case <-durationCh:
		e.logger.Info("duration reached", "duration", cfg.Duration)
	}

	periodicCancel()
	periodicWG.Wait()

	e.mu.Lock()
	e.state = StateStopping
	e.mu.Unlock()

	e.shutdownWorkers(cfg)
	e.computeVerdict(cfg)
	e.logFinalSummary()
}

// startResponder connects the embedded gRPC responder and registers every
// enabled commands/queries channel so the RPC round-trip is self-contained.
func (e *Engine) startResponder(ctx context.Context, cfg *config.Config) error {
	if !cfg.Patterns.Commands.Enabled && !cfg.Patterns.Queries.Enabled {
		return nil
	}
	resp := responder.New(cfg, cfg.Broker.ClientIDPrefix+"-responder-"+e.runID, e.logger)

	if pg := e.patternGroups[worker.PatternCommands]; pg != nil {
		for _, w := range pg.Workers() {
			resp.RegisterCommandChannel(w.ChannelName())
		}
	}
	if pg := e.patternGroups[worker.PatternQueries]; pg != nil {
		for _, w := range pg.Workers() {
			resp.RegisterQueryChannel(w.ChannelName())
		}
	}

	if err := resp.Start(ctx); err != nil {
		return err
	}
	e.mu.Lock()
	e.rpcResponder = resp
	e.mu.Unlock()
	return nil
}

// --- Two-phase shutdown ---

func (e *Engine) shutdownWorkers(cfg *config.Config) {
	for pattern, pg := range e.patternGroups {
		for _, w := range pg.Workers() {
			deltas := w.Tracker().DetectGaps()
			for _, delta := range deltas {
				metrics.AddLost(pattern, delta)
			}
		}
	}

	e.mu.Lock()
	e.producersStoppedAt = time.Now()
	e.mu.Unlock()

	e.capturePatternSnapshots()

	for _, pattern := range config.AllPatternNames {
		pg := e.patternGroups[pattern]
		if pg == nil {
			continue
		}
		pg.StopProducers()
	}
	e.logger.Info("producers stopped, draining", "drain_seconds", cfg.Shutdown.DrainTimeoutSeconds)

	time.Sleep(time.Duration(cfg.Shutdown.DrainTimeoutSeconds) * time.Second)

	for _, pattern := range config.AllPatternNames {
		pg := e.patternGroups[pattern]
		if pg == nil {
			continue
		}
		pg.StopConsumers()
	}

	// Stop the embedded gRPC responder after consumers drain.
	e.mu.Lock()
	resp := e.rpcResponder
	e.rpcResponder = nil
	e.mu.Unlock()
	if resp != nil {
		if err := resp.Stop(); err != nil {
			e.logger.Warn("stop RPC responder", "error", err)
		}
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Sys > e.peakRSS.Load() {
		e.peakRSS.Store(m.Sys)
	}

	e.logger.Info("all workers stopped")
}

func (e *Engine) capturePatternSnapshots() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.patternSnapshots = make(map[string]*PatternSnapshot)

	for pattern, pg := range e.patternGroups {
		snap := &PatternSnapshot{}

		var bestLatAccum, bestRPCLatAccum *metrics.LatencyAccumulator
		var bestLatCount, bestRPCLatCount int64

		for _, w := range pg.Workers() {
			snap.Sent += w.SentCount()
			snap.Received += w.ReceivedCount()
			snap.Errors += w.ErrorCount()
			snap.Corrupted += w.CorruptedCount()
			snap.Reconnections += w.ReconnectionCount()
			snap.DowntimeSeconds += w.DowntimeSeconds()
			snap.BytesSent += w.BytesSentCount()
			snap.BytesReceived += w.BytesReceivedCount()
			snap.Duplicated += w.DuplicatedCount()
			snap.RPCSuccess += w.RPCSuccess()
			snap.RPCTimeout += w.RPCTimeout()
			snap.RPCError += w.RPCError()
			snap.Lost += w.Tracker().TotalLost()
			snap.OutOfOrder += w.Tracker().TotalOutOfOrder()
			snap.Producers = append(snap.Producers, w.ProducerStatSnapshots()...)
			snap.Consumers = append(snap.Consumers, w.ConsumerStatSnapshots()...)

			if c := w.LatencyAccumulator().Count(); c > bestLatCount {
				bestLatCount = c
				bestLatAccum = w.LatencyAccumulator()
			}
			if c := w.RPCLatencyAccumulator().Count(); c > bestRPCLatCount {
				bestRPCLatCount = c
				bestRPCLatAccum = w.RPCLatencyAccumulator()
			}

			if p := w.PeakRate().Peak(); p > snap.PeakRate {
				snap.PeakRate = p
			}
			snap.AvgRate += w.RateWindow().Rate()
		}

		if bestLatAccum != nil {
			snap.LatencyP50, snap.LatencyP95, snap.LatencyP99, snap.LatencyP999 = bestLatAccum.Percentiles()
		}
		if bestRPCLatAccum != nil {
			snap.RPCLatencyP50, _, snap.RPCLatencyP99, _ = bestRPCLatAccum.Percentiles()
		}

		e.patternSnapshots[pattern] = snap
	}
}

// --- Warmup ---

// warmup runs the optional stabilization period, then resets all worker
// counters/trackers so warmup traffic does not pollute measurements. Unlike
// cloud-events (which sent warmup HTTP requests), MQTT consumers are already
// subscribed; the producers emit during the measurement window only.
func (e *Engine) warmup(ctx context.Context, cfg *config.Config) {
	metrics.WarmupActive.WithLabelValues(metrics.SDK()).Set(1)
	defer metrics.WarmupActive.WithLabelValues(metrics.SDK()).Set(0)

	if cfg.WarmupDurationParsed > 0 {
		e.logger.Info("warmup stabilization", "duration", cfg.WarmupDuration)
		select {
		case <-ctx.Done():
			return
		case <-time.After(cfg.WarmupDurationParsed):
		}
	}

	for _, pg := range e.patternGroups {
		for _, w := range pg.Workers() {
			w.ResetAfterWarmup()
		}
	}
	e.logger.Info("warmup complete")
}

// --- Periodic tasks ---

func (e *Engine) startPeriodicTasks(ctx context.Context, wg *sync.WaitGroup, cfg *config.Config) {
	if cfg.ReportIntervalParsed > 0 {
		wg.Add(1)
		go func() { defer wg.Done(); e.periodicReporter(ctx, cfg) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); e.rateAdvancer(ctx) }()
	wg.Add(1)
	go func() { defer wg.Done(); e.uptimeTracker(ctx) }()
	wg.Add(1)
	go func() { defer wg.Done(); e.memoryTracker(ctx) }()
	wg.Add(1)
	go func() { defer wg.Done(); e.timestampPurger(ctx) }()
	wg.Add(1)
	go func() { defer wg.Done(); e.gapDetector(ctx) }()

	if cfg.ForcedDisconnInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allW := e.AllWorkers()
			dm := disconnect.New(cfg.ForcedDisconnInterval, cfg.ForcedDisconnDuration, allW, e.logger)
			dm.Run(ctx)
		}()
	}
}

func (e *Engine) periodicReporter(ctx context.Context, cfg *config.Config) {
	ticker := time.NewTicker(cfg.ReportIntervalParsed)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.logStatus()
		}
	}
}

func (e *Engine) logStatus() {
	e.mu.Lock()
	groups := e.patternGroups
	e.mu.Unlock()

	for _, pattern := range config.AllPatternNames {
		pg := groups[pattern]
		if pg == nil {
			continue
		}

		var sent, received, errors, duplicated uint64
		var rpcSuccess, rpcTimeout, rpcError uint64
		var rate float64

		for _, w := range pg.Workers() {
			sent += w.SentCount()
			received += w.ReceivedCount()
			errors += w.ErrorCount()
			duplicated += w.DuplicatedCount()
			rpcSuccess += w.RPCSuccess()
			rpcTimeout += w.RPCTimeout()
			rpcError += w.RPCError()
			rate += w.RateWindow().Rate()
		}

		if config.IsRPCPattern(pattern) {
			e.logger.Info(pattern, "sent", sent, "resp", rpcSuccess, "tout", rpcTimeout,
				"err", rpcError, "rate", fmt.Sprintf("%.1f/s", rate))
		} else {
			var lost uint64
			for _, w := range pg.Workers() {
				lost += w.Tracker().TotalLost()
			}
			e.logger.Info(pattern, "sent", sent, "recv", received, "lost", lost,
				"dup", duplicated, "err", errors, "rate", fmt.Sprintf("%.1f/s", rate))
		}
	}
}

func (e *Engine) rateAdvancer(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.mu.Lock()
			groups := e.patternGroups
			e.mu.Unlock()

			for pattern, pg := range groups {
				var totalRate float64
				var totalSent, totalRecv uint64
				for _, w := range pg.Workers() {
					w.AdvanceRateWindows()
					totalRate += w.RateWindow().Rate()
					totalSent += w.SentCount()
					totalRecv += w.ReceivedCount()
				}
				metrics.SetActualRate(pattern, totalRate)
				if totalSent > totalRecv {
					metrics.SetConsumerLag(pattern, float64(totalSent-totalRecv))
				} else {
					metrics.SetConsumerLag(pattern, 0)
				}
			}
		}
	}
}

func (e *Engine) uptimeTracker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.UptimeSeconds.WithLabelValues(metrics.SDK()).Set(time.Since(e.bootTime).Seconds())
		}
	}
}

func (e *Engine) memoryTracker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	start := time.Now()
	baselineSet := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.Sys > e.peakRSS.Load() {
				e.peakRSS.Store(m.Sys)
			}
			if !baselineSet && time.Since(start) >= 5*time.Minute {
				e.baselineRSS.Store(m.Sys)
				baselineSet = true
				e.logger.Info("memory baseline captured", "rss_mb", m.Sys/1024/1024)
			}
		}
	}
}

func (e *Engine) timestampPurger(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.mu.Lock()
			groups := e.patternGroups
			e.mu.Unlock()
			for _, pg := range groups {
				for _, w := range pg.Workers() {
					w.TSStore().Purge(60 * time.Second)
				}
			}
		}
	}
}

func (e *Engine) gapDetector(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.mu.Lock()
			groups := e.patternGroups
			e.mu.Unlock()
			for pattern, pg := range groups {
				for _, w := range pg.Workers() {
					deltas := w.Tracker().DetectGaps()
					for _, delta := range deltas {
						metrics.AddLost(pattern, delta)
					}
				}
			}
		}
	}
}

// --- Error helper ---

func (e *Engine) setError(msg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state = StateError
	e.runError = msg
	e.logger.Error("engine error", "error", msg)
}

// --- Verdict ---

func (e *Engine) computeVerdict(cfg *config.Config) {
	result := &VerdictResult{Result: "PASSED", Passed: true}

	measurementDuration := e.producersStoppedAt.Sub(e.producersStartedAt)

	for pattern, snap := range e.patternSnapshots {
		if snap.Corrupted > 0 {
			result.Result = "FAILED"
			result.Passed = false
			result.Warnings = append(result.Warnings, fmt.Sprintf("%s: %d corrupted messages", pattern, snap.Corrupted))
		}

		if snap.Sent > 0 {
			lossPct := float64(snap.Lost) / float64(snap.Sent) * 100
			threshold := cfg.Thresholds.MaxLossPct
			if pattern == worker.PatternEvents {
				threshold = cfg.Thresholds.MaxEventsLossPct
			}
			if lossPct > threshold {
				result.Result = "FAILED"
				result.Passed = false
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: loss %.4f%% exceeds threshold %.4f%%", pattern, lossPct, threshold))
			}
		}

		if snap.Received > 0 {
			dupPct := float64(snap.Duplicated) / float64(snap.Received) * 100
			if dupPct > cfg.Thresholds.MaxDuplicationPct {
				result.Result = "FAILED"
				result.Passed = false
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: duplication %.4f%% exceeds threshold %.4f%%", pattern, dupPct, cfg.Thresholds.MaxDuplicationPct))
			}
		}

		if snap.LatencyP50 > 0 && snap.LatencyP50 > cfg.Thresholds.MaxP50LatencyMS {
			result.Result = "FAILED"
			result.Passed = false
			result.Warnings = append(result.Warnings, fmt.Sprintf("%s: p50 latency %.1fms exceeds %.1fms", pattern, snap.LatencyP50, cfg.Thresholds.MaxP50LatencyMS))
		}
		if snap.LatencyP95 > 0 && snap.LatencyP95 > cfg.Thresholds.MaxP95LatencyMS {
			result.Result = "FAILED"
			result.Passed = false
			result.Warnings = append(result.Warnings, fmt.Sprintf("%s: p95 latency %.1fms exceeds %.1fms", pattern, snap.LatencyP95, cfg.Thresholds.MaxP95LatencyMS))
		}
		if snap.LatencyP99 > 0 && snap.LatencyP99 > cfg.Thresholds.MaxP99LatencyMS {
			result.Result = "FAILED"
			result.Passed = false
			result.Warnings = append(result.Warnings, fmt.Sprintf("%s: p99 latency %.1fms exceeds %.1fms", pattern, snap.LatencyP99, cfg.Thresholds.MaxP99LatencyMS))
		}
		if snap.LatencyP999 > 0 && snap.LatencyP999 > cfg.Thresholds.MaxP999LatencyMS {
			result.Result = "FAILED"
			result.Passed = false
			result.Warnings = append(result.Warnings, fmt.Sprintf("%s: p999 latency %.1fms exceeds %.1fms", pattern, snap.LatencyP999, cfg.Thresholds.MaxP999LatencyMS))
		}

		total := snap.Sent + snap.Received
		if total > 0 {
			errPct := float64(snap.Errors) / float64(total) * 100
			if errPct > cfg.Thresholds.MaxErrorRatePct {
				result.Result = "FAILED"
				result.Passed = false
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: error rate %.4f%% exceeds %.4f%%", pattern, errPct, cfg.Thresholds.MaxErrorRatePct))
			}
		}

		if measurementDuration > 0 && snap.Sent > 0 {
			targetRate := float64(cfg.GetPatternRate(pattern))
			if targetRate > 0 {
				actualRate := float64(snap.Sent) / measurementDuration.Seconds()
				throughputPct := actualRate / targetRate * 100
				if throughputPct < cfg.Thresholds.MinThroughputPct {
					result.Result = "FAILED"
					result.Passed = false
					result.Warnings = append(result.Warnings, fmt.Sprintf("%s: throughput %.1f%% below %.1f%%", pattern, throughputPct, cfg.Thresholds.MinThroughputPct))
				}
			}
		}

		if measurementDuration > 0 && snap.DowntimeSeconds > 0 {
			downtimePct := snap.DowntimeSeconds / measurementDuration.Seconds() * 100
			if downtimePct > cfg.Thresholds.MaxDowntimePct {
				result.Result = "FAILED"
				result.Passed = false
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: downtime %.1f%% exceeds %.1f%%", pattern, downtimePct, cfg.Thresholds.MaxDowntimePct))
			}
		}
	}

	baseline := e.baselineRSS.Load()
	peak := e.peakRSS.Load()
	if baseline > 0 && peak > baseline {
		growth := float64(peak) / float64(baseline)
		if growth > cfg.Thresholds.MaxMemoryGrowthFactor {
			if result.Passed {
				result.Result = "PASSED_WITH_WARNINGS"
			}
			result.Warnings = append(result.Warnings, fmt.Sprintf("memory growth %.2fx exceeds threshold %.2fx", growth, cfg.Thresholds.MaxMemoryGrowthFactor))
		} else if growth > cfg.Thresholds.MaxMemoryGrowthFactor*0.5 {
			if result.Passed {
				result.Result = "PASSED_WITH_WARNINGS"
			}
			result.Warnings = append(result.Warnings, fmt.Sprintf("memory growth trend %.2fx (50%% of threshold %.2fx)", growth, cfg.Thresholds.MaxMemoryGrowthFactor))
		}
	}

	e.mu.Lock()
	e.verdictResult = result
	e.mu.Unlock()
}

func (e *Engine) logFinalSummary() {
	e.mu.Lock()
	verdict := e.verdictResult
	snapshots := e.patternSnapshots
	e.mu.Unlock()

	if verdict == nil {
		return
	}

	e.logger.Info("═══════════════════════════════════════════")
	e.logger.Info("burn-in verdict", "result", verdict.Result, "passed", verdict.Passed)

	for _, pattern := range config.AllPatternNames {
		snap := snapshots[pattern]
		if snap == nil {
			continue
		}
		if config.IsRPCPattern(pattern) {
			e.logger.Info(fmt.Sprintf("  %s", pattern), "sent", snap.Sent, "resp", snap.RPCSuccess,
				"tout", snap.RPCTimeout, "err", snap.RPCError, "corrupted", snap.Corrupted)
		} else {
			e.logger.Info(fmt.Sprintf("  %s", pattern), "sent", snap.Sent, "recv", snap.Received,
				"lost", snap.Lost, "dup", snap.Duplicated, "err", snap.Errors, "corrupted", snap.Corrupted)
		}
	}

	if len(verdict.Warnings) > 0 {
		for _, w := range verdict.Warnings {
			e.logger.Warn("  " + w)
		}
	}
	e.logger.Info("═══════════════════════════════════════════")
}
