package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/kubemq-io/kubemq-mqtt/burnin/config"
	"github.com/kubemq-io/kubemq-mqtt/burnin/metrics"
	"github.com/kubemq-io/kubemq-mqtt/burnin/payload"
	"github.com/kubemq-io/kubemq-mqtt/burnin/tracker"
	"github.com/kubemq-io/kubemq-mqtt/burnin/transport"
)

const (
	PatternEvents      = "events"
	PatternEventsStore = "events_store"
	PatternQueues      = "queues"
	PatternCommands    = "commands"
	PatternQueries     = "queries"
)

func AllPatterns() []string {
	return []string{PatternEvents, PatternEventsStore, PatternQueues, PatternCommands, PatternQueries}
}

// TransportFactory creates one Transport (one MQTT connection) for the given
// client ID. The engine injects a paho-backed factory; tests inject a fake.
// Workers use it to open their producer/consumer connections so they never
// import paho directly.
type TransportFactory func(clientID string, opts transport.ConnectOptions) transport.Transport

// Worker is the per-channel unit of work. One Worker drives a single channel of
// a single pattern, owning its producer goroutine(s), consumer/responder
// goroutine(s), trackers, and latency accumulators. The engine compiles against
// this interface only; concrete workers live in events.go/queues.go/etc.
type Worker interface {
	Pattern() string
	ChannelName() string
	ChannelIndex() int

	// Start brings up consumers/subscriptions (and, for RPC patterns, the
	// $reply subscription) and signals ConsumerReady when subscribed.
	Start(ctx context.Context) error
	StartProducers()
	Stop()
	StopProducers()
	StopConsumers()
	// DisconnectConsumers force-drops consumer connections (no clean
	// DISCONNECT) so autopaho reconnects — drives the forced-disconnect test.
	DisconnectConsumers()
	ConsumerReady() <-chan struct{}

	Tracker() *tracker.Tracker
	LatencyAccumulator() *metrics.LatencyAccumulator
	RPCLatencyAccumulator() *metrics.LatencyAccumulator
	PeakRate() *metrics.PeakRateTracker
	RateWindow() *metrics.SlidingRateWindow
	TSStore() *metrics.SendTimestampStore

	SentCount() uint64
	ReceivedCount() uint64
	ErrorCount() uint64
	CorruptedCount() uint64
	ReconnectionCount() uint64
	DowntimeSeconds() float64
	BytesSentCount() uint64
	BytesReceivedCount() uint64
	DuplicatedCount() uint64

	RPCSuccess() uint64
	RPCTimeout() uint64
	RPCError() uint64

	AdvanceRateWindows()
	ResetAfterWarmup()
	ProducerStatSnapshots() []WorkerStatSnapshot
	ConsumerStatSnapshots() []WorkerStatSnapshot
}

type WorkerStat struct {
	ID   string
	Sent atomic.Uint64
	Recv atomic.Uint64
}

type WorkerStatSnapshot struct {
	ID   string
	Sent uint64
	Recv uint64
}

// BaseWorker holds the shared state and lifecycle plumbing for every pattern
// worker. Pattern-specific files embed *BaseWorker and implement Start,
// StartProducers, StopConsumers, and DisconnectConsumers.
type BaseWorker struct {
	pattern      string
	channelName  string
	channelIndex int
	cfg          *config.Config
	patternCfg   *config.PatternConfig
	logger       *slog.Logger

	transportFactory TransportFactory

	trk         *tracker.Tracker
	latAccum    *metrics.LatencyAccumulator
	rpcLatAccum *metrics.LatencyAccumulator
	peakRate    *metrics.PeakRateTracker
	rateWindow  *metrics.SlidingRateWindow
	tsStore     *metrics.SendTimestampStore
	sizeDistrib *payload.SizeDistribution

	limiter *rate.Limiter

	producerCtx    context.Context
	producerCancel context.CancelFunc
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
	producerWG     sync.WaitGroup
	consumerWG     sync.WaitGroup
	consumerReady  chan struct{}

	sent          atomic.Uint64
	received      atomic.Uint64
	errors        atomic.Uint64
	corrupted     atomic.Uint64
	reconnections atomic.Uint64
	downtime      atomic.Uint64 // nanoseconds
	bytesSent     atomic.Uint64
	bytesReceived atomic.Uint64
	duplicated    atomic.Uint64

	rpcSuccessCount atomic.Uint64
	rpcTimeoutCount atomic.Uint64
	rpcErrorCount   atomic.Uint64

	producerStats []*WorkerStat
	consumerStats []*WorkerStat

	downtimeStart atomic.Int64

	// Transports opened by this worker (producer + consumer connections),
	// tracked so DisconnectConsumers can force-drop them.
	transports   []transport.Transport
	transportsMu sync.Mutex
}

// NewBaseWorker constructs the shared worker state for one channel.
func NewBaseWorker(pattern, channelName string, channelIndex int, cfg *config.Config, factory TransportFactory, logger *slog.Logger) *BaseWorker {
	patternCfg := cfg.GetPatternConfig(pattern)

	targetRate := float64(patternCfg.Rate)
	burst := int(targetRate)
	if burst < 1 {
		burst = 1
	}

	var sizeDistrib *payload.SizeDistribution
	if cfg.Message.SizeMode == "distribution" {
		sizeDistrib, _ = payload.ParseDistribution(cfg.Message.SizeDistribution)
	}

	return &BaseWorker{
		pattern:          pattern,
		channelName:      channelName,
		channelIndex:     channelIndex,
		cfg:              cfg,
		patternCfg:       patternCfg,
		logger:           logger.With("pattern", pattern, "channel", channelName),
		transportFactory: factory,

		trk:         tracker.New(cfg.Message.ReorderWindow),
		latAccum:    metrics.NewLatencyAccumulator(),
		rpcLatAccum: metrics.NewLatencyAccumulator(),
		peakRate:    metrics.NewPeakRateTracker(),
		rateWindow:  metrics.NewSlidingRateWindow(),
		tsStore:     metrics.NewSendTimestampStore(),
		sizeDistrib: sizeDistrib,

		limiter:       rate.NewLimiter(rate.Limit(targetRate), burst),
		consumerReady: make(chan struct{}),
	}
}

// --- Accessors ---

func (b *BaseWorker) Pattern() string                                    { return b.pattern }
func (b *BaseWorker) ChannelName() string                                { return b.channelName }
func (b *BaseWorker) ChannelIndex() int                                  { return b.channelIndex }
func (b *BaseWorker) ConsumerReady() <-chan struct{}                     { return b.consumerReady }
func (b *BaseWorker) Tracker() *tracker.Tracker                          { return b.trk }
func (b *BaseWorker) LatencyAccumulator() *metrics.LatencyAccumulator    { return b.latAccum }
func (b *BaseWorker) RPCLatencyAccumulator() *metrics.LatencyAccumulator { return b.rpcLatAccum }
func (b *BaseWorker) PeakRate() *metrics.PeakRateTracker                 { return b.peakRate }
func (b *BaseWorker) RateWindow() *metrics.SlidingRateWindow             { return b.rateWindow }
func (b *BaseWorker) TSStore() *metrics.SendTimestampStore               { return b.tsStore }

func (b *BaseWorker) SentCount() uint64          { return b.sent.Load() }
func (b *BaseWorker) ReceivedCount() uint64      { return b.received.Load() }
func (b *BaseWorker) ErrorCount() uint64         { return b.errors.Load() }
func (b *BaseWorker) CorruptedCount() uint64     { return b.corrupted.Load() }
func (b *BaseWorker) ReconnectionCount() uint64  { return b.reconnections.Load() }
func (b *BaseWorker) BytesSentCount() uint64     { return b.bytesSent.Load() }
func (b *BaseWorker) BytesReceivedCount() uint64 { return b.bytesReceived.Load() }
func (b *BaseWorker) DuplicatedCount() uint64    { return b.duplicated.Load() }

func (b *BaseWorker) RPCSuccess() uint64 { return b.rpcSuccessCount.Load() }
func (b *BaseWorker) RPCTimeout() uint64 { return b.rpcTimeoutCount.Load() }
func (b *BaseWorker) RPCError() uint64   { return b.rpcErrorCount.Load() }

func (b *BaseWorker) DowntimeSeconds() float64 {
	return float64(b.downtime.Load()) / float64(time.Second)
}

// --- Protected helpers for embedding pattern workers ---

func (b *BaseWorker) Config() *config.Config               { return b.cfg }
func (b *BaseWorker) PatternConfig() *config.PatternConfig { return b.patternCfg }
func (b *BaseWorker) Logger() *slog.Logger                 { return b.logger }
func (b *BaseWorker) Factory() TransportFactory            { return b.transportFactory }

func (b *BaseWorker) IncSent()                  { b.sent.Add(1) }
func (b *BaseWorker) IncReceived()              { b.received.Add(1) }
func (b *BaseWorker) IncErrors()                { b.errors.Add(1) }
func (b *BaseWorker) IncCorrupted()             { b.corrupted.Add(1) }
func (b *BaseWorker) IncReconnections()         { b.reconnections.Add(1) }
func (b *BaseWorker) IncDuplicated()            { b.duplicated.Add(1) }
func (b *BaseWorker) AddBytesSent(n uint64)     { b.bytesSent.Add(n) }
func (b *BaseWorker) AddBytesReceived(n uint64) { b.bytesReceived.Add(n) }
func (b *BaseWorker) IncRPCSuccess()            { b.rpcSuccessCount.Add(1) }
func (b *BaseWorker) IncRPCTimeout()            { b.rpcTimeoutCount.Add(1) }
func (b *BaseWorker) IncRPCError()              { b.rpcErrorCount.Add(1) }

// SignalConsumerReady closes the consumerReady channel exactly once.
func (b *BaseWorker) SignalConsumerReady() {
	select {
	case <-b.consumerReady:
	default:
		close(b.consumerReady)
	}
}

// ProducerContext / ConsumerContext create the cancellable contexts the embedding
// worker uses to scope its goroutines. Producers and consumers are stopped
// independently to support the two-phase (producers → drain → consumers) shutdown.
func (b *BaseWorker) NewProducerContext(parent context.Context) context.Context {
	b.producerCtx, b.producerCancel = context.WithCancel(parent)
	return b.producerCtx
}

func (b *BaseWorker) NewConsumerContext(parent context.Context) context.Context {
	b.consumerCtx, b.consumerCancel = context.WithCancel(parent)
	return b.consumerCtx
}

func (b *BaseWorker) ProducerWG() *sync.WaitGroup { return &b.producerWG }
func (b *BaseWorker) ConsumerWG() *sync.WaitGroup { return &b.consumerWG }

// RegisterTransport tracks a transport for force-disconnect bookkeeping.
func (b *BaseWorker) RegisterTransport(t transport.Transport) {
	b.transportsMu.Lock()
	b.transports = append(b.transports, t)
	b.transportsMu.Unlock()
}

// --- Lifecycle ---

func (b *BaseWorker) StopProducers() {
	if b.producerCancel != nil {
		b.producerCancel()
	}
	b.producerWG.Wait()
}

func (b *BaseWorker) StopConsumers() {
	if b.consumerCancel != nil {
		b.consumerCancel()
	}
	b.consumerWG.Wait()
}

func (b *BaseWorker) Stop() {
	b.StopProducers()
	b.StopConsumers()
}

// DisconnectConsumers force-drops every registered transport (no clean
// DISCONNECT) so autopaho reconnects. Embedding workers may override to scope
// the drop to consumer connections only.
func (b *BaseWorker) DisconnectConsumers() {
	b.transportsMu.Lock()
	defer b.transportsMu.Unlock()
	for _, t := range b.transports {
		t.ForceDisconnect()
	}
}

// --- Rate control ---

func (b *BaseWorker) WaitForRate(ctx context.Context) error {
	return b.limiter.Wait(ctx)
}

func (b *BaseWorker) BackpressureCheck() bool {
	s := b.sent.Load()
	r := b.received.Load()
	if s <= r {
		return false
	}
	return s-r > uint64(b.cfg.Queue.MaxDepth)
}

// --- Message size ---

func (b *BaseWorker) SelectMessageSize() int {
	if b.cfg.Message.SizeMode == "distribution" && b.sizeDistrib != nil {
		return b.sizeDistrib.SelectSize()
	}
	return b.cfg.Message.SizeBytes
}

// --- Downtime tracking ---

func (b *BaseWorker) StartDowntime() {
	b.downtimeStart.Store(time.Now().UnixNano())
}

func (b *BaseWorker) StopDowntime() {
	start := b.downtimeStart.Swap(0)
	if start > 0 {
		dur := time.Now().UnixNano() - start
		b.downtime.Add(uint64(dur))
		metrics.AddDowntime(b.pattern, float64(dur)/float64(time.Second))
	}
}

// --- Rate windows ---

func (b *BaseWorker) AdvanceRateWindows() {
	b.rateWindow.Advance()
	b.peakRate.Advance()
}

// --- Warmup reset ---

func (b *BaseWorker) ResetAfterWarmup() {
	b.trk.Reset()
	b.latAccum.Reset()
	b.rpcLatAccum.Reset()
	b.peakRate = metrics.NewPeakRateTracker()
	b.rateWindow.Reset()
	b.tsStore.Purge(0)

	b.sent.Store(0)
	if b.pattern != PatternQueues {
		b.received.Store(0)
	}
	b.errors.Store(0)
	b.corrupted.Store(0)
	b.reconnections.Store(0)
	b.downtime.Store(0)
	b.bytesSent.Store(0)
	b.bytesReceived.Store(0)
	b.duplicated.Store(0)
	b.rpcSuccessCount.Store(0)
	b.rpcTimeoutCount.Store(0)
	b.rpcErrorCount.Store(0)
}

// --- Stat snapshots ---

func (b *BaseWorker) ProducerStatSnapshots() []WorkerStatSnapshot {
	snaps := make([]WorkerStatSnapshot, len(b.producerStats))
	for i, s := range b.producerStats {
		snaps[i] = WorkerStatSnapshot{ID: s.ID, Sent: s.Sent.Load(), Recv: s.Recv.Load()}
	}
	return snaps
}

func (b *BaseWorker) ConsumerStatSnapshots() []WorkerStatSnapshot {
	snaps := make([]WorkerStatSnapshot, len(b.consumerStats))
	for i, s := range b.consumerStats {
		snaps[i] = WorkerStatSnapshot{ID: s.ID, Sent: s.Sent.Load(), Recv: s.Recv.Load()}
	}
	return snaps
}

// AddProducerStat / AddConsumerStat register a per-goroutine stat counter,
// returning it so the goroutine can increment it directly.
func (b *BaseWorker) AddProducerStat(id string) *WorkerStat {
	s := &WorkerStat{ID: id}
	b.producerStats = append(b.producerStats, s)
	return s
}

func (b *BaseWorker) AddConsumerStat(id string) *WorkerStat {
	s := &WorkerStat{ID: id}
	b.consumerStats = append(b.consumerStats, s)
	return s
}

// ConnectOptions builds the shared MQTT ConnectOptions for this worker's
// connections from config, with downtime/reconnection callbacks wired in.
func (b *BaseWorker) ConnectOptions(clientID string) transport.ConnectOptions {
	return transport.ConnectOptions{
		BrokerAddress:        b.cfg.Broker.Address,
		ClientID:             clientID,
		ProtocolVersion:      b.cfg.MQTT.ProtocolVersion,
		CleanStart:           b.cfg.MQTT.CleanStart,
		KeepAlive:            time.Duration(b.cfg.MQTT.KeepaliveSecs) * time.Second,
		ReconnectInterval:    b.cfg.ReconnectInterval,
		ReconnectMaxInterval: b.cfg.ReconnectMaxInterval,
		OnConnect: func() {
			b.StopDowntime()
		},
		OnDisconnect: func(error) {
			b.IncReconnections()
			metrics.IncReconnection(b.pattern)
			b.StartDowntime()
		},
	}
}
