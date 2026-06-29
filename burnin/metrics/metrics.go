package metrics

import (
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const sdkLabel = "mqtt"

var (
	latencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	rpcBuckets     = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

	// ── Counters ──

	MessagesSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_sent_total",
		Help: "Total messages sent",
	}, []string{"sdk", "pattern", "producerid"})

	MessagesReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_received_total",
		Help: "Total messages received",
	}, []string{"sdk", "pattern", "consumer_id"})

	MessagesLostTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_lost_total",
		Help: "Confirmed lost messages",
	}, []string{"sdk", "pattern"})

	MessagesDuplicatedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_duplicated_total",
		Help: "Messages detected as duplicated",
	}, []string{"sdk", "pattern"})

	MessagesCorruptedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_corrupted_total",
		Help: "Messages with CRC32 hash mismatch",
	}, []string{"sdk", "pattern"})

	MessagesOutOfOrderTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_out_of_order_total",
		Help: "Messages received out of sequence order",
	}, []string{"sdk", "pattern"})

	ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_errors_total",
		Help: "Errors by type",
	}, []string{"sdk", "pattern", "error_type"})

	ReconnectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_reconnections_total",
		Help: "Number of reconnection events",
	}, []string{"sdk", "pattern"})

	BytesSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_bytes_sent_total",
		Help: "Total bytes sent",
	}, []string{"sdk", "pattern"})

	BytesReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_bytes_received_total",
		Help: "Total bytes received",
	}, []string{"sdk", "pattern"})

	RPCResponsesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_rpc_responses_total",
		Help: "RPC responses by status",
	}, []string{"sdk", "pattern", "status"})

	DowntimeSecondsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_downtime_seconds_total",
		Help: "Cumulative time spent in reconnecting state",
	}, []string{"sdk", "pattern"})

	ForcedDisconnectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_forced_disconnects_total",
		Help: "Number of forced disconnect events",
	}, []string{"sdk"})

	// MQTT-specific: PUBACK / SUBACK reason-code distribution. 0x00 success,
	// 0x80 broker-not-ready, 0x83 impl-specific, 0x97 quota exceeded,
	// 0x9A retain-not-supported, 0xA2 wildcard-subs-not-supported, etc.
	PubackReasonTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_mqtt_puback_reason_total",
		Help: "MQTT PUBACK reason-code distribution",
	}, []string{"sdk", "pattern", "reason_code"})

	SubackReasonTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_mqtt_suback_reason_total",
		Help: "MQTT SUBACK reason-code distribution",
	}, []string{"sdk", "pattern", "reason_code"})

	QueuePollEmptyTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_queue_poll_empty_total",
		Help: "Empty queue poll responses",
	}, []string{"sdk", "pattern"})

	// ── Histograms ──

	MessageLatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_message_latency_seconds",
		Help:    "End-to-end message latency",
		Buckets: latencyBuckets,
	}, []string{"sdk", "pattern"})

	SendDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_send_duration_seconds",
		Help:    "MQTT publish-to-PUBACK round-trip time",
		Buckets: latencyBuckets,
	}, []string{"sdk", "pattern"})

	RPCDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_rpc_duration_seconds",
		Help:    "Command/query RPC round-trip duration ($reply wait)",
		Buckets: rpcBuckets,
	}, []string{"sdk", "pattern"})

	// ── Gauges ──

	ActiveConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_active_connections",
		Help: "Currently active MQTT connections",
	}, []string{"sdk", "pattern"})

	UptimeSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_uptime_seconds",
		Help: "Burn-in app uptime",
	}, []string{"sdk"})

	TargetRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_target_rate",
		Help: "Configured target rate (msgs/sec)",
	}, []string{"sdk", "pattern"})

	ActualRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_actual_rate",
		Help: "Current achieved rate (msgs/sec)",
	}, []string{"sdk", "pattern"})

	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_consumer_lag_messages",
		Help: "Sent minus received (producer-consumer lag)",
	}, []string{"sdk", "pattern"})

	WarmupActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_warmup_active",
		Help: "1 during warmup period, 0 after",
	}, []string{"sdk"})
)

func SDK() string { return sdkLabel }

// InitMetrics pre-initializes all Prometheus metrics to 0 with well-known
// label values to prevent absent() alerts in dashboards.
func InitMetrics() {
	patterns := []string{"events", "events_store", "queues", "commands", "queries"}
	errorTypes := []string{
		"send_failure", "publish_rejected", "subscribe_failure", "poll_failure",
		"poll_error", "decode_failure", "rpc_timeout", "rpc_error",
		"missing_response_topic", "missing_correlation_data", "malformed_message",
	}

	for _, p := range patterns {
		MessagesSentTotal.WithLabelValues(sdkLabel, p, "p-"+p+"-000").Add(0)
		MessagesReceivedTotal.WithLabelValues(sdkLabel, p, "c-"+p+"-000").Add(0)
		MessagesLostTotal.WithLabelValues(sdkLabel, p).Add(0)
		MessagesDuplicatedTotal.WithLabelValues(sdkLabel, p).Add(0)
		MessagesCorruptedTotal.WithLabelValues(sdkLabel, p).Add(0)
		MessagesOutOfOrderTotal.WithLabelValues(sdkLabel, p).Add(0)
		ReconnectionsTotal.WithLabelValues(sdkLabel, p).Add(0)
		BytesSentTotal.WithLabelValues(sdkLabel, p).Add(0)
		BytesReceivedTotal.WithLabelValues(sdkLabel, p).Add(0)
		DowntimeSecondsTotal.WithLabelValues(sdkLabel, p).Add(0)
		QueuePollEmptyTotal.WithLabelValues(sdkLabel, p).Add(0)
		PubackReasonTotal.WithLabelValues(sdkLabel, p, "0x00").Add(0)
		SubackReasonTotal.WithLabelValues(sdkLabel, p, "0x00").Add(0)

		for _, et := range errorTypes {
			ErrorsTotal.WithLabelValues(sdkLabel, p, et).Add(0)
		}

		ActiveConnections.WithLabelValues(sdkLabel, p).Set(0)
		TargetRate.WithLabelValues(sdkLabel, p).Set(0)
		ActualRate.WithLabelValues(sdkLabel, p).Set(0)
		ConsumerLag.WithLabelValues(sdkLabel, p).Set(0)
	}

	rpcPatterns := []string{"commands", "queries"}
	for _, p := range rpcPatterns {
		RPCResponsesTotal.WithLabelValues(sdkLabel, p, "success").Add(0)
		RPCResponsesTotal.WithLabelValues(sdkLabel, p, "timeout").Add(0)
		RPCResponsesTotal.WithLabelValues(sdkLabel, p, "error").Add(0)
	}

	ForcedDisconnectsTotal.WithLabelValues(sdkLabel).Add(0)
	UptimeSeconds.WithLabelValues(sdkLabel).Set(0)
	WarmupActive.WithLabelValues(sdkLabel).Set(0)
}

// ── Counter helpers ──

func IncSent(pattern, producerID string) {
	MessagesSentTotal.WithLabelValues(sdkLabel, pattern, producerID).Inc()
}

func IncReceived(pattern, consumerID string) {
	MessagesReceivedTotal.WithLabelValues(sdkLabel, pattern, consumerID).Inc()
}

func AddLost(pattern string, delta uint64) {
	MessagesLostTotal.WithLabelValues(sdkLabel, pattern).Add(float64(delta))
}

func IncDuplicated(pattern string) {
	MessagesDuplicatedTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

func IncCorrupted(pattern string) {
	MessagesCorruptedTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

func IncOutOfOrder(pattern string) {
	MessagesOutOfOrderTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

func IncError(pattern, errorType string) {
	ErrorsTotal.WithLabelValues(sdkLabel, pattern, errorType).Inc()
}

func IncReconnection(pattern string) {
	ReconnectionsTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

func ObserveLatency(pattern string, d time.Duration) {
	MessageLatencySeconds.WithLabelValues(sdkLabel, pattern).Observe(d.Seconds())
}

func ObserveSendDuration(pattern string, d time.Duration) {
	SendDurationSeconds.WithLabelValues(sdkLabel, pattern).Observe(d.Seconds())
}

func ObserveRPCDuration(pattern string, d time.Duration) {
	RPCDurationSeconds.WithLabelValues(sdkLabel, pattern).Observe(d.Seconds())
}

func IncRPCResponse(pattern, status string) {
	RPCResponsesTotal.WithLabelValues(sdkLabel, pattern, status).Inc()
}

func AddDowntime(pattern string, seconds float64) {
	DowntimeSecondsTotal.WithLabelValues(sdkLabel, pattern).Add(seconds)
}

func IncForcedDisconnect() {
	ForcedDisconnectsTotal.WithLabelValues(sdkLabel).Inc()
}

// IncPubackReason records an MQTT PUBACK reason code (e.g. "0x00", "0x97").
func IncPubackReason(pattern, reasonCode string) {
	PubackReasonTotal.WithLabelValues(sdkLabel, pattern, reasonCode).Inc()
}

// IncSubackReason records an MQTT SUBACK reason code (e.g. "0x00", "0x83", "0xA2").
func IncSubackReason(pattern, reasonCode string) {
	SubackReasonTotal.WithLabelValues(sdkLabel, pattern, reasonCode).Inc()
}

func IncQueuePollEmpty(pattern string) {
	QueuePollEmptyTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

func RecordBytesSent(pattern string, n int) {
	BytesSentTotal.WithLabelValues(sdkLabel, pattern).Add(float64(n))
}

func RecordBytesReceived(pattern string, n int) {
	BytesReceivedTotal.WithLabelValues(sdkLabel, pattern).Add(float64(n))
}

// ── Gauge helpers ──

func SetActiveConnections(pattern string, n float64) {
	ActiveConnections.WithLabelValues(sdkLabel, pattern).Set(n)
}

func SetTargetRate(pattern string, rate float64) {
	TargetRate.WithLabelValues(sdkLabel, pattern).Set(rate)
}

func SetActualRate(pattern string, rate float64) {
	ActualRate.WithLabelValues(sdkLabel, pattern).Set(rate)
}

func SetConsumerLag(pattern string, lag float64) {
	ConsumerLag.WithLabelValues(sdkLabel, pattern).Set(lag)
}

// ── In-Memory Accumulators ──

// LatencyAccumulator tracks latency values in-memory for percentile
// computation at verdict time. Uses HdrHistogram for memory-efficient,
// high-precision percentile calculation.
type LatencyAccumulator struct {
	mu   sync.Mutex
	hist *hdrhistogram.Histogram
}

// NewLatencyAccumulator creates a new accumulator recording latencies
// from 1 microsecond to 60 seconds with 3 significant digits.
func NewLatencyAccumulator() *LatencyAccumulator {
	return &LatencyAccumulator{
		hist: hdrhistogram.New(1, 60_000_000, 3),
	}
}

func (a *LatencyAccumulator) Record(d time.Duration) {
	a.mu.Lock()
	_ = a.hist.RecordValue(d.Microseconds())
	a.mu.Unlock()
}

// Percentiles returns P50, P95, P99, P99.9 latencies in milliseconds.
func (a *LatencyAccumulator) Percentiles() (p50, p95, p99, p999 float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p50 = float64(a.hist.ValueAtQuantile(50)) / 1000.0
	p95 = float64(a.hist.ValueAtQuantile(95)) / 1000.0
	p99 = float64(a.hist.ValueAtQuantile(99)) / 1000.0
	p999 = float64(a.hist.ValueAtQuantile(99.9)) / 1000.0
	return
}

func (a *LatencyAccumulator) Reset() {
	a.mu.Lock()
	a.hist.Reset()
	a.mu.Unlock()
}

func (a *LatencyAccumulator) Count() int64 {
	a.mu.Lock()
	c := a.hist.TotalCount()
	a.mu.Unlock()
	return c
}

// SendTimestampStore tracks monotonic send timestamps for latency computation.
type SendTimestampStore struct {
	store sync.Map
}

func NewSendTimestampStore() *SendTimestampStore {
	return &SendTimestampStore{}
}

type tsKey struct {
	ProducerID string
	Seq        uint64
}

func (s *SendTimestampStore) Store(producerID string, seq uint64, t time.Time) {
	s.store.Store(tsKey{producerID, seq}, t)
}

func (s *SendTimestampStore) LoadAndDelete(producerID string, seq uint64) (time.Time, bool) {
	v, ok := s.store.LoadAndDelete(tsKey{producerID, seq})
	if !ok {
		return time.Time{}, false
	}
	return v.(time.Time), true
}

func (s *SendTimestampStore) Purge(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)
	s.store.Range(func(key, value any) bool {
		if t, ok := value.(time.Time); ok && t.Before(cutoff) {
			s.store.Delete(key)
		}
		return true
	})
}

// SlidingRateWindow tracks message rate over a 30-second sliding window.
// Call Advance() once per second to rotate buckets, and Record() per message.
const slidingWindowSize = 30

type SlidingRateWindow struct {
	mu      sync.Mutex
	buckets [slidingWindowSize]int64
	idx     int
	total   int64
	ticks   int
}

func NewSlidingRateWindow() *SlidingRateWindow {
	return &SlidingRateWindow{}
}

func (w *SlidingRateWindow) Record() {
	w.mu.Lock()
	w.buckets[w.idx]++
	w.total++
	w.mu.Unlock()
}

func (w *SlidingRateWindow) Advance() {
	w.mu.Lock()
	w.idx = (w.idx + 1) % slidingWindowSize
	w.total -= w.buckets[w.idx]
	w.buckets[w.idx] = 0
	w.ticks++
	w.mu.Unlock()
}

// Rate returns the average messages/sec over the sliding window.
// Uses min(ticks, 30) as the divisor to avoid undercount during ramp-up.
func (w *SlidingRateWindow) Rate() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	window := w.ticks
	if window > slidingWindowSize {
		window = slidingWindowSize
	}
	if window == 0 {
		return 0
	}
	return float64(w.total) / float64(window)
}

func (w *SlidingRateWindow) Reset() {
	w.mu.Lock()
	for i := range w.buckets {
		w.buckets[i] = 0
	}
	w.total = 0
	w.idx = 0
	w.ticks = 0
	w.mu.Unlock()
}

// PeakRateTracker tracks peak throughput over a 10-second sliding window.
// Call Advance() every second to rotate buckets, and Record() for each message.
const peakWindowSize = 10

type PeakRateTracker struct {
	mu      sync.Mutex
	buckets []int64
	idx     int
	peak    float64
}

func NewPeakRateTracker() *PeakRateTracker {
	return &PeakRateTracker{
		buckets: make([]int64, peakWindowSize),
	}
}

func (p *PeakRateTracker) Record() {
	p.mu.Lock()
	p.buckets[p.idx]++
	p.mu.Unlock()
}

// Advance rotates to the next bucket and updates the peak if the current
// 10-second average exceeds the previous peak. Call once per second.
func (p *PeakRateTracker) Advance() {
	p.mu.Lock()
	defer p.mu.Unlock()

	var total int64
	for _, b := range p.buckets {
		total += b
	}
	avg := float64(total) / float64(peakWindowSize)
	if avg > p.peak {
		p.peak = avg
	}

	p.idx = (p.idx + 1) % peakWindowSize
	p.buckets[p.idx] = 0
}

func (p *PeakRateTracker) Peak() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}
