package worker

import (
	"encoding/binary"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kubemq-io/kubemq-mqtt/burnin/metrics"
	"github.com/kubemq-io/kubemq-mqtt/burnin/payload"
	"github.com/kubemq-io/kubemq-mqtt/burnin/transport"
)

// User-property keys carried on RPC responses by the embedded gRPC responder
// (responder/grpc_responder.go), which the connector surfaces as v5 User
// Properties on the $reply delivery. A command reply carries kubemq-executed
// ("true"/"false") and NO body; a query reply carries a body + metadata/tags.
const (
	propKubeMQExecuted = "kubemq-executed"
	propKubeMQError    = "kubemq-error"
)

// rpcPending tracks an in-flight RPC keyed by its CorrelationData. The waiting
// sender registers a pending entry before publishing, then blocks on its channel
// until the $reply delivery arrives (correlated by CorrelationData) or the
// rpc.timeout_ms deadline elapses.
type rpcPending struct {
	mu      sync.Mutex
	waiters map[string]chan transport.IncomingMessage
	counter atomic.Uint64
}

func newRPCPending() *rpcPending {
	return &rpcPending{waiters: make(map[string]chan transport.IncomingMessage)}
}

// nextCorrelation returns a unique 8-byte CorrelationData and its string key.
func (p *rpcPending) nextCorrelation() ([]byte, string) {
	n := p.counter.Add(1)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b, string(b)
}

// register installs a waiter for key and returns its delivery channel.
func (p *rpcPending) register(key string) chan transport.IncomingMessage {
	ch := make(chan transport.IncomingMessage, 1)
	p.mu.Lock()
	p.waiters[key] = ch
	p.mu.Unlock()
	return ch
}

// deregister removes the waiter for key (called by the sender once it stops
// waiting, so a late/duplicate reply is dropped rather than leaking).
func (p *rpcPending) deregister(key string) {
	p.mu.Lock()
	delete(p.waiters, key)
	p.mu.Unlock()
}

// deliver routes a $reply message to its waiter by CorrelationData. Returns
// false when no waiter is registered (late/unknown correlation).
func (p *rpcPending) deliver(msg transport.IncomingMessage) bool {
	key := string(msg.CorrelationData)
	p.mu.Lock()
	ch, ok := p.waiters[key]
	if ok {
		delete(p.waiters, key)
	}
	p.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- msg:
	default:
	}
	return true
}

// User-property keys used to carry the per-message tracking metadata as MQTT 5.0
// User Properties (which the connector maps to/from KubeMQ Tags for
// events/store/queues on v5). The JSON payload is self-describing, so these are
// belt-and-suspenders: they let a consumer recover sequence/producer/CRC even
// if it never decodes the body, and they exercise the User-Property <-> Tag
// round-trip the connector contract specifies. We stay well under the 32-prop /
// 4096-byte cap (3 small props).
const (
	propProducerID = "burnin-producerid"
	propSequence   = "burnin-sequence"
	propCRC        = "burnin-crc"
)

// trackingUserProps builds the User Properties carried on an outbound publish.
// Only meaningful on v5 (v3.1.1 has no user properties); harmless to attach on
// v4 since the transport simply omits them when the connection is v3.1.1.
func trackingUserProps(producerID string, seq uint64, crcHex string) []transport.UserProperty {
	return []transport.UserProperty{
		{Key: propProducerID, Value: producerID},
		{Key: propSequence, Value: strconv.FormatUint(seq, 10)},
		{Key: propCRC, Value: crcHex},
	}
}

// userProp returns the first value for key among the message's User Properties.
func userProp(props []transport.UserProperty, key string) string {
	for _, p := range props {
		if p.Key == key {
			return p.Value
		}
	}
	return ""
}

// decodeInbound parses the tracking fields from a received message. It prefers
// the JSON body (authoritative, always present), falling back to User Properties
// only when the body cannot be decoded. Returns ok=false on a structurally
// malformed message (counted as a malformed-message error by the caller).
func decodeInbound(msg transport.IncomingMessage) (producerID string, seq uint64, crcHex string, body []byte, ok bool) {
	body = msg.Payload

	if m, err := payload.Decode(body); err == nil && m.ProducerID != "" {
		producerID = m.ProducerID
		seq = m.Sequence
		crcHex = userProp(msg.UserProps, propCRC)
		return producerID, seq, crcHex, body, true
	}

	// Body did not decode — try recovering from User Properties (v5 tags).
	producerID = userProp(msg.UserProps, propProducerID)
	seqStr := userProp(msg.UserProps, propSequence)
	crcHex = userProp(msg.UserProps, propCRC)
	if producerID == "" || seqStr == "" {
		return "", 0, "", body, false
	}
	s, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return "", 0, "", body, false
	}
	return producerID, s, crcHex, body, true
}

// recordReceipt applies the shared receive-side bookkeeping for a delivered
// message: CRC verification, dup/out-of-order tracking, latency capture, and the
// received/bytes counters + Prometheus metrics. It is safe for concurrent use
// across consumer goroutines (all underlying state is mutex/atomic-guarded).
//
// crcHex may be empty (e.g. when only the body was available and no CRC tag was
// carried); in that case the CRC check is skipped — the JSON body is still
// validated by Decode upstream. Returns true when the message was accepted
// (not corrupted).
func recordReceipt(b *BaseWorker, stat *WorkerStat, producerID string, seq uint64, crcHex string, body []byte) bool {
	if crcHex != "" && !payload.VerifyCRC(body, crcHex) {
		b.IncCorrupted()
		metrics.IncCorrupted(b.Pattern())
		return false
	}

	isDup, isOOO := b.Tracker().Record(producerID, seq)
	if isDup {
		b.IncDuplicated()
		metrics.IncDuplicated(b.Pattern())
	}
	if isOOO {
		metrics.IncOutOfOrder(b.Pattern())
	}

	if sendTime, found := b.TSStore().LoadAndDelete(producerID, seq); found {
		latency := time.Since(sendTime)
		b.LatencyAccumulator().Record(latency)
		metrics.ObserveLatency(b.Pattern(), latency)
	}

	b.IncReceived()
	if stat != nil {
		stat.Recv.Add(1)
	}
	b.AddBytesReceived(uint64(len(body)))
	metrics.RecordBytesReceived(b.Pattern(), len(body))
	return true
}

// consumerTransports is a small helper embedded by pattern workers to track the
// transports that back their consumer/subscription connections (separate from
// producer connections), so DisconnectConsumers can force-drop ONLY the consumer
// side — exactly what the forced-disconnect injector wants to exercise.
type consumerTransports struct {
	mu sync.Mutex
	ts []transport.Transport
}

func (c *consumerTransports) add(t transport.Transport) {
	c.mu.Lock()
	c.ts = append(c.ts, t)
	c.mu.Unlock()
}

func (c *consumerTransports) disconnectAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.ts {
		t.ForceDisconnect()
	}
}

func (c *consumerTransports) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ts)
}
