package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kubemq-io/kubemq-mqtt/burnin/config"
	"github.com/kubemq-io/kubemq-mqtt/burnin/metrics"
	"github.com/kubemq-io/kubemq-mqtt/burnin/payload"
	"github.com/kubemq-io/kubemq-mqtt/burnin/transport"
)

// QueriesWorker SENDS queries over MQTT (v5 RPC only). Each sender first
// SUBSCRIBES to its OWN $reply/<clientID>/qry, then PUBLISHES to queries/<ch>
// with Properties.ResponseTopic = that $reply topic + a unique CorrelationData.
// PUBACK is immediate; the sender waits up to rpc.timeout_ms for the response.
//
// A query response = body + user-props (kubemq-metadata + tags); CorrelationData
// is echoed. The embedded gRPC responder echoes the request body verbatim, so we
// verify the echoed body's CRC32 to prove round-trip fidelity.
//
// The RESPONDER side is the embedded in-process gRPC responder
// (responder/grpc_responder.go) subscribed to the same queries channel.
type QueriesWorker struct {
	*BaseWorker
	pending *rpcPending
	senders []*rpcSender

	// expected maps an in-flight CorrelationData key to the CRC32 hex of the
	// query body we sent, so the reply handler can verify the echoed body.
	expMu    sync.Mutex
	expected map[string]string
}

func NewQueriesWorker(cfg *config.Config, channelIndex int, factory TransportFactory, logger *slog.Logger) *QueriesWorker {
	channel := fmt.Sprintf("burnin.queries.%d", channelIndex)
	return &QueriesWorker{
		BaseWorker: NewBaseWorker(PatternQueries, channel, channelIndex, cfg, factory, logger),
		pending:    newRPCPending(),
		expected:   make(map[string]string),
	}
}

func (w *QueriesWorker) Start(ctx context.Context) error {
	ctx = w.NewConsumerContext(ctx)

	n := w.PatternConfig().SendersPerChannel
	for i := 0; i < n; i++ {
		senderID := fmt.Sprintf("s-%s-%04d-%03d", w.Pattern(), w.ChannelIndex(), i)
		clientID := w.Config().Broker.ClientIDPrefix + "-" + senderID
		stat := w.AddProducerStat(senderID)

		t := w.Factory()(clientID, w.ConnectOptions(clientID))
		w.RegisterTransport(t)
		if err := t.Connect(ctx); err != nil {
			return fmt.Errorf("queries sender connect %q: %w", clientID, err)
		}

		replyFilter := transport.ReplyTopic(clientID, "qry")
		err := t.Subscribe(ctx, transport.SubscribeOptions{
			Filter:  replyFilter,
			QoS:     byte(w.Config().MQTT.Qos),
			Handler: w.handleReply,
		})
		if err != nil {
			metrics.IncError(w.Pattern(), "subscribe_failure")
			return fmt.Errorf("queries $reply subscribe %q: %w", replyFilter, err)
		}

		w.senders = append(w.senders, &rpcSender{id: senderID, clientID: clientID, t: t, stat: stat})
	}

	metrics.SetActiveConnections(w.Pattern(), float64(len(w.senders)))
	w.SignalConsumerReady()
	return nil
}

func (w *QueriesWorker) StartProducers() {
	ctx := w.NewProducerContext(context.Background())
	for _, s := range w.senders {
		w.ProducerWG().Add(1)
		go w.runSender(ctx, s)
	}
}

func (w *QueriesWorker) handleReply(msg transport.IncomingMessage) {
	w.pending.deliver(msg)
}

func (w *QueriesWorker) runSender(ctx context.Context, s *rpcSender) {
	defer w.ProducerWG().Done()

	topic := transport.RPCPublishTopic(w.Pattern(), w.ChannelName())
	replyTopic := transport.ReplyTopic(s.clientID, "qry")
	qos := byte(w.Config().MQTT.Qos)
	timeout := time.Duration(w.Config().RPC.TimeoutMs) * time.Millisecond

	var seq uint64
	for {
		if err := w.WaitForRate(ctx); err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		seq++

		size := w.SelectMessageSize()
		body, crcHex := payload.Encode(metrics.SDK(), w.Pattern(), s.id, seq, size)

		corr, key := w.pending.nextCorrelation()
		replyCh := w.pending.register(key)
		w.setExpected(key, crcHex)

		t0 := time.Now()
		err := s.t.Publish(ctx, transport.PublishOptions{
			Topic:           topic,
			QoS:             qos,
			Payload:         body,
			ResponseTopic:   replyTopic,
			CorrelationData: corr,
		})
		if err != nil {
			w.pending.deregister(key)
			w.clearExpected(key)
			if ctx.Err() != nil {
				return
			}
			w.IncErrors()
			w.IncRPCError()
			metrics.IncError(w.Pattern(), "rpc_error")
			metrics.IncRPCResponse(w.Pattern(), "error")
			continue
		}
		metrics.IncPubackReason(w.Pattern(), "0x00")
		w.AddBytesSent(uint64(len(body)))
		metrics.RecordBytesSent(w.Pattern(), len(body))

		select {
		case <-ctx.Done():
			w.pending.deregister(key)
			w.clearExpected(key)
			return
		case reply := <-replyCh:
			elapsed := time.Since(t0)
			w.handleQueryReply(s, seq, key, reply, elapsed)
		case <-time.After(timeout):
			w.pending.deregister(key)
			w.clearExpected(key)
			w.IncRPCTimeout()
			metrics.IncError(w.Pattern(), "rpc_timeout")
			metrics.IncRPCResponse(w.Pattern(), "timeout")
		}
	}
}

// handleQueryReply validates a query response: body present and its CRC matches
// the echoed request body. Records success, corruption, or error.
func (w *QueriesWorker) handleQueryReply(s *rpcSender, seq uint64, key string, reply transport.IncomingMessage, elapsed time.Duration) {
	expCRC := w.takeExpected(key)

	if len(reply.Payload) == 0 {
		w.IncRPCError()
		metrics.IncError(w.Pattern(), "rpc_error")
		metrics.IncRPCResponse(w.Pattern(), "error")
		return
	}

	if expCRC != "" && !payload.VerifyCRC(reply.Payload, expCRC) {
		w.IncCorrupted()
		metrics.IncCorrupted(w.Pattern())
		metrics.IncRPCResponse(w.Pattern(), "error")
		w.IncRPCError()
		return
	}

	w.IncRPCSuccess()
	metrics.IncRPCResponse(w.Pattern(), "success")
	metrics.ObserveRPCDuration(w.Pattern(), elapsed)
	w.RPCLatencyAccumulator().Record(elapsed)

	w.IncSent()
	s.stat.Sent.Add(1)
	w.AddBytesReceived(uint64(len(reply.Payload)))
	metrics.IncSent(w.Pattern(), s.id)
	metrics.RecordBytesReceived(w.Pattern(), len(reply.Payload))
	w.RateWindow().Record()
	w.PeakRate().Record()
	w.Tracker().Record(s.id, seq)
}

func (w *QueriesWorker) DisconnectConsumers() {
	for _, s := range w.senders {
		s.t.ForceDisconnect()
	}
}

// --- expected-CRC bookkeeping ---

func (w *QueriesWorker) setExpected(key, crc string) {
	w.expMu.Lock()
	w.expected[key] = crc
	w.expMu.Unlock()
}

func (w *QueriesWorker) takeExpected(key string) string {
	w.expMu.Lock()
	crc := w.expected[key]
	delete(w.expected, key)
	w.expMu.Unlock()
	return crc
}

func (w *QueriesWorker) clearExpected(key string) {
	w.expMu.Lock()
	delete(w.expected, key)
	w.expMu.Unlock()
}
