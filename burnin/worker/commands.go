package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-mqtt/burnin/config"
	"github.com/kubemq-io/kubemq-mqtt/burnin/metrics"
	"github.com/kubemq-io/kubemq-mqtt/burnin/payload"
	"github.com/kubemq-io/kubemq-mqtt/burnin/transport"
)

// rpcSender bundles one sender's connection with the $reply subscription
// established on it. Created in Start (so the $reply subscribe precedes any
// publish, per the contract) and driven by StartProducers.
type rpcSender struct {
	id       string
	clientID string
	t        transport.Transport
	stat     *WorkerStat
}

// CommandsWorker SENDS commands over MQTT (v5 RPC only). Each sender first
// SUBSCRIBES to its OWN $reply/<clientID>/cmd, then PUBLISHES to commands/<ch>
// with Properties.ResponseTopic = that $reply topic + a unique CorrelationData.
// PUBACK is immediate; the sender then waits up to rpc.timeout_ms for the
// response delivered on its $reply topic, correlated by CorrelationData. A
// command response carries NO body — only the kubemq-executed user-prop (+
// optional kubemq-error); CorrelationData is echoed.
//
// The RESPONDER side is the embedded in-process gRPC responder
// (responder/grpc_responder.go) subscribed to the same commands channel — MQTT
// clients cannot be RPC responders. The engine registers this worker's channel
// with that responder before producers start.
type CommandsWorker struct {
	*BaseWorker
	pending *rpcPending
	senders []*rpcSender
}

func NewCommandsWorker(cfg *config.Config, channelIndex int, factory TransportFactory, logger *slog.Logger) *CommandsWorker {
	channel := fmt.Sprintf("burnin.commands.%d", channelIndex)
	return &CommandsWorker{
		BaseWorker: NewBaseWorker(PatternCommands, channel, channelIndex, cfg, factory, logger),
		pending:    newRPCPending(),
	}
}

// Start brings up each sender's connection and its OWN $reply subscription. No
// commands are published yet (that is StartProducers); subscribing here
// guarantees the $reply subscription exists before the first publish.
func (w *CommandsWorker) Start(ctx context.Context) error {
	ctx = w.NewConsumerContext(ctx)

	n := w.PatternConfig().SendersPerChannel
	for i := 0; i < n; i++ {
		senderID := fmt.Sprintf("s-%s-%04d-%03d", w.Pattern(), w.ChannelIndex(), i)
		clientID := w.Config().Broker.ClientIDPrefix + "-" + senderID
		stat := w.AddProducerStat(senderID)

		t := w.Factory()(clientID, w.ConnectOptions(clientID))
		w.RegisterTransport(t)
		if err := t.Connect(ctx); err != nil {
			return fmt.Errorf("commands sender connect %q: %w", clientID, err)
		}

		// Subscribe to the sender's OWN $reply/<clientID>/cmd namespace BEFORE
		// publishing. A $reply outside the client's own namespace -> PUBACK 0x83.
		replyFilter := transport.ReplyTopic(clientID, "cmd")
		err := t.Subscribe(ctx, transport.SubscribeOptions{
			Filter:  replyFilter,
			QoS:     byte(w.Config().MQTT.Qos),
			Handler: w.handleReply,
		})
		if err != nil {
			metrics.IncError(w.Pattern(), "subscribe_failure")
			return fmt.Errorf("commands $reply subscribe %q: %w", replyFilter, err)
		}

		w.senders = append(w.senders, &rpcSender{id: senderID, clientID: clientID, t: t, stat: stat})
	}

	metrics.SetActiveConnections(w.Pattern(), float64(len(w.senders)))
	w.SignalConsumerReady()
	return nil
}

func (w *CommandsWorker) StartProducers() {
	ctx := w.NewProducerContext(context.Background())
	for _, s := range w.senders {
		w.ProducerWG().Add(1)
		go w.runSender(ctx, s)
	}
}

// handleReply routes a $reply delivery to the waiting sender by CorrelationData.
func (w *CommandsWorker) handleReply(msg transport.IncomingMessage) {
	w.pending.deliver(msg)
}

func (w *CommandsWorker) runSender(ctx context.Context, s *rpcSender) {
	defer w.ProducerWG().Done()

	topic := transport.RPCPublishTopic(w.Pattern(), w.ChannelName())
	replyTopic := transport.ReplyTopic(s.clientID, "cmd")
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
		body, _ := payload.Encode(metrics.SDK(), w.Pattern(), s.id, seq, size)

		corr, key := w.pending.nextCorrelation()
		replyCh := w.pending.register(key)

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

		// PUBACK is immediate; wait for the executed reply on our own $reply
		// topic, bounded by rpc.timeout_ms.
		select {
		case <-ctx.Done():
			w.pending.deregister(key)
			return
		case reply := <-replyCh:
			elapsed := time.Since(t0)
			w.handleCommandReply(s, seq, reply, elapsed)
		case <-time.After(timeout):
			w.pending.deregister(key)
			w.IncRPCTimeout()
			metrics.IncError(w.Pattern(), "rpc_timeout")
			metrics.IncRPCResponse(w.Pattern(), "timeout")
		}
	}
}

// handleCommandReply validates a command response: NO body, kubemq-executed
// user-prop present and "true". Records success or an executed=false error.
func (w *CommandsWorker) handleCommandReply(s *rpcSender, seq uint64, reply transport.IncomingMessage, elapsed time.Duration) {
	executed := userProp(reply.UserProps, propKubeMQExecuted)
	if executed != "true" {
		// executed=false (or missing) — the responder reported a failed command.
		// kubemq-error, when present, carries the reason.
		w.IncRPCError()
		metrics.IncError(w.Pattern(), "rpc_error")
		metrics.IncRPCResponse(w.Pattern(), "error")
		return
	}

	w.IncRPCSuccess()
	metrics.IncRPCResponse(w.Pattern(), "success")
	metrics.ObserveRPCDuration(w.Pattern(), elapsed)
	w.RPCLatencyAccumulator().Record(elapsed)

	w.IncSent()
	s.stat.Sent.Add(1)
	metrics.IncSent(w.Pattern(), s.id)
	w.RateWindow().Record()
	w.PeakRate().Record()
	w.Tracker().Record(s.id, seq)
}

// DisconnectConsumers force-drops the sender connections (which also carry the
// $reply subscriptions) so autopaho reconnects — exercises RPC recovery.
func (w *CommandsWorker) DisconnectConsumers() {
	for _, s := range w.senders {
		s.t.ForceDisconnect()
	}
}
