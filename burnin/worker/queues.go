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

// QueuesWorker drives one Queues channel: producers PUBLISH to queues/<ch>;
// consumers CONSUME via an MQTT 5.0 shared subscription
// $share/<shared_group>/queues/<ch> at QoS >= 1.
//
// Wire-contract facts this worker honors:
//   - A plain queues/... subscribe (no $share) -> SUBACK 0x83; a QoS-0 shared
//     subscribe -> SUBACK 0x83. Config validation already forces QoS >= 1 and a
//     non-empty shared_group when queues is enabled, so we subscribe with
//     SharedQueueFilter at the configured (>=1) QoS.
//   - Ack-on-PUBACK: autopaho sends the PUBACK once OnPublishReceived returns
//     without error, so simply handling the message acks it (the broker then
//     removes it from the queue pool). No PUBACK within QueueAckTimeoutSeconds ->
//     redeliver; a disconnect with unacked messages -> immediate requeue.
//   - The $share group name is audit/metrics-only: ALL groups compete in ONE
//     shared KubeMQ queue pool, so each published message is delivered to exactly
//     one consumer across the whole pool (no per-group fan-out copies).
type QueuesWorker struct {
	*BaseWorker
	consumers consumerTransports
}

func NewQueuesWorker(cfg *config.Config, channelIndex int, factory TransportFactory, logger *slog.Logger) *QueuesWorker {
	// NOTE: the channel must be a single MQTT segment with NO literal '.'. The
	// connector's queue bridge re-publishes consumed messages on a topic it
	// RECONSTRUCTS via ToTopic (KubeMQ channel '.' -> MQTT '/'), unlike the
	// events bridge which reuses the original subscription filter string. A
	// dotted name like "burnin.queues.1" is delivered on "queues/burnin/queues/1"
	// which never matches the client's "$share/<g>/queues/burnin.queues.1"
	// shared-subscription filter -> 0 received. A hyphenated, dot-free name
	// round-trips through ToTopic as an identity, so delivery matches the filter.
	channel := fmt.Sprintf("burnin-queues-%d", channelIndex)
	return &QueuesWorker{
		BaseWorker: NewBaseWorker(PatternQueues, channel, channelIndex, cfg, factory, logger),
	}
}

func (w *QueuesWorker) Start(ctx context.Context) error {
	ctx = w.NewConsumerContext(ctx)

	n := w.PatternConfig().ConsumersPerChannel
	for i := 0; i < n; i++ {
		consumerID := fmt.Sprintf("c-%s-%04d-%03d", w.Pattern(), w.ChannelIndex(), i)
		stat := w.AddConsumerStat(consumerID)
		if err := w.startConsumer(ctx, consumerID, stat); err != nil {
			return err
		}
	}

	w.SignalConsumerReady()
	return nil
}

func (w *QueuesWorker) StartProducers() {
	ctx := w.NewProducerContext(context.Background())
	n := w.PatternConfig().ProducersPerChannel
	for i := 0; i < n; i++ {
		producerID := fmt.Sprintf("p-%s-%04d-%03d", w.Pattern(), w.ChannelIndex(), i)
		stat := w.AddProducerStat(producerID)
		w.ProducerWG().Add(1)
		go w.runProducer(ctx, producerID, stat)
	}
}

func (w *QueuesWorker) DisconnectConsumers() { w.consumers.disconnectAll() }

func (w *QueuesWorker) startConsumer(ctx context.Context, consumerID string, stat *WorkerStat) error {
	t := w.Factory()(consumerID, w.ConnectOptions(consumerID))
	w.RegisterTransport(t)
	w.consumers.add(t)

	if err := t.Connect(ctx); err != nil {
		return fmt.Errorf("queues consumer connect %q: %w", consumerID, err)
	}

	// Shared subscription at QoS >= 1 — required by the connector for queues
	// consume. config.Validate already rejects QoS 0 / empty shared_group.
	qos := byte(w.Config().MQTT.Qos)
	if qos < 1 {
		qos = 1
	}
	filter := transport.SharedQueueFilter(w.Config().MQTT.SharedGroup, w.ChannelName())
	err := t.Subscribe(ctx, transport.SubscribeOptions{
		Filter:  filter,
		QoS:     qos,
		Handler: w.handleMessage(stat),
	})
	if err != nil {
		metrics.IncError(w.Pattern(), "subscribe_failure")
		return fmt.Errorf("queues shared-subscribe %q: %w", filter, err)
	}
	metrics.SetActiveConnections(w.Pattern(), float64(w.consumers.count()))
	return nil
}

func (w *QueuesWorker) handleMessage(stat *WorkerStat) transport.MessageHandler {
	return func(msg transport.IncomingMessage) {
		// Returning from this handler is the ack: autopaho sends the PUBACK after
		// OnPublishReceived completes, which removes the message from the queue
		// pool. We always "consume" (ack) the message; corruption/duplication are
		// recorded as metrics but do not block the ack (the message is gone).
		producerID, seq, crcHex, body, ok := decodeInbound(msg)
		if !ok {
			metrics.IncError(w.Pattern(), "malformed_message")
			return
		}
		if recordReceipt(w.BaseWorker, stat, producerID, seq, crcHex, body) {
			metrics.IncReceived(w.Pattern(), stat.ID)
		}
	}
}

func (w *QueuesWorker) runProducer(ctx context.Context, producerID string, stat *WorkerStat) {
	defer w.ProducerWG().Done()

	t := w.Factory()(producerID, w.ConnectOptions(producerID))
	w.RegisterTransport(t)
	if err := t.Connect(ctx); err != nil {
		w.Logger().Error("queues producer connect failed", "producer", producerID, "error", err)
		return
	}
	defer func() { _ = t.Disconnect(context.Background()) }()

	// Produce to the plain queues/<ch> topic (the $share prefix is consume-only).
	topic := transport.PublishTopic(w.Pattern(), w.ChannelName())
	qos := byte(w.Config().MQTT.Qos)

	var seq uint64
	for {
		if err := w.WaitForRate(ctx); err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if w.BackpressureCheck() {
			continue
		}
		seq++

		size := w.SelectMessageSize()
		body, crcHex := payload.Encode(metrics.SDK(), w.Pattern(), producerID, seq, size)

		w.TSStore().Store(producerID, seq, time.Now())

		t0 := time.Now()
		err := t.Publish(ctx, transport.PublishOptions{
			Topic:     topic,
			QoS:       qos,
			Payload:   body,
			UserProps: trackingUserProps(producerID, seq, crcHex),
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.IncErrors()
			metrics.IncError(w.Pattern(), "send_failure")
			metrics.IncPubackReason(w.Pattern(), "error")
			w.TSStore().LoadAndDelete(producerID, seq)
			continue
		}
		metrics.ObserveSendDuration(w.Pattern(), time.Since(t0))
		metrics.IncPubackReason(w.Pattern(), "0x00")

		w.IncSent()
		stat.Sent.Add(1)
		w.AddBytesSent(uint64(len(body)))
		metrics.IncSent(w.Pattern(), producerID)
		metrics.RecordBytesSent(w.Pattern(), len(body))
		w.RateWindow().Record()
		w.PeakRate().Record()
	}
}
