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

// EventsWorker drives one Events channel: producers PUBLISH to events/<ch>;
// consumers SUBSCRIBE to events/<ch>. Fire-and-forget at the configured QoS;
// Events is the only pattern where wildcard subscriptions are permitted, but the
// burn-in uses an exact filter (one channel per worker).
type EventsWorker struct {
	*BaseWorker
	consumers consumerTransports
}

func NewEventsWorker(cfg *config.Config, channelIndex int, factory TransportFactory, logger *slog.Logger) *EventsWorker {
	channel := fmt.Sprintf("burnin.events.%d", channelIndex)
	return &EventsWorker{
		BaseWorker: NewBaseWorker(PatternEvents, channel, channelIndex, cfg, factory, logger),
	}
}

func (w *EventsWorker) Start(ctx context.Context) error {
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

func (w *EventsWorker) StartProducers() {
	ctx := w.NewProducerContext(context.Background())
	n := w.PatternConfig().ProducersPerChannel
	for i := 0; i < n; i++ {
		producerID := fmt.Sprintf("p-%s-%04d-%03d", w.Pattern(), w.ChannelIndex(), i)
		stat := w.AddProducerStat(producerID)
		w.ProducerWG().Add(1)
		go w.runProducer(ctx, producerID, stat)
	}
}

func (w *EventsWorker) DisconnectConsumers() { w.consumers.disconnectAll() }

// startConsumer opens one consumer connection and subscribes to events/<ch>.
func (w *EventsWorker) startConsumer(ctx context.Context, consumerID string, stat *WorkerStat) error {
	t := w.Factory()(consumerID, w.ConnectOptions(consumerID))
	w.RegisterTransport(t)
	w.consumers.add(t)

	if err := t.Connect(ctx); err != nil {
		return fmt.Errorf("events consumer connect %q: %w", consumerID, err)
	}

	qos := byte(w.Config().MQTT.Qos)
	err := t.Subscribe(ctx, transport.SubscribeOptions{
		Filter:  transport.EventsSubscribeFilter(w.ChannelName()),
		QoS:     qos,
		Handler: w.handleMessage(stat),
	})
	if err != nil {
		metrics.IncError(w.Pattern(), "subscribe_failure")
		return fmt.Errorf("events subscribe %q: %w", w.ChannelName(), err)
	}
	metrics.SetActiveConnections(w.Pattern(), float64(w.consumers.count()))
	return nil
}

func (w *EventsWorker) handleMessage(stat *WorkerStat) transport.MessageHandler {
	return func(msg transport.IncomingMessage) {
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

func (w *EventsWorker) runProducer(ctx context.Context, producerID string, stat *WorkerStat) {
	defer w.ProducerWG().Done()

	t := w.Factory()(producerID, w.ConnectOptions(producerID))
	w.RegisterTransport(t)
	if err := t.Connect(ctx); err != nil {
		w.Logger().Error("events producer connect failed", "producer", producerID, "error", err)
		return
	}
	defer func() { _ = t.Disconnect(context.Background()) }()

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
