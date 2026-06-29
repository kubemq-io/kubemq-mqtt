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

// EventsStoreWorker drives one Events-Store channel: producers PUBLISH to
// store/<ch>; consumers SUBSCRIBE to store/<ch>.
//
// Over MQTT, Events-Store is ALWAYS StartNewOnly — there is NO historical
// replay. The consumer therefore only ever sees messages published AFTER its
// subscription is established; the engine's "wait for ConsumerReady before
// starting producers" sequencing guarantees no published message is missed for
// reasons of subscription ordering. There is no offset/replay knob to set on the
// MQTT side — StartNewOnly is forced by the connector.
type EventsStoreWorker struct {
	*BaseWorker
	consumers consumerTransports
}

func NewEventsStoreWorker(cfg *config.Config, channelIndex int, factory TransportFactory, logger *slog.Logger) *EventsStoreWorker {
	channel := fmt.Sprintf("burnin.store.%d", channelIndex)
	return &EventsStoreWorker{
		BaseWorker: NewBaseWorker(PatternEventsStore, channel, channelIndex, cfg, factory, logger),
	}
}

func (w *EventsStoreWorker) Start(ctx context.Context) error {
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

func (w *EventsStoreWorker) StartProducers() {
	ctx := w.NewProducerContext(context.Background())
	n := w.PatternConfig().ProducersPerChannel
	for i := 0; i < n; i++ {
		producerID := fmt.Sprintf("p-%s-%04d-%03d", w.Pattern(), w.ChannelIndex(), i)
		stat := w.AddProducerStat(producerID)
		w.ProducerWG().Add(1)
		go w.runProducer(ctx, producerID, stat)
	}
}

func (w *EventsStoreWorker) DisconnectConsumers() { w.consumers.disconnectAll() }

func (w *EventsStoreWorker) startConsumer(ctx context.Context, consumerID string, stat *WorkerStat) error {
	t := w.Factory()(consumerID, w.ConnectOptions(consumerID))
	w.RegisterTransport(t)
	w.consumers.add(t)

	if err := t.Connect(ctx); err != nil {
		return fmt.Errorf("events_store consumer connect %q: %w", consumerID, err)
	}

	qos := byte(w.Config().MQTT.Qos)
	err := t.Subscribe(ctx, transport.SubscribeOptions{
		Filter:  transport.StoreSubscribeFilter(w.ChannelName()),
		QoS:     qos,
		Handler: w.handleMessage(stat),
	})
	if err != nil {
		metrics.IncError(w.Pattern(), "subscribe_failure")
		return fmt.Errorf("events_store subscribe %q: %w", w.ChannelName(), err)
	}
	metrics.SetActiveConnections(w.Pattern(), float64(w.consumers.count()))
	return nil
}

func (w *EventsStoreWorker) handleMessage(stat *WorkerStat) transport.MessageHandler {
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

func (w *EventsStoreWorker) runProducer(ctx context.Context, producerID string, stat *WorkerStat) {
	defer w.ProducerWG().Done()

	t := w.Factory()(producerID, w.ConnectOptions(producerID))
	w.RegisterTransport(t)
	if err := t.Connect(ctx); err != nil {
		w.Logger().Error("events_store producer connect failed", "producer", producerID, "error", err)
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
