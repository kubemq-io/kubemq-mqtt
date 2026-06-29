package transport

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

// PahoTransport is the eclipse/paho.golang (autopaho) implementation of
// Transport. It targets MQTT 5.0 (paho.golang is v5-only); ProtocolVersion 4
// callers must use a v3.1.1-capable client — the connector accepts both, but
// this transport speaks v5. RPC and User Properties require v5 regardless.
//
// autopaho maintains the connection with built-in exponential-backoff
// reconnection; registered subscriptions are re-applied on every OnConnectionUp.
type PahoTransport struct {
	clientID string
	opts     ConnectOptions
	logger   *slog.Logger

	cm *autopaho.ConnectionManager

	mu   sync.Mutex
	subs []SubscribeOptions // re-applied on each (re)connect
}

// NewPahoTransport constructs a PahoTransport for one MQTT connection. It does
// not dial until Connect is called.
func NewPahoTransport(clientID string, opts ConnectOptions, logger *slog.Logger) *PahoTransport {
	return &PahoTransport{
		clientID: clientID,
		opts:     opts,
		logger:   logger.With("component", "mqtt-transport", "client_id", clientID),
	}
}

// NewPahoFactory returns a worker.TransportFactory-compatible factory that
// produces PahoTransport instances. The engine passes this to workers so they
// never import paho directly.
func NewPahoFactory(logger *slog.Logger) func(clientID string, opts ConnectOptions) Transport {
	return func(clientID string, opts ConnectOptions) Transport {
		return NewPahoTransport(clientID, opts, logger)
	}
}

func (t *PahoTransport) Connect(ctx context.Context) error {
	serverURL, err := brokerURL(t.opts.BrokerAddress)
	if err != nil {
		return fmt.Errorf("parse broker address %q: %w", t.opts.BrokerAddress, err)
	}

	keepalive := uint16(t.opts.KeepAlive / time.Second)
	if keepalive == 0 {
		keepalive = 30
	}

	cfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{serverURL},
		KeepAlive:                     keepalive,
		CleanStartOnInitialConnection: t.opts.CleanStart,
		ReconnectBackoff:              t.backoff(),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			if t.opts.OnConnect != nil {
				t.opts.OnConnect()
			}
			t.resubscribe(ctx, cm)
		},
		OnConnectionDown: func() bool {
			if t.opts.OnDisconnect != nil {
				t.opts.OnDisconnect(nil)
			}
			return true // keep reconnecting
		},
		OnConnectError: func(err error) {
			t.logger.Debug("connect attempt failed", "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: t.clientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				t.dispatch,
			},
		},
	}

	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new connection: %w", err)
	}
	t.cm = cm

	return cm.AwaitConnection(ctx)
}

func (t *PahoTransport) Subscribe(ctx context.Context, opts SubscribeOptions) error {
	t.mu.Lock()
	t.subs = append(t.subs, opts)
	cm := t.cm
	t.mu.Unlock()

	if cm == nil {
		return nil // queued; applied on connect
	}
	_, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: opts.Filter, QoS: opts.QoS}},
	})
	return err
}

func (t *PahoTransport) Unsubscribe(ctx context.Context, filter string) error {
	t.mu.Lock()
	for i, s := range t.subs {
		if s.Filter == filter {
			t.subs = append(t.subs[:i], t.subs[i+1:]...)
			break
		}
	}
	cm := t.cm
	t.mu.Unlock()

	if cm == nil {
		return nil
	}
	_, err := cm.Unsubscribe(ctx, &paho.Unsubscribe{Topics: []string{filter}})
	return err
}

func (t *PahoTransport) Publish(ctx context.Context, opts PublishOptions) error {
	if t.cm == nil {
		return fmt.Errorf("publish before connect")
	}
	pub := &paho.Publish{
		Topic:   opts.Topic,
		QoS:     opts.QoS,
		Retain:  opts.Retain,
		Payload: opts.Payload,
	}
	if opts.ResponseTopic != "" || len(opts.CorrelationData) > 0 || len(opts.UserProps) > 0 {
		props := &paho.PublishProperties{
			ResponseTopic:   opts.ResponseTopic,
			CorrelationData: opts.CorrelationData,
		}
		for _, up := range opts.UserProps {
			props.User = append(props.User, paho.UserProperty{Key: up.Key, Value: up.Value})
		}
		pub.Properties = props
	}
	_, err := t.cm.Publish(ctx, pub)
	return err
}

func (t *PahoTransport) Disconnect(ctx context.Context) error {
	if t.cm == nil {
		return nil
	}
	return t.cm.Disconnect(ctx)
}

// ForceDisconnect drops the underlying connection without a clean DISCONNECT so
// autopaho reconnects. Uses TerminateConnectionForTest, the only public hook to
// sever the socket from outside the manager.
func (t *PahoTransport) ForceDisconnect() {
	if t.cm == nil {
		return
	}
	t.cm.TerminateConnectionForTest()
}

func (t *PahoTransport) ClientID() string { return t.clientID }

func (t *PahoTransport) IsConnected() bool {
	if t.cm == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	return t.cm.AwaitConnection(ctx) == nil
}

// --- internals ---

func (t *PahoTransport) resubscribe(ctx context.Context, cm *autopaho.ConnectionManager) {
	t.mu.Lock()
	subs := append([]SubscribeOptions(nil), t.subs...)
	t.mu.Unlock()

	for _, s := range subs {
		if _, err := cm.Subscribe(ctx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: s.Filter, QoS: s.QoS}},
		}); err != nil {
			t.logger.Warn("resubscribe failed", "filter", s.Filter, "error", err)
		}
	}
}

// dispatch routes an inbound PUBLISH to the handler of the first subscription
// whose filter matches the message topic (MQTT topic-filter wildcard rules).
func (t *PahoTransport) dispatch(pr paho.PublishReceived) (bool, error) {
	t.mu.Lock()
	subs := append([]SubscribeOptions(nil), t.subs...)
	t.mu.Unlock()

	msg := IncomingMessage{
		Topic:   pr.Packet.Topic,
		Payload: pr.Packet.Payload,
		QoS:     pr.Packet.QoS,
		Retain:  pr.Packet.Retain,
	}
	if pr.Packet.Properties != nil {
		msg.ResponseTopic = pr.Packet.Properties.ResponseTopic
		msg.CorrelationData = pr.Packet.Properties.CorrelationData
		for _, up := range pr.Packet.Properties.User {
			msg.UserProps = append(msg.UserProps, UserProperty{Key: up.Key, Value: up.Value})
		}
	}

	handled := false
	for _, s := range subs {
		if s.Handler != nil && topicMatches(s.Filter, pr.Packet.Topic) {
			s.Handler(msg)
			handled = true
		}
	}
	return handled, nil
}

func (t *PahoTransport) backoff() func(int) time.Duration {
	base := t.opts.ReconnectInterval
	if base <= 0 {
		base = time.Second
	}
	max := t.opts.ReconnectMaxInterval
	if max <= 0 {
		max = 30 * time.Second
	}
	return func(attempt int) time.Duration {
		d := base << uint(attempt)
		if d <= 0 || d > max {
			return max
		}
		return d
	}
}

// brokerURL normalizes a host:port (or scheme://host:port) broker address into a
// *url.URL with a tcp scheme when none is given.
func brokerURL(addr string) (*url.URL, error) {
	if !strings.Contains(addr, "://") {
		addr = "tcp://" + addr
	}
	return url.Parse(addr)
}

// topicMatches reports whether an MQTT topic filter (supporting '+' single-level
// and '#' multi-level, plus a $share/<group>/ prefix) matches a concrete topic.
func topicMatches(filter, topic string) bool {
	// Strip a leading $share/<group>/ so a shared subscription filter matches
	// the concrete delivery topic.
	if strings.HasPrefix(filter, "$share/") {
		parts := strings.SplitN(filter, "/", 3)
		if len(parts) == 3 {
			filter = parts[2]
		}
	}
	fSeg := strings.Split(filter, "/")
	tSeg := strings.Split(topic, "/")
	for i, f := range fSeg {
		if f == "#" {
			return true
		}
		if i >= len(tSeg) {
			return false
		}
		if f == "+" {
			continue
		}
		if f != tSeg[i] {
			return false
		}
	}
	return len(fSeg) == len(tSeg)
}
