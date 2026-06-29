package transport

import (
	"context"
	"time"
)

// UserProperty is one MQTT 5.0 User Property (maps to/from a KubeMQ message Tag
// for events/store/queues on v5 only). The connector caps these at 32 props /
// 4096 bytes total; exceeding -> publish rejected (v5 PUBACK 0x97).
type UserProperty struct {
	Key   string
	Value string
}

// PublishOptions carries one outbound MQTT publish.
//
// Topic is the full publish topic from the topic.go builders. ResponseTopic and
// CorrelationData are v5 RPC fields ($reply round-trip): set ResponseTopic to the
// client's OWN $reply/<clientID>/<suffix> namespace and a unique CorrelationData
// per request. UserProps map to KubeMQ tags on v5 for events/store/queues.
//
// Retain MUST stay false: the connector forces RetainAvailable=0 and a runtime
// retained publish is silently stripped (PUBACK 0x00, message dropped).
type PublishOptions struct {
	Topic           string
	QoS             byte
	Retain          bool
	Payload         []byte
	ResponseTopic   string
	CorrelationData []byte
	UserProps       []UserProperty
}

// IncomingMessage is a received MQTT publish, normalized away from the paho
// packet type so workers/responders compile against the interface only.
type IncomingMessage struct {
	Topic           string
	Payload         []byte
	QoS             byte
	Retain          bool
	ResponseTopic   string // v5 only
	CorrelationData []byte // v5 only
	UserProps       []UserProperty
}

// MessageHandler is invoked for each received message on a subscription.
type MessageHandler func(msg IncomingMessage)

// SubscribeOptions describes a single subscription.
//
// For Events/Store use the events/store filters (wildcards '+'/'#' allowed on
// Events only). For Queues consume use SharedQueueFilter ($share/<group>/queues/<ch>)
// at QoS >= 1. For RPC, subscribe to the client's own $reply/<clientID>/<suffix>
// BEFORE publishing the request.
type SubscribeOptions struct {
	Filter  string
	QoS     byte
	Handler MessageHandler
}

// ConnectOptions configures a single MQTT connection (one Transport instance).
type ConnectOptions struct {
	// BrokerAddress is host:port (e.g. localhost:1883); a tcp:// scheme is
	// applied by the implementation when absent.
	BrokerAddress string
	ClientID      string
	// ProtocolVersion is 4 (MQTT 3.1.1) or 5 (MQTT 5.0). RPC and User
	// Properties require 5.
	ProtocolVersion int
	CleanStart      bool
	KeepAlive       time.Duration
	// Reconnect parameters drive autopaho's exponential backoff.
	ReconnectInterval    time.Duration
	ReconnectMaxInterval time.Duration

	// OnConnect / OnDisconnect fire on (re)connection state changes so workers
	// can track downtime and reconnection counts.
	OnConnect    func()
	OnDisconnect func(err error)
}

// Transport is the MQTT publish/subscribe abstraction every leaf package
// compiles against. The concrete implementation (PahoTransport) wraps
// eclipse/paho.golang's autopaho ConnectionManager; tests may substitute a fake.
//
// Connect establishes (and thereafter maintains, via autopaho) the connection.
// Subscriptions registered before Connect are (re)applied on every connection so
// clean_start=false node-local session restore and forced-disconnect recovery
// both work. Publish blocks until the PUBACK/PUBREC handshake for the configured
// QoS completes (QoS>0); the caller treats a returned error as a send failure.
type Transport interface {
	// Connect dials the broker and blocks until the first successful CONNACK
	// (or ctx/err). The connection is then maintained until Disconnect.
	Connect(ctx context.Context) error

	// Subscribe registers a subscription. Calling before Connect queues it for
	// (re)application on connect; calling after Connect applies it immediately.
	Subscribe(ctx context.Context, opts SubscribeOptions) error

	// Unsubscribe removes a subscription filter.
	Unsubscribe(ctx context.Context, filter string) error

	// Publish sends one message, blocking for the QoS acknowledgement.
	Publish(ctx context.Context, opts PublishOptions) error

	// Disconnect closes the connection cleanly (sends DISCONNECT).
	Disconnect(ctx context.Context) error

	// ForceDisconnect drops the underlying TCP connection WITHOUT a clean
	// DISCONNECT, so autopaho reconnects — used by the forced-disconnect
	// injector to exercise recovery. Unacked QoS>0 messages are requeued.
	ForceDisconnect()

	// ClientID returns the connection's MQTT client identifier (needed to build
	// the client's own $reply/<clientID>/... RPC response namespace).
	ClientID() string

	// IsConnected reports the current connection state.
	IsConnected() bool
}
