// Package responder embeds an in-process KubeMQ gRPC client that answers the
// Commands and Queries the MQTT-side workers SEND.
//
// MQTT clients cannot be RPC responders against the KubeMQ MQTT connector (an
// RPC subscribe over MQTT is rejected with SUBACK 0x83 — responders must be
// gRPC-side). So the harness self-contains the RPC round-trip: the MQTT workers
// publish to commands/<ch> or queries/<ch> with a v5 $reply ResponseTopic +
// CorrelationData, and THIS responder subscribes to the SAME channels over the
// KubeMQ gRPC API (kubemq-go/v2) and echoes replies — mirroring integration
// test #10's startQueryResponder pattern, but via the PUBLIC SDK
// (SubscribeToCommands / SubscribeToQueries + SendCommandResponse /
// SendQueryResponse), NOT the server-internal array.* API.
package responder

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"

	"github.com/kubemq-io/kubemq-mqtt/burnin/config"
)

// Responder is the embedded gRPC RPC responder. The engine starts one Responder
// for the run, registers every enabled commands/queries channel, then Start
// connects to the gRPC API and subscribes. Stop tears the connection down.
//
// A Query reply echoes the request body plus user-props (metadata + tags); a
// Command reply carries NO body — only kubemq-executed + optional kubemq-error.
type Responder interface {
	// RegisterCommandChannel registers a Commands channel to respond on.
	RegisterCommandChannel(channel string)
	// RegisterQueryChannel registers a Queries channel to respond on.
	RegisterQueryChannel(channel string)
	// Start dials grpc.responder_address and subscribes to all registered
	// channels. Returns once subscriptions are established (or on error).
	Start(ctx context.Context) error
	// Stop unsubscribes and closes the gRPC client.
	Stop() error
	// HandledCount returns the total RPC requests answered (commands + queries).
	HandledCount() uint64
}

// GRPCResponder is the kubemq-go/v2-backed Responder implementation.
type GRPCResponder struct {
	grpcAddress string
	clientID    string
	logger      *slog.Logger

	mu              sync.Mutex
	client          *kubemq.Client
	commandChannels []string
	queryChannels   []string
	subscriptions   []*kubemq.Subscription
	cancel          context.CancelFunc

	handled atomic.Uint64
}

// New constructs a GRPCResponder. grpcAddress is host:port (default
// config.GRPCConfig.ResponderAddress = localhost:50000), SEPARATE from the MQTT
// broker address. clientID is the gRPC client identifier for the responder.
func New(cfg *config.Config, clientID string, logger *slog.Logger) *GRPCResponder {
	return &GRPCResponder{
		grpcAddress: cfg.GRPC.ResponderAddress,
		clientID:    clientID,
		logger:      logger.With("component", "grpc-responder"),
	}
}

func (r *GRPCResponder) RegisterCommandChannel(channel string) {
	r.mu.Lock()
	r.commandChannels = append(r.commandChannels, channel)
	r.mu.Unlock()
}

func (r *GRPCResponder) RegisterQueryChannel(channel string) {
	r.mu.Lock()
	r.queryChannels = append(r.queryChannels, channel)
	r.mu.Unlock()
}

func (r *GRPCResponder) HandledCount() uint64 { return r.handled.Load() }

// Start connects the gRPC client and subscribes to all registered channels.
func (r *GRPCResponder) Start(parent context.Context) error {
	host, port, err := splitHostPort(r.grpcAddress)
	if err != nil {
		return fmt.Errorf("invalid grpc.responder_address %q: %w", r.grpcAddress, err)
	}

	ctx, cancel := context.WithCancel(parent)

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId(r.clientID),
		kubemq.WithCheckConnection(true),
	)
	if err != nil {
		cancel()
		return fmt.Errorf("connect gRPC responder at %s: %w", r.grpcAddress, err)
	}

	r.mu.Lock()
	r.client = client
	r.cancel = cancel
	commandChannels := append([]string(nil), r.commandChannels...)
	queryChannels := append([]string(nil), r.queryChannels...)
	r.mu.Unlock()

	onErr := func(e error) { r.logger.Warn("responder subscription error", "error", e) }

	for _, ch := range commandChannels {
		sub, serr := client.SubscribeToCommands(ctx, ch, "",
			kubemq.WithOnCommandReceive(r.handleCommand),
			kubemq.WithOnError(onErr),
		)
		if serr != nil {
			cancel()
			return fmt.Errorf("subscribe commands %q: %w", ch, serr)
		}
		r.trackSubscription(sub)
	}

	for _, ch := range queryChannels {
		sub, serr := client.SubscribeToQueries(ctx, ch, "",
			kubemq.WithOnQueryReceive(r.handleQuery),
			kubemq.WithOnError(onErr),
		)
		if serr != nil {
			cancel()
			return fmt.Errorf("subscribe queries %q: %w", ch, serr)
		}
		r.trackSubscription(sub)
	}

	r.logger.Info("gRPC responder ready",
		"address", r.grpcAddress,
		"command_channels", len(commandChannels),
		"query_channels", len(queryChannels),
	)
	return nil
}

// handleQuery echoes the request body back plus metadata + tags.
func (r *GRPCResponder) handleQuery(q *kubemq.QueryReceive) {
	reply := kubemq.NewQueryReply().
		SetRequestId(q.Id).
		SetResponseTo(q.ResponseTo).
		SetBody(q.Body).
		SetMetadata("burnin/responder").
		SetTags(map[string]string{"responder": "burnin", "rpc-type": "query"}).
		SetExecutedAt(time.Now())

	r.mu.Lock()
	client := r.client
	r.mu.Unlock()
	if client == nil {
		return
	}
	if err := client.SendQueryResponse(context.Background(), reply); err != nil {
		r.logger.Warn("send query response failed", "id", q.Id, "error", err)
		return
	}
	r.handled.Add(1)
}

// handleCommand replies executed=true with NO body (command-reply semantics).
func (r *GRPCResponder) handleCommand(c *kubemq.CommandReceive) {
	reply := kubemq.NewCommandReply().
		SetRequestId(c.Id).
		SetResponseTo(c.ResponseTo).
		SetTags(map[string]string{"responder": "burnin", "rpc-type": "command"}).
		SetExecutedAt(time.Now())

	r.mu.Lock()
	client := r.client
	r.mu.Unlock()
	if client == nil {
		return
	}
	if err := client.SendCommandResponse(context.Background(), reply); err != nil {
		r.logger.Warn("send command response failed", "id", c.Id, "error", err)
		return
	}
	r.handled.Add(1)
}

// Stop cancels subscriptions and closes the gRPC client.
func (r *GRPCResponder) Stop() error {
	r.mu.Lock()
	cancel := r.cancel
	client := r.client
	subs := r.subscriptions
	r.client = nil
	r.cancel = nil
	r.subscriptions = nil
	r.mu.Unlock()

	for _, sub := range subs {
		if sub != nil {
			sub.Cancel()
		}
	}
	if cancel != nil {
		cancel()
	}
	if client != nil {
		return client.Close()
	}
	return nil
}

func (r *GRPCResponder) trackSubscription(sub *kubemq.Subscription) {
	r.mu.Lock()
	r.subscriptions = append(r.subscriptions, sub)
	r.mu.Unlock()
}

func splitHostPort(addr string) (string, int, error) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	if h == "" {
		return "", 0, fmt.Errorf("host is required")
	}
	port, err := strconv.Atoi(p)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("invalid port %q", p)
	}
	return h, port, nil
}
