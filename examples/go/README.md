# KubeMQ MQTT — Go Examples

All Go examples use:
- [`github.com/eclipse/paho.golang`](https://github.com/eclipse/paho.golang) v0.23.0 — MQTT 5.0 (primary)
- [`github.com/eclipse/paho.mqtt.golang`](https://github.com/eclipse/paho.mqtt.golang) v1.5.1 — MQTT 3.1.1 (protocol comparison only)

## Prerequisites

- Go 1.24+
- KubeMQ server running with the MQTT connector enabled

## Setup

```bash
cd examples/go
go mod download
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KUBEMQ_MQTT_URL` | `tcp://localhost:1883` | Broker address. Scheme selects transport: `tcp://` plain TCP, `tls://` TLS, `ws://` / `wss://` WebSocket |
| `KUBEMQ_MQTT_TOKEN` | _(none)_ | JWT for auth-jwt example (requires auth-enabled broker) |
| `KUBEMQ_GRPC_ADDR` | `localhost:50000` | KubeMQ gRPC address for RPC examples (test responder) |

## Run Any Example

```bash
export KUBEMQ_MQTT_URL=tcp://localhost:1883

go run ./events/basic-pubsub/main.go
```

## Examples

| ID | Pattern | Directory | Notes |
|----|---------|-----------|-------|
| events/basic-pubsub | Basic Events pub/sub | [events/basic-pubsub/](events/basic-pubsub/) | |
| events/wildcard-subscribe | Events wildcard subscribe | [events/wildcard-subscribe/](events/wildcard-subscribe/) | `+` and `#` on `events/` only |
| events-store/start-new-only | Events-Store (StartNewOnly) | [events-store/start-new-only/](events-store/start-new-only/) | No historical replay over MQTT |
| queues/shared-consume | Queue shared consume | [queues/shared-consume/](queues/shared-consume/) | `$share/<group>/queues/<ch>` at QoS ≥ 1 |
| queues/ack-and-redelivery | Queue ack & requeue on disconnect | [queues/ack-and-redelivery/](queues/ack-and-redelivery/) | |
| rpc/query-round-trip | Query RPC round-trip | [rpc/query-round-trip/](rpc/query-round-trip/) | MQTT 5.0 only; needs gRPC responder |
| rpc/command-round-trip | Command RPC round-trip | [rpc/command-round-trip/](rpc/command-round-trip/) | MQTT 5.0 only; needs gRPC responder |
| connectivity/tls | TLS connection | [connectivity/tls/](connectivity/tls/) | Live test: TLS listener closed on reference broker |
| connectivity/websocket | WebSocket connection | [connectivity/websocket/](connectivity/websocket/) | Set `KUBEMQ_MQTT_URL=ws://...` |
| connectivity/auth-jwt | JWT authentication | [connectivity/auth-jwt/](connectivity/auth-jwt/) | Live test: auth disabled on reference broker |
| protocol/mqtt-v311-vs-v5 | MQTT 3.1.1 vs 5.0 comparison | [protocol/mqtt-v311-vs-v5/](protocol/mqtt-v311-vs-v5/) | |

## Topic Grammar Reference

| MQTT Topic Prefix | KubeMQ Pattern | Notes |
|-------------------|----------------|-------|
| `events/<ch>` | Events | Wildcards `+`/`#` allowed |
| `store/<ch>` | Events-Store | Always StartNewOnly |
| `queues/<ch>` | Queues (produce) | QoS ≥ 1 |
| `$share/<g>/queues/<ch>` | Queues (consume) | QoS ≥ 1; group is audit-only |
| `commands/<ch>` | Commands RPC | MQTT 5.0 only |
| `queries/<ch>` | Queries RPC | MQTT 5.0 only |

`/` in topic ↔ `.` in KubeMQ channel name (e.g. `events/a/b` → channel `a.b`).
