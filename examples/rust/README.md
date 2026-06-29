# KubeMQ MQTT — Rust Examples

All Rust examples use [`rumqttc`](https://crates.io/crates/rumqttc) (v0.25, MQTT 5.0) with the `tokio` async runtime.

## Prerequisites

- Rust 1.75+ (stable)
- Cargo
- KubeMQ server running with the MQTT connector enabled (default port 1883)

## Environment Variable

| Variable | Default | Description |
|---|---|---|
| `KUBEMQ_MQTT_URL` | `tcp://localhost:1883` | Broker URL. Scheme selects transport: `tcp://` plain TCP, `tls://` TLS (port 8883), `ws://` WebSocket (path `/`). |

## Run Any Example

```bash
export KUBEMQ_MQTT_URL=tcp://localhost:1883

# From the workspace root:
cd examples/rust
cargo run -p basic-pubsub

# Or with the manifest path:
cargo run --manifest-path examples/rust/events/basic-pubsub/Cargo.toml
```

## Variants

| # | Variant | Directory | KubeMQ Pattern |
|---|---|---|---|
| 1 | Events: Basic Pub/Sub | [events/basic-pubsub](events/basic-pubsub/) | Events |
| 2 | Events: Wildcard Subscribe | [events/wildcard-subscribe](events/wildcard-subscribe/) | Events |
| 3 | Events-Store: Start New Only | [events-store/start-new-only](events-store/start-new-only/) | Events-Store |
| 4 | Queues: Shared Consume | [queues/shared-consume](queues/shared-consume/) | Queues |
| 5 | Queues: Ack and Redelivery | [queues/ack-and-redelivery](queues/ack-and-redelivery/) | Queues |
| 6 | RPC: Query Round-Trip | [rpc/query-round-trip](rpc/query-round-trip/) | Queries |
| 7 | RPC: Command Round-Trip | [rpc/command-round-trip](rpc/command-round-trip/) | Commands |
| 8 | Connectivity: TLS | [connectivity/tls](connectivity/tls/) | Events over TLS |
| 9 | Connectivity: WebSocket | [connectivity/websocket](connectivity/websocket/) | Events over WebSocket |
| 10 | Connectivity: Auth JWT | [connectivity/auth-jwt](connectivity/auth-jwt/) | Events with JWT auth |
| 11 | Protocol: MQTT v3.1.1 vs v5 | [protocol/mqtt-v311-vs-v5](protocol/mqtt-v311-vs-v5/) | Events (v3.1.1 RPC drop) |

## Topic Grammar Quick Reference

| MQTT topic prefix | KubeMQ pattern | Notes |
|---|---|---|
| `events/<ch>` | Events | `/` becomes `.` in channel name |
| `store/<ch>` | Events-Store | Always `StartNewOnly` — no historical replay |
| `queues/<ch>` | Queues (produce) | Subscribe requires `$share/<group>/queues/<ch>` at QoS >= 1 |
| `commands/<ch>` | Commands (RPC) | MQTT 5.0 only; v3.1.1 publishes are silently dropped |
| `queries/<ch>` | Queries (RPC) | MQTT 5.0 only; v3.1.1 publishes are silently dropped |

## Key MQTT 5.0 Conventions Used

- **RPC**: subscribe to `$reply/<clientID>/<suffix>` first, then publish with `ResponseTopic` + `CorrelationData` properties.
- **Queue consume**: `$share/<group>/queues/<ch>` at QoS 1 only (QoS 0 → SUBACK `0x83`).
- **Wildcards**: `+` → `*`, `#` → `>` allowed only on `events/...` subscriptions.
- **Retain**: forced `RetainAvailable=0`; retained publishes are silently dropped (PUBACK `0x00` but message not delivered).
- **User Properties**: map to/from KubeMQ Tags for events, store, and queues (capped at 32 props / 4096 bytes).
