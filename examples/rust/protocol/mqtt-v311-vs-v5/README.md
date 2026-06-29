# Rust — Protocol: MQTT v3.1.1 vs v5

Contrasts MQTT 3.1.1 and 5.0 behaviour on the KubeMQ MQTT connector using `rumqttc`. Three demos run sequentially.

## Prerequisites

- Rust 1.75+ (stable)
- KubeMQ server with MQTT connector enabled on port 1883

## How to Run

```bash
cd examples/rust
KUBEMQ_MQTT_URL=tcp://localhost:1883 cargo run -p mqtt-v311-vs-v5
```

## Expected Output

```
--- Demo 1: MQTT 5.0 events pub/sub ---
v5: published to events/demo/v3
v5 received on 'events/demo/v3': v5 event payload
V5_EVENT_RECEIVED

--- Demo 2: MQTT 3.1.1 events pub/sub ---
v3.1.1: published to events/demo/v3
v3.1.1 received on 'events/demo/v3': v3.1.1 event payload
V311_EVENT_RECEIVED

--- Demo 3: MQTT 3.1.1 publish to commands/ -> silently dropped ---
v3.1.1: published to commands/demo/x (expecting silent drop)
v3.1.1: PUBACK received for commands/demo/x (message silently dropped by broker, not delivered to any responder)
V311_RPC_DROP_CONFIRMED

All demos complete.
```

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version (Demo 1) | 5.0 |
| MQTT version (Demo 2 & 3) | 3.1.1 |
| Events topic | `events/demo/v3` |
| Commands topic | `commands/demo/x` |
| QoS | 1 (AtLeastOnce) |
| KubeMQ patterns | Events, Commands |

## What's Happening

### Demo 1 — MQTT 5.0 Events
Standard pub/sub on `events/demo/v3` using the MQTT 5.0 client. Works normally.

### Demo 2 — MQTT 3.1.1 Events
Same pub/sub using a MQTT 3.1.1 client. Events work with v3.1.1 too.

### Demo 3 — v3.1.1 RPC Drop
Publish to `commands/demo/x` using a MQTT 3.1.1 client:
- PUBACK `0x00` returns immediately (broker accepts the publish).
- **Message is silently dropped** — never delivered to any responder.
- No error code, no DISCONNECT. The drop is silent.

This happens because MQTT 3.1.1 has no `ResponseTopic` or `CorrelationData` properties, which are required for RPC routing. The broker cannot route the command without them.

## Key Protocol Differences

| Feature | MQTT 3.1.1 | MQTT 5.0 |
|---|---|---|
| Events pub/sub | Yes | Yes |
| Events-Store pub/sub | Yes | Yes |
| Queue produce (publish `queues/<ch>`) | Yes | Yes |
| Queue consume (`$share/<g>/queues/<ch>`) | **No** (needs shared subscriptions) | Yes |
| RPC (Commands/Queries) | **Silently dropped** | Yes |
| User Properties (Tags) | No | Yes |
| Shared subscriptions ($share) | No | Yes |
| Response Topic / Correlation Data | No | Yes |

## Related Examples

- [rpc/command-round-trip](../../rpc/command-round-trip/) — v5 RPC that works
- [events/basic-pubsub](../../events/basic-pubsub/) — v5 events baseline
