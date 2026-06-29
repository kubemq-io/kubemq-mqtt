# KubeMQ MQTT — JavaScript/TypeScript Examples

All examples use [MQTT.js](https://github.com/mqttjs/MQTT.js) (`mqtt` npm package, v5.15.1+) and are written in TypeScript. They target **MQTT 5.0** by default (`protocolVersion: 5`); the `protocol/mqtt-v311-vs-v5` example explicitly contrasts v5 with v3.1.1.

## Prerequisites

- Node.js 18+
- npm
- KubeMQ server with the MQTT connector enabled (default: port 1883)

## Setup

```bash
cd examples/javascript
npm install
```

## Broker URL

All examples read `KUBEMQ_MQTT_URL` (default `tcp://localhost:1883`). The scheme selects the transport:

| Scheme | Transport | Port |
|--------|-----------|------|
| `tcp://` | plain TCP | 1883 |
| `tls://` | TLS | 8883 |
| `ws://` | WebSocket | 8083 |

## Run Any Example

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
npx tsx events/basic-pubsub/index.ts
```

## Examples

| Variant | Pattern | Directory |
|---------|---------|-----------|
| events/basic-pubsub | Events fire-and-forget pub/sub + v5 user-props | [events/basic-pubsub/](events/basic-pubsub/) |
| events/wildcard-subscribe | Events wildcard subscribe (`+`, `#`); overlapping-filter copies | [events/wildcard-subscribe/](events/wildcard-subscribe/) |
| events-store/start-new-only | Events-Store (persistent, StartNewOnly — no replay) | [events-store/start-new-only/](events-store/start-new-only/) |
| queues/shared-consume | Queues produce + `$share` consume at QoS 1 | [queues/shared-consume/](queues/shared-consume/) |
| queues/ack-and-redelivery | Queues ack-on-PUBACK, ack-timeout, disconnect requeue | [queues/ack-and-redelivery/](queues/ack-and-redelivery/) |
| rpc/query-round-trip | RPC Queries (v5-only): ResponseTopic + CorrelationData | [rpc/query-round-trip/](rpc/query-round-trip/) |
| rpc/command-round-trip | RPC Commands (v5-only): `kubemq-executed` response prop | [rpc/command-round-trip/](rpc/command-round-trip/) |
| connectivity/tls | Events over TLS (`tls://host:8883`) | [connectivity/tls/](connectivity/tls/) |
| connectivity/websocket | Events over WebSocket (`ws://host:8083/`) | [connectivity/websocket/](connectivity/websocket/) |
| connectivity/auth-jwt | Events with password-as-JWT auth | [connectivity/auth-jwt/](connectivity/auth-jwt/) |
| protocol/mqtt-v311-vs-v5 | v5 vs v3.1.1 feature contrast (user-props, RPC drop) | [protocol/mqtt-v311-vs-v5/](protocol/mqtt-v311-vs-v5/) |

## Key Gotchas

1. **Retain silently dropped** — PUBACK succeeds (0x00) but the message is not delivered and not stored.
2. **Events-Store: no historical replay** — subscription is always StartNewOnly; pre-subscribe messages are never delivered.
3. **`$share` group is one competing pool** — ALL groups share ONE KubeMQ queue; the group name is audit/metrics-only, not per-group-copy.
4. **RPC is MQTT 5.0 only + immediate PUBACK** — v3.1.1 RPC publishes are silently dropped; PUBACK arrives before the response, so implement your own timeout.
5. **Literal `.` in topic conflates with `/`** — `events/a.b/c` and `events/a/b/c` both map to channel `a.b.c`.
6. **Overlapping events wildcard filters** — N overlapping subscribe filters matching one publish → N copies delivered (no cross-entry dedup).

## Related Resources

- [docs/](../../docs/) — full connector contract, topic grammar, configuration
- [examples/](../) — other language examples
