# Protocol Versions

## Overview

The KubeMQ MQTT connector supports **MQTT 3.1.1 (protocol level 4)** and **MQTT 5.0 (protocol level 5)**. MQTT 3.1 (protocol level 3) is rejected at connect time. MQTT 5.0 is the default and recommended version — it unlocks RPC, Queues consumption, User Properties, and richer error reporting. MQTT 3.1.1 is retained for compatibility with existing clients and Ruby tooling.

---

## Feature Matrix

| Feature | MQTT 3.1.1 | MQTT 5.0 | Notes |
|---------|-----------|---------|-------|
| Events publish + subscribe | ✅ | ✅ | |
| Events-Store publish + subscribe (StartNewOnly) | ✅ | ✅ | No historical replay over MQTT regardless of version |
| Queues produce (plain publish to `queues/<ch>`) | ✅ | ✅ | |
| Queues consume (`$share/<g>/queues/<ch>`) | ❌ requires v5 | ✅ | Requires QoS ≥ 1; `$share` shared subscriptions are MQTT 5.0 only over this connector |
| RPC Commands (publish to `commands/<ch>`) | ❌ silently dropped | ✅ | v3.1.1 PUBACK succeeds but message is NOT executed |
| RPC Queries (publish to `queries/<ch>`) | ❌ silently dropped | ✅ | Same silent-drop behaviour |
| MQTT User Properties → KubeMQ Tags | ❌ unavailable | ✅ | Wire format only has user-props in v5 |
| KubeMQ Tags → MQTT User Properties on delivery | ❌ unavailable | ✅ | Delivered silently absent to v3.1.1 subscribers |
| Rich PUBACK / SUBACK reason codes | Limited | ✅ | v3.1.1 PUBACK is single-byte 0x00; detailed codes (0x83, 0x87, etc.) require v5 |
| `ResponseTopic` and `CorrelationData` properties | ❌ | ✅ | Required for RPC |
| Shared subscriptions (`$share/`) | ❌ | ✅ | `$share` is an MQTT 5.0 feature; over this connector queue consume is unconditionally v5-only |
| `clean_session=false` session restore | ✅ | ✅ (`CleanStart=false`) | Node-local only |
| Will-retain at CONNECT | ❌ CONNACK `0x9A` | ❌ CONNACK `0x9A` | Retain not supported on either version |
| Runtime retain publish | Silent drop — PUBACK `0x00` | Silent drop — PUBACK `0x00` | See gotcha #1 |
| MQTT 3.1 (level 3) | ❌ rejected (CONNACK) | n/a | `MinProtocolVersion=4` |

---

## Gotcha #4 — RPC Is MQTT 5.0 Only; Silent Drop on v3.1.1

> **Warning:** A v3.1.1 publish to `commands/<ch>` or `queries/<ch>` receives a **success PUBACK (`0x00`)** at the wire level but the message is **silently dropped** — it is never executed. The `publish.error` event is audited server-side.

There is **no error visible to the v3.1.1 publisher**. The silent drop exists because MQTT 3.1.1 has no `ResponseTopic` or `CorrelationData` properties, so the connector cannot construct an RPC request.

To use RPC you must:
1. Connect with MQTT 5.0 (`protocolVersion: 5` / `MQTTv5` / `Level5`).
2. Subscribe to your own `$reply/<clientID>/...` topic **before** publishing.
3. Set `Properties.ResponseTopic` and `Properties.CorrelationData` on every RPC publish.

---

## MQTT 5.0 — Recommended for All New Clients

MQTT 5.0 adds the following capabilities that the KubeMQ connector uses directly:

| v5 addition | KubeMQ usage |
|------------|-------------|
| `ResponseTopic` | RPC reply routing |
| `CorrelationData` | RPC response correlation |
| User Properties | KubeMQ Tags bidirectional mapping |
| Shared subscriptions (`$share/`) | Queue consumption |
| Extended reason codes | Detailed PUBACK / SUBACK / CONNACK errors |
| `CleanStart` flag | Per-connect session control |

---

## MQTT 3.1.1 — Supported Subset

MQTT 3.1.1 clients have access to:

- Events publish and subscribe (including wildcards)
- Events-Store publish and subscribe (StartNewOnly)
- Queues produce
- TLS and WebSocket transports
- Password-as-JWT authentication

Not available from MQTT 3.1.1:

| Feature | Reason |
|---------|--------|
| RPC (Commands / Queries) | No `ResponseTopic` / `CorrelationData` — silently dropped |
| Queues consume via `$share` | `$share` shared subscriptions require v5 support in the client stack |
| User Properties ↔ KubeMQ Tags | v3.1.1 wire format has no user-properties field |
| Extended reason codes | v3.1.1 PUBACK is a bare `0x00` success byte only |

---

## MQTT 3.1 Rejected

Protocol level 3 (MQTT 3.1, the original 2010 spec) is explicitly rejected. Connecting with protocol level 3 results in CONNACK refused. Set `CONNECTORSMQTT_CAPABILITIES_MIN_PROTOCOL_VERSION=4` (the default) to enforce this.

---

## Ruby — MQTT 3.1.1 Only

The Ruby `mqtt` gem (njh/ruby-mqtt) is **MQTT 3.1.1 only** — it does not support MQTT 5.0. Ruby examples therefore ship a documented v3.1.1 subset:

| Example | Ruby status |
|---------|------------|
| `events/basic-pubsub` | ✅ |
| `events/wildcard-subscribe` | ✅ |
| `events-store/start-new-only` | ✅ (v3.1.1 publish/subscribe) |
| `queues/shared-consume` | ❌ N/A — needs v5 |
| `queues/ack-and-redelivery` | ❌ N/A — needs v5 |
| `rpc/query-round-trip` | ❌ N/A — needs v5 |
| `rpc/command-round-trip` | ❌ N/A — needs v5 |
| `connectivity/tls` | ✅ (TCP TLS via `ssl: true`) |
| `connectivity/websocket` | ❌ N/A — gem has no WebSocket transport |
| `connectivity/auth-jwt` | ✅ |
| `protocol/mqtt-v311-vs-v5` | ✅ (note only — gem is v3.1.1 only) |

For v5-only patterns from Ruby, use a different MQTT client library or a different language.

---

## Comparing v3.1.1 and v5 (mqtt-v311-vs-v5 Example)

The `protocol/mqtt-v311-vs-v5` example in each language demonstrates:

1. **v5 publish with User Properties** → KubeMQ Tags arrive on a gRPC subscriber
2. **v3.1.1 publish to the same channel** → no User Properties, no Tags
3. **v3.1.1 publish to `commands/<ch>`** → PUBACK `0x00`, command silently dropped (contrast)

---

## See Also

- [guides/topic-mapping.md](topic-mapping.md) — User Properties ↔ KubeMQ Tags mapping
- [patterns/commands.md](../patterns/commands.md) — RPC commands (v5 only)
- [patterns/queries.md](../patterns/queries.md) — RPC queries (v5 only)
- [reference/reason-codes.md](../reference/reason-codes.md) — full reason code table
- [examples/go/protocol/mqtt-v311-vs-v5/](../../examples/go/protocol/mqtt-v311-vs-v5/)
- [examples/python/protocol/mqtt_v311_vs_v5/](../../examples/python/protocol/mqtt_v311_vs_v5/)
- [examples/javascript/protocol/mqtt-v311-vs-v5/](../../examples/javascript/protocol/mqtt-v311-vs-v5/)
- [examples/ruby/](../../examples/ruby/) — v3.1.1 subset documentation
