# Architecture

## Overview

The KubeMQ MQTT connector is an embedded [mochi-mqtt](https://github.com/mochi-mqtt/server) broker that runs inside the KubeMQ server. It bridges the standard MQTT protocol (3.1.1 and 5.0) onto KubeMQ's five native messaging patterns. No custom SDK or library is required — any standard MQTT client library works.

## Protocol Stack

```
MQTT Client (any language / tool)
         │  tcp://host:1883
         │  tls://host:8883  (requires Security config)
         │  ws://host:8083/  (WebSocket path is always "/")
         ▼
┌────────────────────────────────────────┐
│         KubeMQ server                  │
│  ┌──────────────────────────────────┐  │
│  │  mochi-mqtt server               │  │
│  │  ┌────────────────────────────┐  │  │
│  │  │  Bridge Hook               │  │  │
│  │  │                            │  │  │
│  │  │  ┌──────────────────────┐  │  │  │
│  │  │  │  Topic Mapper        │  │  │  │
│  │  │  │                      │  │  │  │
│  │  │  └──────────┬───────────┘  │  │  │
│  │  └─────────────┼──────────────┘  │  │
│  └───────────────-┼─────────────────┘  │
│                   ▼                    │
│  ┌────────────────────────────────────┐ │
│  │  Array Service (KubeMQ core)       │ │
│  │  ┌────────┬──────┬─────┬──────┐   │ │
│  │  │Events  │Store │Queue│ RPC  │   │ │
│  │  └────────┴──────┴─────┴──────┘   │ │
│  └────────────────────────────────────┘ │
│                   │                    │
│  ┌────────────────▼───────────────────┐ │
│  │  NATS broker (internal transport)  │ │
│  └────────────────────────────────────┘ │
└────────────────────────────────────────┘
         │
         ▼  (also bridged to)
gRPC clients, REST clients, other connectors
```

## MQTT Topic → KubeMQ Pattern / Channel Mapping

The first topic segment (the **prefix**) selects the KubeMQ pattern. The remaining segments become the KubeMQ channel, with `/` translated to `.`.

| MQTT topic / filter | KubeMQ pattern | Direction | Example topic | KubeMQ channel |
|---------------------|----------------|-----------|---------------|----------------|
| `events/<ch>` | Events | publish + subscribe | `events/site1/temp` | `site1.temp` |
| `store/<ch>` | Events-Store | publish + subscribe (StartNewOnly) | `store/site1/temp` | `site1.temp` |
| `queues/<ch>` | Queues | **publish only** (produce) | `queues/jobs/email` | `jobs.email` |
| `commands/<ch>` | Commands RPC | **publish only**, MQTT 5.0 only | `commands/svc/reboot` | `svc.reboot` |
| `queries/<ch>` | Queries RPC | **publish only**, MQTT 5.0 only | `queries/svc/status` | `svc.status` |
| `$share/<group>/queues/<ch>` | Queues | **subscribe only** (consume), QoS ≥ 1 | `$share/g1/queues/jobs/email` | `jobs.email` |
| `$reply/<clientID>/<suffix>` | (mochi-local) | subscribe (own namespace only) | `$reply/client1/inbox` | not routed to array |
| *(prefixless)* | DefaultPattern (`events` by default) | publish + subscribe | `site1/temp` | `site1.temp` |

**Translation rule**: topic path separators `/` become KubeMQ channel dot separators `.`.
Example: `events/site1/sensors/temp` → channel `site1.sensors.temp`.

**Warning — gotcha #5**: a literal `.` inside a topic segment is **not** escaped — it passes through unchanged and conflates with `/`. Both `events/a.b/c` and `events/a/b/c` map to channel `a.b.c`. Avoid dots in MQTT topic segments.

## Wildcard Translation

Wildcards are permitted **only** on Events subscriptions. Attempting a wildcard subscribe on any other pattern returns SUBACK `0xA2`.

| MQTT wildcard | KubeMQ wildcard | Constraint |
|---------------|-----------------|------------|
| `+` (single level) | `*` | any segment position |
| `#` (multi level) | `>` | must be the final segment |

Examples:
- subscribe `events/site1/+` → KubeMQ channel `site1.*`
- subscribe `events/#` → KubeMQ channel `>`
- subscribe `store/#` → **SUBACK `0xA2`** (not permitted on Events-Store)

**Gotcha #6 — overlapping wildcard filters multiply delivery**: each distinct subscribe filter creates an independent bridge registry entry. If N of a client's subscriptions match the same published message, the client receives N copies — one per matching entry. There is no cross-entry deduplication in V1. For example, if you subscribe to `#`, `events/leak/x` (exact), and `events/#` simultaneously, a publish to `events/leak/x` delivers **3 copies**.

## Cross-Protocol Bridging

The array service is the shared message bus for all KubeMQ connectors. An MQTT publish to `events/it/cross` is received by a gRPC `SubscribeEvents` on channel `it.cross`, and vice versa. The same applies to Events-Store, Queues, Commands, and Queries — any connector (gRPC, REST, CloudEvents, MQTT) can interoperate.

## User Properties ↔ KubeMQ Tags Mapping

MQTT 5.0 User Properties map bidirectionally to KubeMQ message Tags. This mapping is **MQTT 5.0 only** — MQTT 3.1.1 has no user-properties, so nothing is carried in either direction over a v3.1.1 connection.

| Direction | MQTT side | KubeMQ side | Notes |
|-----------|-----------|-------------|-------|
| Inbound (publish → KubeMQ) | `Properties.User` entries | `Tags` map | Copied 1:1; duplicate keys: last-wins |
| Outbound (KubeMQ → subscriber) | `Properties.User` entries | `Tags` map | Injected on delivery to v5 subscribers only; v3.1.1 subscribers receive no user-props |

**Caps** (enforced by the connector):

| Cap | Value | Violation result |
|-----|-------|-----------------|
| Max properties per message | 32 | PUBACK `0x97` (v5) or silent drop (v3.1.1) |
| Max total bytes (all keys + values) | 4096 bytes | PUBACK `0x97` (v5) or silent drop (v3.1.1) |

These caps apply to Events, Events-Store, and Queues publishes. Exceeding either cap causes the message to be rejected — it is not routed to the array.

**RPC response props** (outbound from server on v5 only):

| User-prop key | Pattern | Value |
|---------------|---------|-------|
| `kubemq-metadata` | Queries | response metadata string |
| `kubemq-executed` | Commands | `"true"` or `"false"` |
| `kubemq-error` | Commands | error string (omitted on success) |
