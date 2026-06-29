# Topic Mapping

## Overview

The KubeMQ MQTT connector maps every MQTT topic to a KubeMQ messaging pattern and channel through a well-defined grammar. The **first topic segment (the prefix)** selects the KubeMQ pattern; the **remaining segments** become the KubeMQ channel name with `/` replaced by `.`.

Understanding this grammar is essential before writing any producer or consumer code.

---

## Prefix-to-Pattern Table

| MQTT topic | KubeMQ pattern | Direction | Example topic | KubeMQ channel |
|------------|----------------|-----------|---------------|----------------|
| `events/<ch>` | Events | publish + subscribe | `events/site1/temp` | `site1.temp` |
| `store/<ch>` | Events-Store | publish + subscribe (StartNewOnly) | `store/site1/temp` | `site1.temp` |
| `queues/<ch>` | Queues | publish = produce only | `queues/jobs/email` | `jobs.email` |
| `$share/<group>/queues/<ch>` | Queues | subscribe = consume (QoS ≥ 1) | `$share/g1/queues/jobs/email` | `jobs.email` |
| `commands/<ch>` | Commands (RPC) | publish = send (MQTT v5 only) | `commands/svc/reboot` | `svc.reboot` |
| `queries/<ch>` | Queries (RPC) | publish = send (MQTT v5 only) | `queries/svc/status` | `svc.status` |
| `$reply/<clientID>/<suffix>` | (mochi-local) | subscribe + RPC ResponseTopic | `$reply/c1/inbox` | not routed to KubeMQ |

---

## Path Separator Conversion: `/` → `.`

MQTT uses `/` as the path separator; KubeMQ uses `.`. The connector converts every `/` in the channel portion of the topic to `.`:

```
events/site1/sensors/temp
       ↓ strip prefix
       site1/sensors/temp
       ↓ replace / with .
       site1.sensors.temp        ← KubeMQ channel
```

This conversion is bidirectional — when the connector delivers a KubeMQ message to an MQTT subscriber, the channel `.` separators are not converted back. The MQTT subscribe filter must match the dotted form.

---

## Gotcha #5 — Literal `.` in a Topic Segment Conflates With `/`

> **Warning:** A literal `.` character inside a topic segment maps to `.` in the KubeMQ channel, which is **indistinguishable** from a `/`-converted `.`. The round-trip is lossy.

| MQTT topic | KubeMQ channel |
|------------|----------------|
| `events/a/b/c` | `a.b.c` |
| `events/a.b/c` | `a.b.c` ← same channel! |
| `events/a/b.c` | `a.b.c` ← same channel! |

**Best practice:** do not use literal `.` in MQTT topic segments. Use `/` exclusively for hierarchy. Reserve `.` for KubeMQ channel names used on gRPC/REST clients.

---

## Prefixless Topics and DefaultPattern

A topic with no recognized prefix (e.g. `sensor/data`) is routed to the `DefaultPattern`:

| `DefaultPattern` setting | Behaviour |
|--------------------------|-----------|
| `events` (default) | routed to the Events pattern on channel `sensor.data` |
| `store` | routed to the Events-Store pattern |
| `none` | publish rejected — PUBACK `0x90`; subscribe rejected — SUBACK `0x8F` |

Configure with `CONNECTORSMQTT_DEFAULT_PATTERN`. The default value is `events`.

---

## Wildcard Subscriptions (Events Only)

MQTT wildcards are supported **on Events subscriptions only**. Wildcards on any other pattern trigger SUBACK `0xA2`.

| MQTT wildcard | KubeMQ equivalent | Example MQTT filter | Matches |
|---------------|-------------------|---------------------|---------|
| `+` (single level) | `*` | `events/site1/+` | `events/site1/temp`, `events/site1/hum` |
| `#` (multi level) | `>` | `events/site1/#` | `events/site1/temp`, `events/site1/a/b/c` |

Rules:
- `#` must be the **final segment** of the topic filter.
- Both wildcards can appear in a single filter: `events/+/sensors/#`.
- Non-events wildcard subscribe → SUBACK `0xA2` wildcard-subscriptions-not-supported.

---

## Gotcha #6 — Overlapping Wildcard Filters Deliver Multiple Copies

> **Warning:** Each distinct Events subscribe filter creates an independent bridge registry entry. When a publish matches N overlapping entries, the message is injected N times — one copy per matching entry. There is **no cross-entry deduplication** in V1.

Example: client subscribes to three overlapping filters:

| Filter | Matches `events/leak/x`? |
|--------|--------------------------|
| `#` | yes |
| `events/leak/x` | yes (exact) |
| `events/#` | yes |

A single publish to `events/leak/x` delivers **3 copies** to the subscriber — one per matching bridge entry. This is permitted by MQTT 5.0 §3.3.5 and is the documented V1 behaviour (integration test #25 asserts `wantCopies = 3`).

**Best practice:** use non-overlapping subscribe filters when duplicate delivery is unacceptable.

---

## Queue Consume: `$share` Shared Subscriptions

Plain queue subscribe (`queues/<ch>`) is **not allowed** — SUBACK `0x83`. Queue consumption requires an MQTT 5.0 shared subscription:

```
$share/<group>/queues/<channel>
```

| Component | Meaning |
|-----------|---------|
| `$share` | MQTT 5.0 shared subscription prefix |
| `<group>` | Group name — **audit/metrics label only** (see gotcha #3) |
| `queues/<channel>` | Must resolve to the Queues pattern |

QoS must be ≥ 1. QoS 0 shared queue subscribe → SUBACK `0x83`.

### Gotcha #3 — `$share` Group Is One Competing Pool

> **Warning:** The `$share` group name is **audit and metrics-only**. ALL groups compete in **ONE shared KubeMQ queue pool**. This is NOT MQTT per-group-copy semantics.

| Behaviour | MQTT standard `$share` | KubeMQ MQTT connector (V1) |
|-----------|------------------------|---------------------------|
| `$share/A/topic` vs `$share/B/topic` | Each group gets its own copy | Both groups consume from the **same pool** |
| Message delivery | One copy per group | One copy total, any consumer wins |

Use different KubeMQ channels when true fan-out to independent consumer groups is required.

---

## RPC Reply Topics: `$reply/<clientID>/<suffix>`

`$reply` is a mochi-local reserved namespace for RPC response routing. It is never routed to the KubeMQ array.

Rules:

| Rule | Details |
|------|---------|
| Format | `$reply/<clientID>/<suffix>` — **3 segments minimum** |
| Own namespace | A client may only subscribe to or use as ResponseTopic its **own** `$reply/<own-clientID>/...` |
| Foreign namespace | Using another client's `$reply` namespace → PUBACK `0x83` (on publish with foreign ResponseTopic) |
| Array routing | Never forwarded to KubeMQ — stays inside the broker |

RPC clients subscribe to their own reply topic **before** publishing the RPC request, then use it as the `Properties.ResponseTopic` in the publish packet.

---

## MQTT 5.0 User Properties ↔ KubeMQ Tags

On MQTT 5.0 connections, User Properties on a PUBLISH are carried through to KubeMQ message `Tags` and vice versa on delivery. This mapping is **MQTT 5.0 only** — MQTT 3.1.1 has no User Properties.

### Inbound: PUBLISH User Properties → KubeMQ Tags

Every `Properties.User` entry on a v5 PUBLISH is copied 1:1 into the KubeMQ message `Tags` map. Duplicate keys are last-wins. Applies to Events, Events-Store, Queues publishes and RPC bridge.

### Outbound: KubeMQ Tags → Delivery User Properties

When a KubeMQ message is delivered to a v5 MQTT subscriber, the message `Tags` are written back as MQTT 5.0 `Properties.User`. A v3.1.1 subscriber receives no user properties (the v3 wire format has no such field).

### Canonical cross-protocol example

An MQTT publish to `events/it/cross` with user-prop `k1=v1` is received by a gRPC `SubscribeEvents` consumer as `ev.Tags["k1"] == "v1"`, demonstrating cross-protocol tag propagation.

### Caps

| Cap | Limit | Exceeded on v5 | Exceeded on v3.1.1 |
|-----|-------|----------------|-------------------|
| Max user properties count | 32 | PUBACK `0x97` + `publish.error` | Silent drop + `publish.error` |
| Max total bytes (all keys + values) | 4096 bytes | PUBACK `0x97` + `publish.error` | Silent drop + `publish.error` |

The message is not routed to the array when either cap is exceeded.

---

## Reason Codes for Invalid Topics

| Trigger | Packet | Code |
|---------|--------|------|
| `DefaultPattern=none`, prefixless publish | PUBACK | `0x90` topic-name-invalid |
| `DefaultPattern=none`, prefixless subscribe | SUBACK | `0x8F` topic-filter-invalid |
| Empty channel segment | SUBACK | `0x8F` topic-filter-invalid |
| Non-events wildcard subscribe | SUBACK | `0xA2` wildcard-subscriptions-not-supported |
| Plain `queues/<ch>` subscribe | SUBACK | `0x83` implementation-specific |
| QoS 0 `$share/*/queues/<ch>` subscribe | SUBACK | `0x83` implementation-specific |
| Foreign `$reply` namespace (publish) | PUBACK | `0x83` implementation-specific |

---

## See Also

- [reference/topic-grammar.md](../reference/topic-grammar.md) — formal grammar reference
- [patterns/events.md](../patterns/events.md) — wildcard subscribe + gotcha #6
- [patterns/queues.md](../patterns/queues.md) — `$share` consume semantics + gotcha #3
- [patterns/commands.md](../patterns/commands.md) — RPC + `$reply` + gotcha #4
- [guides/protocol-versions.md](protocol-versions.md) — v3.1.1 has no User Properties
- [examples/go/events/basic-pubsub/](../../examples/go/events/basic-pubsub/) — user-prop ↔ tag demo
- [examples/go/events/wildcard-subscribe/](../../examples/go/events/wildcard-subscribe/) — overlapping filters
- [examples/go/queues/shared-consume/](../../examples/go/queues/shared-consume/) — `$share` consume
