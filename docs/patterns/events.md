# Events Pattern

## Overview

Events implement **fire-and-forget pub/sub**. A publisher sends a message to an MQTT topic
prefixed with `events/`; the KubeMQ MQTT connector routes it to all connected subscribers on
the matching channel. Messages are **not persisted** — a subscriber must be online at the moment
of delivery or it misses the message.

| Property | Value |
|----------|-------|
| KubeMQ pattern | Events |
| MQTT topic prefix | `events/<channel-path>` |
| Direction | publish + subscribe |
| Wildcard subscribe | Supported (`+` → `*`, `#` → `>`) |
| Minimum QoS | 0 (any QoS) |
| MQTT 5.0 required | No (works on 3.1.1 and 5.0) |
| User Properties → KubeMQ Tags | v5 only (§ User Properties section below) |

## Topic Grammar

```
MQTT topic              KubeMQ channel
events/demo/x    →      demo.x
events/site1/temp →     site1.temp
events/alerts    →      alerts
```

- The `events/` prefix is stripped; the remaining path segments are joined with `.`.
- `/` in the MQTT path becomes `.` in the KubeMQ channel.

> **Gotcha #5 — literal `.` conflates with `/`.**
> A literal dot in any topic segment is indistinguishable from a slash after mapping.
> `events/a.b/c` and `events/a/b/c` both map to channel `a.b.c`. Avoid dots in
> MQTT topic segments if round-trip fidelity matters.

## Publishing

Publish to `events/<channel-path>` at any QoS. No additional MQTT 5.0 properties are
required; a v3.1.1 client works fine.

```
PUBLISH  topic=events/demo/x  QoS=1  payload=<bytes>
PUBACK   reason=0x00  (success)
```

Common failure codes on publish:

| PUBACK reason | Cause |
|---------------|-------|
| `0x80` | Broker not yet ready (startup gating) |
| `0x87` | ACL deny on this channel |
| `0x90` | Prefixless topic AND `DefaultPattern=none` |
| `0x97` | User-Properties cap exceeded (v5, >32 props or >4096 bytes total) |

## Subscribing

Subscribe to `events/<channel-path>` at any QoS (QoS 0, 1, or 2).

```
SUBSCRIBE  filter=events/demo/+  QoS=1
SUBACK     reason=0x01  (granted QoS 1)
```

### Wildcard Subscriptions

MQTT wildcards are allowed **only** on Events subscriptions:

| MQTT wildcard | KubeMQ wildcard | Semantics |
|---------------|-----------------|-----------|
| `+` | `*` | Matches exactly one segment |
| `#` | `>` | Matches one or more trailing segments (must be last) |

Examples:

| MQTT filter | Matches |
|-------------|---------|
| `events/demo/+` | `events/demo/x`, `events/demo/y` — not `events/demo/a/b` |
| `events/demo/#` | `events/demo/x`, `events/demo/a/b/c`, … |
| `#` | All events topics (bare wildcard = DefaultPattern `events`) |

Wildcard subscribe on a non-Events prefix returns SUBACK `0xA2` (wildcard subscriptions not
supported):

```
SUBSCRIBE  filter=store/#  QoS=1
SUBACK     reason=0xA2   (wildcard rejected on non-events pattern)
```

> **Gotcha #6 — overlapping wildcard filters deliver one copy per matching bridge registry entry.**
> Each distinct subscribe filter creates an independent bridge registry entry. If N of your
> active filters match the same publish, the message is routed through the KubeMQ array N times
> and you receive **N copies** — there is no cross-entry dedup in V1.
>
> **Example (verified by integration test #25 `QueueNoWildcardLeak`):**
> Three overlapping filters are active on the same client: `#`, `events/leak/x`, and
> `events/#`. A single publish to `events/leak/x` generates **3 copies** to each matching
> subscriber — one per matching bridge entry.
>
> Design guidance: use a single, specific filter per subscription to avoid duplicate delivery.

## MQTT 5.0 User Properties ↔ KubeMQ Tags

On MQTT 5.0 connections, `PUBLISH.Properties.User` entries are mapped bidirectionally to/from
KubeMQ message `Tags`:

| Direction | Mapping |
|-----------|---------|
| Inbound (MQTT 5.0 publish → KubeMQ) | `Properties.User` copied to `Tags` (duplicate keys: last wins) |
| Outbound (KubeMQ → MQTT 5.0 subscriber) | `Tags` copied to `Properties.User` on delivery |

**Caps:**

| Limit | Value |
|-------|-------|
| Maximum user properties per message | 32 |
| Maximum total bytes (all key + value lengths) | 4096 |

Exceeding either cap on a v5 PUBLISH returns PUBACK `0x97` and the message is dropped.
On a v3.1.1 connection there are no user properties — nothing is carried in either direction.

## Fan-out Semantics

- All subscribers with a matching filter receive every message (fan-out).
- There is no load-balancing group for Events — use Queues if you need competing consumers.
- Messages are **not persisted**: a subscriber that is offline at delivery time misses the message.

## Retain

> **Gotcha #1 — retain is silently dropped.**
> `RetainAvailable=0` is forced by the connector. If you set the RETAIN flag on a runtime
> PUBLISH, the broker strips it silently. The PUBACK still returns `0x00` (success), but the
> message is dropped — it is not delivered and not stored. No DISCONNECT is issued. The only
> retain-related error is a CONNACK `0x9A` for Will-retain at CONNECT.
>
> Verified by integration test #15 `RetainRejected`.

## Prefixless Topics and DefaultPattern

Topics published without a recognized prefix are routed via `DefaultPattern` (default `events`).
Publishing to `demo/x` behaves identically to `events/demo/x` when `DefaultPattern=events`.

If `DefaultPattern=none`, prefixless publishes return PUBACK `0x90` and prefixless subscribes
return SUBACK `0x8F`.

## Connection Parameters (quick reference)

```
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883   # TCP (default)
                ws://127.0.0.1:8083/   # WebSocket
                tls://127.0.0.1:8883   # TLS (requires Security config)
```

KeepAlive: 30 s (recommended). Sessions are in-memory and node-local; `clean_session=false`
reconnect on the same node restores subscriptions.

## Examples

| Language | basic-pubsub | wildcard-subscribe |
|----------|--------------|--------------------|
| Go | [examples/go/events/basic-pubsub/](../../examples/go/events/basic-pubsub/) | [examples/go/events/wildcard-subscribe/](../../examples/go/events/wildcard-subscribe/) |
| Python | [examples/python/events/basic_pubsub/](../../examples/python/events/basic_pubsub/) | [examples/python/events/wildcard_subscribe/](../../examples/python/events/wildcard_subscribe/) |
| JavaScript | [examples/javascript/events/basic-pubsub/](../../examples/javascript/events/basic-pubsub/) | [examples/javascript/events/wildcard-subscribe/](../../examples/javascript/events/wildcard-subscribe/) |
| Java | [examples/java/events/basic-pubsub/](../../examples/java/events/basic-pubsub/) | [examples/java/events/wildcard-subscribe/](../../examples/java/events/wildcard-subscribe/) |
| C# | [examples/csharp/events/basic-pubsub/](../../examples/csharp/events/basic-pubsub/) | [examples/csharp/events/wildcard-subscribe/](../../examples/csharp/events/wildcard-subscribe/) |
| Ruby | [examples/ruby/events/basic_pubsub/](../../examples/ruby/events/basic_pubsub/) | [examples/ruby/events/wildcard_subscribe/](../../examples/ruby/events/wildcard_subscribe/) |
| Rust | [examples/rust/events/basic-pubsub/](../../examples/rust/events/basic-pubsub/) | [examples/rust/events/wildcard-subscribe/](../../examples/rust/events/wildcard-subscribe/) |

## Related Docs

- [Topic Grammar reference](../reference/topic-grammar.md)
- [Reason Codes reference](../reference/reason-codes.md)
- [Guide: Topic Mapping](../guides/topic-mapping.md)
- [Guide: Protocol Versions](../guides/protocol-versions.md) — User Properties require MQTT 5.0
- [Events-Store pattern](events-store.md) — persistent variant (StartNewOnly over MQTT)
