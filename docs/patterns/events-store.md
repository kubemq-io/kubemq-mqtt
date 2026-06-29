# Events-Store Pattern

## Overview

Events-Store provides **persistent pub/sub**: messages are durably stored by KubeMQ and
delivered to subscribers. Over MQTT the subscription start position is **always `StartNewOnly`**
— there is no historical replay. A subscriber that connects after a message was published will
**not** receive that message; only messages published after the subscription is established are
delivered.

| Property | Value |
|----------|-------|
| KubeMQ pattern | Events-Store |
| MQTT topic prefix | `store/<channel-path>` |
| Direction | publish + subscribe |
| Wildcard subscribe | Not supported (returns SUBACK `0xA2`) |
| Minimum QoS | 0 (any QoS) |
| MQTT 5.0 required | No (works on 3.1.1 and 5.0) |
| Start position | Always `StartNewOnly` — **no replay** |
| User Properties → KubeMQ Tags | v5 only |

> **Gotcha #2 — Events-Store over MQTT is always StartNewOnly. There is no historical replay.**
> The MQTT connector hard-codes `StartNewOnly` as the subscribe start position (verified by
> integration test #4 `EventsStoreStartNewOnly`). Messages published before the subscription was
> established are **never delivered**, regardless of how many are stored in KubeMQ. If your
> application requires replay (StartFromFirst, StartAtSequence, StartAtTime, etc.), use the
> KubeMQ gRPC or REST API instead.

## Difference from Events

| Aspect | Events | Events-Store |
|--------|--------|--------------|
| Persistence | No | Yes — messages stored in KubeMQ |
| Offline delivery | Missed | Delivered when subscriber connects (after publish time) |
| Replay over MQTT | N/A | Not available (StartNewOnly only) |
| MQTT topic prefix | `events/` | `store/` |
| Wildcard subscribe | Supported | Not supported (SUBACK `0xA2`) |

## Topic Grammar

```
MQTT topic             KubeMQ channel
store/demo/s    →      demo.s
store/audit/log →      audit.log
store/alerts    →      alerts
```

The `store/` prefix is stripped; the remaining path segments are joined with `.`.

> **Gotcha #5 — literal `.` conflates with `/`.**
> `store/a.b/c` and `store/a/b/c` both map to channel `a.b.c`.

## Publishing

Publish to `store/<channel-path>`. The message is persisted by KubeMQ before any delivery.

```
PUBLISH  topic=store/demo/s  QoS=1  payload=<bytes>
PUBACK   reason=0x00  (success; message stored)
```

Common failure codes on publish:

| PUBACK reason | Cause |
|---------------|-------|
| `0x80` | Broker not yet ready |
| `0x87` | ACL deny on this channel |
| `0x97` | User-Properties cap exceeded (v5, >32 props or >4096 bytes total) |

## Subscribing

Subscribe to `store/<channel-path>`. The connector registers a `StartNewOnly` subscription
with the KubeMQ array immediately on SUBACK.

```
SUBSCRIBE  filter=store/demo/s  QoS=1
SUBACK     reason=0x01  (granted QoS 1)
```

Messages published **before** the SUBSCRIBE handshake completes are not delivered. Only messages
published after the subscription is active arrive on this connection.

Wildcard filters on `store/` topics are rejected:

```
SUBSCRIBE  filter=store/#  QoS=1
SUBACK     reason=0xA2   (wildcard not supported on non-events pattern)
```

## Delivery Guarantee and StartNewOnly

Integration test #4 (`EventsStoreStartNewOnly`) verifies the exact behavior:

1. Publish N messages to `store/it/s` **before** subscribing.
2. Subscribe to `store/it/s`.
3. Publish 1 message **after** subscribing.
4. Assert: only the post-subscribe message is delivered; the N pre-subscribe messages are never received.

This is the only supported behavior over MQTT. There is no way to configure a different start
position through MQTT topic syntax or MQTT 5.0 properties.

## MQTT 5.0 User Properties ↔ KubeMQ Tags

Same mapping as Events:

| Direction | Mapping |
|-----------|---------|
| Inbound (MQTT 5.0 publish → KubeMQ) | `Properties.User` copied to `Tags` |
| Outbound (KubeMQ → MQTT 5.0 subscriber) | `Tags` copied to `Properties.User` on delivery |

**Caps:** 32 properties max, 4096 bytes total. Exceeding either cap returns PUBACK `0x97`.
MQTT 3.1.1 carries no user properties.

## Retain

> **Gotcha #1 — retain is silently dropped.**
> Setting the RETAIN flag on a `store/` publish still returns PUBACK `0x00` but the message
> is dropped. See [Events pattern](events.md#retain) for the full explanation.

## Connection Parameters (quick reference)

```
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883   # TCP (default)
                ws://127.0.0.1:8083/   # WebSocket
                tls://127.0.0.1:8883   # TLS
```

## Examples

| Language | start-new-only |
|----------|---------------|
| Go | [examples/go/events-store/start-new-only/](../../examples/go/events-store/start-new-only/) |
| Python | [examples/python/events_store/start_new_only/](../../examples/python/events_store/start_new_only/) |
| JavaScript | [examples/javascript/events-store/start-new-only/](../../examples/javascript/events-store/start-new-only/) |
| Java | [examples/java/events-store/start-new-only/](../../examples/java/events-store/start-new-only/) |
| C# | [examples/csharp/events-store/start-new-only/](../../examples/csharp/events-store/start-new-only/) |
| Ruby | [examples/ruby/events_store/start_new_only/](../../examples/ruby/events_store/start_new_only/) |
| Rust | [examples/rust/events-store/start-new-only/](../../examples/rust/events-store/start-new-only/) |

## Related Docs

- [Events pattern](events.md) — non-persistent fire-and-forget with wildcard support
- [Topic Grammar reference](../reference/topic-grammar.md)
- [Reason Codes reference](../reference/reason-codes.md)
- [Guide: Protocol Versions](../guides/protocol-versions.md)
