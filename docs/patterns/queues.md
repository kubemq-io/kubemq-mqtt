# Queues Pattern

## Overview

Queues provide **point-to-point, durable** messaging. Messages are stored until consumed.
Each message is delivered to exactly one consumer (competing-consumer semantics).

Over MQTT there are two separate operations:

- **Produce** — plain PUBLISH to `queues/<channel-path>` (any MQTT version, any QoS ≥ 0).
- **Consume** — MQTT 5.0 shared subscription `$share/<group>/queues/<channel-path>` at QoS ≥ 1.

| Property | Value |
|----------|-------|
| KubeMQ pattern | Queues |
| Produce topic | `queues/<channel-path>` (plain publish) |
| Consume topic | `$share/<group>/queues/<channel-path>` |
| Consume: minimum QoS | 1 (QoS 0 → SUBACK `0x83`) |
| Consume: MQTT version | MQTT 5.0 only — v3.1.1 has no shared-subscription support |
| Wildcard subscribe | Not supported (SUBACK `0xA2` on wildcard; `0x83` on plain queues/ subscribe) |
| User Properties → KubeMQ Tags | v5 only |

## Topic Grammar

```
Produce (plain publish):
  queues/jobs/email  →  channel  jobs.email

Consume (shared subscription):
  $share/workers/queues/jobs/email  →  channel  jobs.email  (group  workers)
```

The `queues/` prefix selects the Queues pattern; `/` → `.` maps the channel path.

> **Gotcha #5 — literal `.` conflates with `/`.**
> `queues/a.b/c` and `queues/a/b/c` both map to channel `a.b.c`.

## Producing (Publishing to a Queue)

Publish to `queues/<channel-path>`. Any MQTT version (3.1.1 or 5.0), any QoS. The message is
enqueued durably in KubeMQ.

```
PUBLISH  topic=queues/jobs/email  QoS=1  payload=<bytes>
PUBACK   reason=0x00  (message enqueued)
```

**Produce failure codes:**

| PUBACK reason | Cause |
|---------------|-------|
| `0x80` | Broker not yet ready |
| `0x87` | ACL deny on this channel |
| `0x97` | User-Properties cap exceeded (v5, >32 props or >4096 bytes total) |

## Consuming via Shared Subscription

MQTT consumers use the MQTT 5.0 shared-subscription syntax:

```
SUBSCRIBE  filter=$share/<group>/queues/<channel-path>  QoS=1
SUBACK     reason=0x01  (granted QoS 1)
```

Example:

```
SUBSCRIBE  filter=$share/workers/queues/jobs/email  QoS=1
SUBACK     reason=0x01
```

On message delivery the broker sends a PUBLISH; the consumer sends PUBACK to acknowledge:

```
← PUBLISH  topic=queues/jobs/email  QoS=1  payload=<bytes>
→ PUBACK   reason=0x00
```

**Consume failure codes at SUBSCRIBE time:**

| SUBACK reason | Cause |
|---------------|-------|
| `0x83` | Plain `queues/` subscribe (non-`$share`), QoS 0 shared subscribe, or `$share` on a non-queues pattern |
| `0xA2` | Wildcard in a queues filter |

Plain subscribe (without `$share`) is rejected:

```
SUBSCRIBE  filter=queues/jobs/email  QoS=1
SUBACK     reason=0x83   (impl-specific error — must use $share syntax)
```

QoS 0 shared subscribe is rejected:

```
SUBSCRIBE  filter=$share/workers/queues/jobs/email  QoS=0
SUBACK     reason=0x83   (QoS 0 not allowed for queue consume)
```

## Ack Model and Redelivery

The connector uses an **ack-on-PUBACK** model:

| Event | Outcome |
|-------|---------|
| Consumer sends PUBACK within `QueueAckTimeoutSeconds` (default 30 s) | Message acknowledged — removed from queue |
| No PUBACK within timeout | NAck → message redelivered to another consumer |
| Consumer disconnects with unacked message | Immediate NAck → message requeued immediately (no loss) |

### Ack-on-PUBACK flow

```
Server → PUBLISH  QoS=1  packet-id=5  payload=<bytes>
Client →  PUBACK  packet-id=5  reason=0x00     (acknowledges; message consumed)
```

### Timeout / NAck flow

```
Server → PUBLISH  QoS=1  packet-id=5  payload=<bytes>
  … no PUBACK within QueueAckTimeoutSeconds …
Server re-delivers to another consumer
```

### Disconnect-requeue flow

```
Server → PUBLISH  QoS=1  packet-id=5  payload=<bytes>
Client disconnects (TCP close / DISCONNECT without PUBACK)
Server → immediate NAck → message requeued → delivered to next available consumer
```

Verified by integration tests #6 (`QueueRedeliveryOnDisconnect`) and #7 (`QueueAckTimeout`).

## The `$share` Group Name is Audit/Metrics-Only

> **Gotcha #3 — ALL `$share` groups compete in ONE shared KubeMQ queue pool.**
> Unlike standard MQTT shared subscriptions, the `<group>` name in `$share/<group>/queues/<ch>`
> does NOT create per-group copies of the message. All consumers across ALL group names compete
> in a single KubeMQ queue pool. A message produced to `queues/jobs/email` is consumed by
> exactly one MQTT subscriber regardless of which `$share` group name they used.
>
> The group name is recorded in the connection metadata (`/api/mqtt/connections`) and in
> Prometheus metrics for observability purposes only. It has no effect on message routing.
>
> If you need per-group fan-out (each group gets its own copy), use the Events or Events-Store
> pattern instead.

## Multiple Consumers (Load Balancing)

Multiple subscribers with `$share/<any-group>/queues/<ch>` compete for messages in the same
pool. Each message goes to exactly one subscriber:

```
# Consumer A
SUBSCRIBE  $share/g1/queues/jobs/email  QoS=1

# Consumer B (different group name, but same pool)
SUBSCRIBE  $share/g2/queues/jobs/email  QoS=1

# Produce
PUBLISH    queues/jobs/email  QoS=1  payload=msg1   → delivered to A or B (not both)
PUBLISH    queues/jobs/email  QoS=1  payload=msg2   → delivered to the other one
```

## MQTT 5.0 User Properties ↔ KubeMQ Tags

Same as Events (v5 only):

| Direction | Mapping |
|-----------|---------|
| Producer PUBLISH `Properties.User` | Copied to KubeMQ message `Tags` |
| KubeMQ `Tags` on delivery | Copied to consumer `Properties.User` |

**Caps:** 32 properties max, 4096 bytes total (PUBACK `0x97` if exceeded).

## Observing Queue Consumers

```http
GET http://127.0.0.1:8080/api/mqtt/connections
```

Response envelope (verified `pkg/api/mqtt_metrics.go`):

```json
{
  "error": false,
  "error_string": "",
  "data": {
    "connections": [
      {
        "client_id": "worker-1",
        "protocol_version": 5,
        "remote_addr": "127.0.0.1:54321",
        "connected_at": "2026-06-12T10:00:00Z",
        "clean_session": false,
        "username": "",
        "subscriptions": [
          {
            "filter": "$share/g1/queues/jobs/email",
            "qos": 1,
            "pattern": "queues",
            "channel": "jobs.email",
            "shared_group": "g1"
          }
        ]
      }
    ],
    "total": 1
  }
}
```

## Connection Parameters (quick reference)

```
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883   # TCP (default)
                ws://127.0.0.1:8083/   # WebSocket
                tls://127.0.0.1:8883   # TLS
```

Consumers should use `clean_session=false` (MQTT 3.1.1) / `session_expiry_interval > 0`
(MQTT 5.0) so that the subscription survives a brief reconnect without requiring a
re-SUBSCRIBE.

## Examples

| Language | shared-consume | ack-and-redelivery |
|----------|----------------|--------------------|
| Go | [examples/go/queues/shared-consume/](../../examples/go/queues/shared-consume/) | [examples/go/queues/ack-and-redelivery/](../../examples/go/queues/ack-and-redelivery/) |
| Python | [examples/python/queues/shared_consume/](../../examples/python/queues/shared_consume/) | [examples/python/queues/ack_and_redelivery/](../../examples/python/queues/ack_and_redelivery/) |
| JavaScript | [examples/javascript/queues/shared-consume/](../../examples/javascript/queues/shared-consume/) | [examples/javascript/queues/ack-and-redelivery/](../../examples/javascript/queues/ack-and-redelivery/) |
| Java | [examples/java/queues/shared-consume/](../../examples/java/queues/shared-consume/) | [examples/java/queues/ack-and-redelivery/](../../examples/java/queues/ack-and-redelivery/) |
| C# | [examples/csharp/queues/shared-consume/](../../examples/csharp/queues/shared-consume/) | [examples/csharp/queues/ack-and-redelivery/](../../examples/csharp/queues/ack-and-redelivery/) |
| Ruby | N/A (v3.1.1 only; shared-subscribe requires v5) | N/A |
| Rust | [examples/rust/queues/shared-consume/](../../examples/rust/queues/shared-consume/) | [examples/rust/queues/ack-and-redelivery/](../../examples/rust/queues/ack-and-redelivery/) |

Ruby note: the `mqtt` gem is MQTT 3.1.1 only and has no shared-subscription support. Queue
consume is not available from Ruby. Queue produce (plain PUBLISH to `queues/<ch>`) does work
from Ruby because it requires no v5 feature.

## Related Docs

- [Topic Grammar reference](../reference/topic-grammar.md)
- [Reason Codes reference](../reference/reason-codes.md)
- [Capabilities reference](../reference/capabilities.md) — `QueueAckTimeoutSeconds`
- [Guide: QoS and Sessions](../guides/qos-and-sessions.md)
- [Guide: Protocol Versions](../guides/protocol-versions.md) — shared subscriptions are v5-only
- [Connections Endpoint](../reference/connections-endpoint.md)
