# rust — Queues: Shared-Subscription Consume

Demonstrates MQTT 5.0 shared subscription (`$share/<group>/queues/<ch>`) for consuming
from a KubeMQ Queue using `rumqttc` (v5 async client).

The example shows a full round-trip: produce one order message to `queues/demo/q`,
then consume it via `$share/g1/queues/demo/q` at QoS 1. PUBACK is sent automatically
by `rumqttc`, which acks and removes the message from the queue.

## Prerequisites

- Rust 1.75+ (stable)
- KubeMQ server with MQTT connector enabled (default port 1883)

## How to Run

```bash
cd examples/rust
KUBEMQ_MQTT_URL=tcp://localhost:1883 cargo run -p shared-consume
```

## Expected Output

> Regenerated from a live run in the LiveTest stage. Topic strings below are canonical.

```
Connecting to broker: tcp://localhost:1883
[consumer] Connecting and subscribing to $share/g1/queues/demo/q at QoS 1...
[consumer] Subscription active
[producer] Connecting...
[producer] Publishing to queues/demo/q (QoS 1)...
[producer] Published: {"order_id":"ORD-001","item":"widget","qty":3}
[consumer] Received on '$share/g1/queues/demo/q': {"order_id":"ORD-001","item":"widget","qty":3}
[consumer] PUBACK sent automatically -> message acked
QUEUE_CONSUMED
Round-trip complete. Received: {"order_id":"ORD-001","item":"widget","qty":3}
```

## What's Happening

1. **Connect (consumer)** — a dedicated MQTT 5.0 consumer client connects with
   `clean_start=false` so the session persists across reconnects on the same broker node
   (sessions are in-memory node-local).
2. **Subscribe** — consumer subscribes to `$share/g1/queues/demo/q` at QoS 1
   *before* producing, so no messages are missed. The event loop is driven in a background
   task while the main task waits for the SUBACK to settle (400 ms).
3. **Connect (producer)** — a separate producer client connects. Using separate clients
   avoids sharing the same event loop buffer between produce and consume paths.
4. **Produce** — producer publishes to `queues/demo/q` at QoS 1; the KubeMQ bridge
   routes it into the Queues channel `demo.q` (MQTT `/` becomes `.` in KubeMQ channels).
5. **Consume** — consumer receives the message; `rumqttc` sends PUBACK automatically.
   PUBACK = ack: the message is removed from the KubeMQ queue.
6. **Exit** — the example disconnects both clients and exits cleanly.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 (required for shared subscriptions) |
| Produce topic | `queues/demo/q` |
| Consume topic | `$share/g1/queues/demo/q` |
| QoS | 1 (AtLeastOnce) |
| `clean_start` | `false` (persistent session) |
| KubeMQ pattern | Queues |
| KubeMQ channel | `demo.q` (`/` mapped to `.`) |

## Gotchas

**`$share` group name is audit/metrics-only.**
The group name `g1` appears in audit logs and metrics, but ALL groups compete in
ONE shared KubeMQ queue pool — there are no per-group message copies.
If you subscribe with both `$share/g1/queues/demo/q` and `$share/g2/queues/demo/q`,
both consumers pull from the same pool; a single message is delivered to exactly one of them.

**QoS 0 is rejected.**
Subscribing to `$share/.../queues/...` at QoS 0 returns SUBACK `0x83` (implementation-specific error).
Always use QoS 1 or QoS 2 for queue subscriptions.

**Plain subscribe is rejected.**
Subscribing directly to `queues/demo/q` (without the `$share/` prefix) returns
SUBACK `0x83`. The `$share/<group>/` prefix is mandatory.

**Ack timeout and redelivery.**
No PUBACK within the default `QueueAckTimeoutSeconds` (30 s) causes the broker to
redeliver the message. Disconnecting with unacked messages causes immediate requeue.
See [queues/ack-and-redelivery](../ack-and-redelivery/) for a focused demonstration.

**Retain flag is silently dropped.**
Publishing with `retain=true` is silently stripped by the KubeMQ MQTT connector
(PUBACK `0x00` is still returned, but the message is dropped, not stored).
Always publish with `retain=false`.

## Related Examples

- [queues/ack-and-redelivery](../ack-and-redelivery/) — ack timeout and redelivery behaviour
- [events/basic-pubsub](../../events/basic-pubsub/) — non-queue fire-and-forget events
- [events-store/start-new-only](../../events-store/start-new-only/) — durable events (always StartNewOnly over MQTT)
