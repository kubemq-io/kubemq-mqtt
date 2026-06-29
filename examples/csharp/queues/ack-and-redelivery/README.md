# csharp — Queues: Ack & Redelivery

Demonstrates at-least-once delivery semantics for KubeMQ Queues over MQTT 5.0.

A queue message is not removed from the broker until the consumer sends a PUBACK (QoS 1 acknowledgement). This example shows both redelivery triggers:

- **Fast path — disconnect without ack**: Consumer 1 receives the message, suppresses the automatic PUBACK, then disconnects. KubeMQ immediately NAcks and requeues the message. Consumer 2 picks it up and acks it.
- **Slow path — ack-timeout**: If the consumer stays connected but never calls `AcknowledgeAsync()`, the broker waits `QueueAckTimeoutSeconds` (default 30 s) before NAcking and redelivering.

## Prerequisites

- .NET 8 or later
- KubeMQ server with MQTT connector enabled (default `tcp://localhost:1883`)

## How to Run

```bash
cd examples/csharp/queues/ack-and-redelivery
dotnet run
# or with a custom broker URL:
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run
```

## Expected Output

```
Step 1: Producing one queue message...
  Produced: {"msg":"ack-test","ts":"..."}

Step 2: Consumer 1 — receives message WITHOUT acking, then disconnects...
  Consumer 1 subscribed: $share/g1/queues/demo/rq
  Consumer 1 received (NOT acked): {"msg":"ack-test","ts":"..."}
  Consumer 1 disconnecting (no PUBACK) -> message requeued immediately...

Step 3: Consumer 2 — receives redelivered message and acks it (PUBACK sent)...
  Consumer 2 subscribed: $share/g1/queues/demo/rq
  Consumer 2 received (will ack): {"msg":"ack-test","ts":"..."}
  Consumer 2 sent PUBACK — message fully acked and removed from queue.

Done. At-least-once delivery demonstrated:
  - Consumer 1 received the message but did not ack.
  - Disconnect without PUBACK -> immediate requeue (fast path).
  - Consumer 2 received the redelivered message and acked it.
  - Slow path: no PUBACK within QueueAckTimeoutSeconds (30s) also triggers redelivery.
```

## What's Happening

1. **Produce**: a message is published to `queues/demo/rq` at QoS 1. The broker returns PUBACK `0x00` and stores the message in the queue.
2. **Consumer 1 subscribes** using `$share/g1/queues/demo/rq` at QoS 1. The broker delivers the message.
3. **Manual ack suppressed**: `e.AutoAcknowledge = false` prevents MQTTnet from sending PUBACK automatically. The message remains "in-flight" on the broker.
4. **Consumer 1 disconnects**: the broker detects an unacked in-flight message and immediately NAcks it, requeueing the message (no message loss).
5. **Consumer 2 subscribes** to the same filter. The broker redelivers the now-requeued message.
6. **Consumer 2 acks**: `e.AcknowledgeAsync()` sends PUBACK. The broker removes the message from the queue. Delivery is complete.

## MQTT Specifics

| Aspect | Value |
|--------|-------|
| Produce topic | `queues/demo/rq` |
| Consume filter | `$share/g1/queues/demo/rq` |
| QoS | 1 (AtLeastOnce) — required for queue consume; QoS 0 -> SUBACK `0x83` |
| MQTT version | 5.0 (required for shared subscriptions) |
| MQTT 5.0 properties | None on produce; standard shared-sub filter on consume |
| Auto-ack suppression | `MqttApplicationMessageReceivedEventArgs.AutoAcknowledge = false` |
| Manual ack | `MqttApplicationMessageReceivedEventArgs.AcknowledgeAsync()` |
| Ack-timeout (slow path) | `QueueAckTimeoutSeconds` — default 30 s; server-side config |

### Gotchas

- **Plain `queues/...` subscribe** (without `$share/...`) returns SUBACK `0x83` (implementation-specific error) and delivers no messages. Always use the `$share/<group>/queues/<channel>` filter.
- **QoS 0 shared subscribe** also returns SUBACK `0x83`. QoS must be 1 or 2.
- **`$share` group name is audit/metrics-only.** All groups (`g1`, `g2`, etc.) compete in a single shared KubeMQ queue pool — this is NOT the MQTT broker-level per-group-copy semantics. Two different group names do NOT produce two independent copies of each message.
- **Retain is silently stripped.** Publishing with the retain flag set returns PUBACK `0x00` but the message is dropped and never delivered. Will-retain at CONNECT returns CONNACK `0x9A`.

## Related Examples

- [queues/shared-consume](../shared-consume/) — basic shared-consume flow without manual ack
