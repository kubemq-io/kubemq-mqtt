# csharp — Queues: Shared-Subscription Consume

Produce messages to a KubeMQ Queue and consume them via an MQTT 5.0 shared subscription (`$share/<group>/queues/<ch>`), demonstrating the full round-trip including acknowledgement via PUBACK.

## Prerequisites

- .NET 8+
- KubeMQ server with MQTT connector enabled (default `tcp://localhost:1883`)

## How to Run

```bash
cd examples/csharp/queues/shared-consume
dotnet run
# or with a custom broker URL
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run
```

## Expected Output

```
Consumer connected (127.0.0.1:1883)
Subscribed with shared filter '$share/g1/queues/demo/q'
Produced message 1
Produced message 2
Produced message 3
Consumed: {"seq":1,"msg":"queue message 1","ts":"..."}
Consumed: {"seq":2,"msg":"queue message 2","ts":"..."}
Consumed: {"seq":3,"msg":"queue message 3","ts":"..."}
All 3 messages consumed.
```

## What's Happening

1. **Consumer connects** with MQTT 5.0 (`MqttProtocolVersion.V500`) and a clean session.
2. **Consumer subscribes** to `$share/g1/queues/demo/q` at QoS 1. The `$share/<group>/` prefix is the MQTT 5.0 shared-subscription syntax required by the KubeMQ connector; a plain `queues/...` subscribe returns SUBACK `0x83` and is rejected.
3. **Producer connects** (separate MQTT client) and publishes 3 JSON messages to `queues/demo/q` at QoS 1. The connector routes the `queues/` prefix to the KubeMQ Queues pattern; `demo/q` becomes channel `demo.q`.
4. **Consumer receives** each message via the `ApplicationMessageReceivedAsync` handler. MQTTnet automatically sends PUBACK when the handler returns, which the connector treats as the queue acknowledgement — the message is removed from the queue.
5. **`TaskCompletionSource<bool>`** signals completion once all 3 messages are received; the program exits cleanly after both clients disconnect.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 (required — queue consume is not available on 3.1.1) |
| Produce topic | `queues/demo/q` |
| Consume filter | `$share/g1/queues/demo/q` |
| QoS | 1 (AtLeastOnce) — QoS 0 shared-sub returns SUBACK `0x83` |
| MQTT 5.0 properties | None required for this basic flow |
| KubeMQ channel | `demo.q` (`/` becomes `.`) |
| Ack mechanism | PUBACK (sent automatically by MQTTnet on handler return) |

## Gotchas

**`$share` group name is audit/metrics-only — not per-group copies.**
Unlike standard MQTT broker shared subscriptions where each group gets its own copy of each message, the KubeMQ connector maps ALL `$share` groups to a SINGLE shared queue pool. Two consumers subscribed as `$share/groupA/queues/ch` and `$share/groupB/queues/ch` **compete** for the same messages — they do not each receive a separate copy. The group name is recorded for observability only.

**QoS 0 is rejected.**
Subscribing with `$share/<g>/queues/<ch>` at QoS 0 returns SUBACK reason code `0x83` (implementation-specific error). Always use QoS 1 or QoS 2.

**Plain `queues/...` subscribe is rejected.**
Subscribing directly to `queues/demo/q` (without the `$share/` prefix) returns SUBACK `0x83`. Queue consumption requires the shared-subscription syntax.

**No PUBACK within 30 s triggers redelivery.**
If the consumer's handler does not return (and therefore PUBACK is not sent) within `QueueAckTimeoutSeconds` (default 30 s), the broker redelivers the message. Disconnecting before PUBACK triggers **immediate** requeue. See [queues/ack-and-redelivery](../ack-and-redelivery/) for a demonstration.

## Related Examples

- [queues/ack-and-redelivery](../ack-and-redelivery/) — ack timeout and redelivery guarantee
