# java — Queues: Shared-Subscription Consume

Demonstrates KubeMQ Queues consumption over MQTT 5.0 using shared subscriptions
with two competing consumers. Each message is delivered to exactly one consumer;
PUBACK acknowledges and removes the message from the queue.

## Prerequisites

- Java 21+ and Maven 3.8+
- KubeMQ server with the MQTT connector enabled (default port 1883)
- `KUBEMQ_MQTT_URL` pointing at the broker (see below)

## How to Run

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883   # default: tcp://localhost:1883
cd examples/java/queues/shared-consume
mvn compile exec:java
```

## Expected Output

```
[shared-consume] broker  : tcp://127.0.0.1:1883
[shared-consume] produce : queues/demo/q
[shared-consume] consume : $share/g1/queues/demo/q (QoS 1)

[consumer-1] connected  : tcp://127.0.0.1:1883
[consumer-1] subscribed : $share/g1/queues/demo/q (granted QoS=1)
[consumer-2] connected  : tcp://127.0.0.1:1883
[consumer-2] subscribed : $share/g1/queues/demo/q (granted QoS=1)
[producer] publishing 4 messages -> queues/demo/q
[producer]  connected  : tcp://127.0.0.1:1883
[producer] published  : order-1
[producer] published  : order-2
[producer] published  : order-3
[producer] published  : order-4
[consumer-2] received : order-2 (msgId=1)
[consumer-1] received : order-1 (msgId=1)
[consumer-2] received : order-4 (msgId=2)
[consumer-1] received : order-3 (msgId=2)

[shared-consume] all 4 messages received (4 total)
[shared-consume] done.
```

The exact distribution across consumer-1 and consumer-2 — and the interleaving
of the `received` lines — depends on KubeMQ's internal scheduling and async
delivery timing; the total delivered count is always equal to the number of
messages produced. The `msgId` is the per-client Paho packet id, so each
consumer numbers its own deliveries from 1.

## What's Happening (step by step)

1. **Two consumers connect** using MQTT 5.0 (Paho `mqttv5` client), each with a
   unique client ID to avoid session collision.
2. **Both consumers subscribe** to `$share/g1/queues/demo/q` at QoS 1. The
   `$share/<group>/queues/<channel>` format is the only valid way to consume
   KubeMQ Queues over MQTT; a plain `queues/...` subscribe returns SUBACK `0x83`.
3. **A producer connects** and publishes 4 messages to `queues/demo/q` at QoS 1.
   The `queues/` prefix routes the publish into the KubeMQ Queues pattern. The
   `/` → `.` mapping converts `demo/q` to channel `demo.q` inside KubeMQ.
4. **KubeMQ distributes** each message to exactly one consumer from the shared pool.
5. **Auto-ack via PUBACK**: the Paho library sends PUBACK automatically when
   `messageArrived` returns. The PUBACK is the acknowledgement that removes the
   message from the queue. If no PUBACK arrives within `QueueAckTimeoutSeconds`
   (default 30 s), KubeMQ NAcks the message and redelivers it to another consumer.
6. **Clean disconnect**: both consumers and the producer are disconnected cleanly
   after all messages are received.

## MQTT Specifics

| Property | Value |
|---|---|
| Protocol | MQTT 5.0 (required; v3.1.1 cannot use shared subscriptions) |
| Producer topic | `queues/demo/q` |
| Consumer topic | `$share/g1/queues/demo/q` |
| QoS | 1 (QoS 0 shared subscribe -> SUBACK `0x83`; test #8) |
| Ack mechanism | PUBACK (auto-ack; Paho sends after callback returns) |
| Ack timeout | 30 s default (`QueueAckTimeoutSeconds`) — no PUBACK -> redeliver |
| Retain | Not set (`RetainAvailable=0`; retain-flagged publish silently dropped) |
| MQTT 5.0 User Properties | Not used in this example (tags require v5 user properties) |
| Negative path: plain subscribe | `queues/demo/q` subscribe -> SUBACK `0x83` (test #9) |
| Negative path: QoS 0 | `$share/g1/queues/demo/q` at QoS 0 -> SUBACK `0x83` (test #8) |

## Gotcha: `$share` Group Is One Competing Pool

The group name in `$share/<group>/...` (here `g1`) is **audit and metrics only**.
All subscribers — regardless of their group name — compete in a **single shared
KubeMQ queue pool**. There are **no per-group message copies**.

This differs from standard MQTT 5.0 broker semantics (where different group names
typically receive independent copies). If you subscribe two groups `g1` and `g2`
to the same channel, they still share one pool — each message goes to exactly one
subscriber across both groups combined.

## Related Examples

- [queues/ack-and-redelivery](../ack-and-redelivery/) — manual ack, disconnect-requeue, ack-timeout redelivery
- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget fan-out (every subscriber gets a copy)
- [events-store/start-new-only](../../events-store/start-new-only/) — persistent events (StartNewOnly, no replay)
