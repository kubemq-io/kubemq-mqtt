# python — Queues: Ack & Redelivery

Demonstrates KubeMQ Queues at-least-once semantics over MQTT 5.0: a consumer
receives a message and disconnects without sending a PUBACK, which triggers an
immediate requeue and redelivery to a waiting survivor consumer.

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` (from `examples/python/`)
- KubeMQ server with MQTT connector enabled
- **MQTT 5.0 required** — `$share` queue subscriptions require `MQTTv5`

## How to Run

```bash
cd examples/python
export KUBEMQ_MQTT_URL=tcp://localhost:1883
python queues/ack_and_redelivery/main.py
```

## Expected Output

```
Broker: tcp://127.0.0.1:1883
Produce topic : queues/demo/rq
Consume filter: $share/g1/queues/demo/rq  (QoS 1, MQTT 5.0 required)

[consumer-A] subscribed to $share/g1/queues/demo/rq (QoS 1, manual-ack)
[producer] published to queues/demo/rq: {"task": "redelivery-demo", "attempt": 1}
[consumer-A] received (NOT acked): {"task": "redelivery-demo", "attempt": 1}
[consumer-B] subscribed to $share/g1/queues/demo/rq (QoS 1)
[consumer-A] disconnected (unacked) — message requeued for redelivery
             Alternatively: QueueAckTimeoutSeconds (default 30 s) also triggers requeue.
[consumer-B] received redelivered: {"task": "redelivery-demo", "attempt": 1}
[consumer-B] PUBACK sent — message acked and removed from queue.

Done. At-least-once delivery confirmed:
  A received : {"task": "redelivery-demo", "attempt": 1}
  B received : {"task": "redelivery-demo", "attempt": 1}
```

Captured from a live run against `tcp://127.0.0.1:1883` (fast disconnect-triggered
redelivery path).

## What's Happening

1. **Consumer A** subscribes on `$share/g1/queues/demo/rq` at QoS 1 as the **sole**
   consumer, using `manual_ack_set(True)` so paho-mqtt 2.x does **not** auto-send the
   PUBACK on receipt (mirrors connector test #6, where sub1 is the sole subscriber when
   the message is published).
2. **Producer** publishes one message to `queues/demo/rq` at QoS 1.  The topic prefix
   `queues/` selects the KubeMQ Queues pattern; `/` → `.` gives channel `demo.rq`.
   Consumer A receives it and holds it unacked in-flight.
3. **Consumer B** subscribes on the same filter *after* Consumer A has the message but
   *before* Consumer A's disconnect completes — it is waiting when the redelivery fires.
4. **Consumer A** disconnects while holding the unacked message.  The connector detects
   the unacked in-flight message on disconnect → **immediate NAck → message requeued**
   (no loss).  Without `manual_ack_set(True)`, paho would PUBACK immediately and the
   message would be acked before the disconnect could trigger redelivery.
5. **Consumer B** receives the redelivered message and sends its PUBACK, which acks the
   message to KubeMQ — the message is removed from the queue.

The slower redelivery path — never sending PUBACK within **QueueAckTimeoutSeconds**
(default 30 s) — is equally valid.

## MQTT Specifics

| Property | Value |
|----------|-------|
| MQTT version | 5.0 (`MQTTv5`) |
| Produce topic | `queues/demo/rq` |
| Consume filter | `$share/g1/queues/demo/rq` |
| Subscribe QoS | 1 (QoS 0 → SUBACK `0x83`, rejected) |
| Publish QoS | 1 |
| MQTT 5.0 properties used | None (no User Properties in this example) |
| Ack mechanism | PUBACK (sent automatically by paho on QoS 1 message receipt) |
| Redelivery trigger | Disconnect with unacked in-flight (immediate); or ack timeout (30 s default) |

## Ack Model

| Event | KubeMQ effect |
|-------|---------------|
| PUBACK received from consumer | Message acked — removed from queue |
| No PUBACK within `QueueAckTimeoutSeconds` (default 30 s) | NAck → message requeued (test #7) |
| Consumer disconnects with unacked message | Immediate NAck → message requeued (test #6) |

## Gotcha — $share Group Is One Competing Pool (gotcha #3)

The `<group>` segment in `$share/<group>/queues/<ch>` is **audit and metrics only**.
All subscribers, regardless of group name, compete in a **single shared KubeMQ queue
pool**.  This is different from standard MQTT shared-subscription semantics where each
group receives independent copies.  Consumer A and Consumer B both use `g1`; using
`g2` for one of them would have exactly the same effect.

## Negative Paths

| Situation | Reason code |
|-----------|-------------|
| QoS 0 shared queue subscribe | SUBACK `0x83` — rejected (test #8) |
| Plain `queues/...` subscribe (no `$share`) | SUBACK `0x83` — rejected (test #9) |
| `$share` on non-queue topic | SUBACK `0x83` — rejected |

## Related Examples

- [queues/shared_consume](../shared_consume/) — basic shared subscription with auto-ack
- [events/basic_pubsub](../../events/basic_pubsub/) — fire-and-forget pub/sub (no ack)
