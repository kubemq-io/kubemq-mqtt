# python — Queues: Shared-Subscription Consume

Demonstrates KubeMQ Queues via MQTT 5.0 shared subscriptions (`$share`).

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` (from `examples/python/`) — installs `paho-mqtt>=2.1.0`
- KubeMQ server with MQTT connector enabled
- **MQTT 5.0 required** — shared subscription queue consume is a v5-only feature;
  v3.1.1 clients cannot use `$share` subscriptions on queue topics.

## How to Run

```bash
cd examples/python
export KUBEMQ_MQTT_URL=tcp://localhost:1883
python queues/shared_consume/main.py
```

## Expected Output

```
Broker: tcp://127.0.0.1:1883  (scheme=tcp, host=127.0.0.1, port=1883)
Produce topic : queues/demo/q
Consume filter: $share/g1/queues/demo/q  (QoS 1)
KubeMQ channel: demo.q  (/ -> . translation)

[consumer] connected; subscribing to $share/g1/queues/demo/q
[consumer] SUBACK mid=1 reason_code=1  (subscribed OK)

[producer] published order 1 to queues/demo/q
[producer] published order 2 to queues/demo/q
[producer] published order 3 to queues/demo/q

[consumer] received msg 1/3: '{"order_id": 1, "item": "widget-1"}'  (PUBACK = ack to KubeMQ)
[consumer] received msg 2/3: '{"order_id": 2, "item": "widget-2"}'  (PUBACK = ack to KubeMQ)
[consumer] received msg 3/3: '{"order_id": 3, "item": "widget-3"}'  (PUBACK = ack to KubeMQ)

Done. Round-trip verified: produced 3, consumed 3 via shared subscription.

Negative paths (not triggered in this run — for reference):
  QoS 0 shared-sub  -> SUBACK 0x83  [test #8]
  Plain queues/ sub -> SUBACK 0x83  [test #9]
```

Captured from a live run against `tcp://127.0.0.1:1883`.

## What's Happening

1. **Consumer subscribes** to `$share/g1/queues/demo/q` at QoS 1 and waits for
   SUBACK confirmation before the producer publishes (avoids a race condition).
2. **Producer publishes** three messages to `queues/demo/q` at QoS 1.
3. The connector routes each PUBLISH into the KubeMQ `demo.q` queue.
4. The consumer receives each message; paho-mqtt 2.x automatically sends PUBACK.
5. **PUBACK = ack**: each PUBACK signals a successful ack to KubeMQ, removing the
   message from the queue.
6. Both clients disconnect cleanly after all messages are consumed.

## MQTT Specifics

| Property | Value |
|----------|-------|
| Produce topic | `queues/demo/q` |
| Consume filter | `$share/g1/queues/demo/q` |
| QoS | 1 (both publish and subscribe) |
| MQTT version | 5.0 (required) |
| v5 properties used | none (basic produce/consume) |
| KubeMQ channel | `demo.q` (`/` → `.` translation) |
| KubeMQ pattern | Queues |

## Negative Paths (test traceability)

| Situation | SUBACK code | Test |
|-----------|-------------|------|
| QoS 0 shared-subscription queue subscribe | `0x83` (impl-specific, rejected) | test #8 |
| Plain `queues/...` subscribe (no `$share` prefix) | `0x83` (impl-specific, rejected) | test #9 |

(negative paths: QoS0 → 0x83 test #8; plain `queues/` → 0x83 test #9)

## Gotcha — `$share` Group Is One Competing Pool

The `<group>` name in `$share/<group>/queues/<ch>` is **audit and metrics only**.

All subscribers — regardless of group name — compete in a **single shared KubeMQ
queue pool**. This differs from standard MQTT broker semantics where each group
receives independent copies of each message. With KubeMQ's MQTT connector there is
only one queue pool; adding more groups does NOT produce more copies.

Example: `$share/groupA/queues/demo/q` and `$share/groupB/queues/demo/q` both draw
from the same underlying queue — they do not each receive a copy.

## Ack Model

| Event | Effect |
|-------|--------|
| PUBACK received from consumer | KubeMQ acks message (removed from queue) |
| No PUBACK within `QueueAckTimeoutSeconds` (default 30 s) | KubeMQ NAcks → requeued for redelivery |
| Consumer disconnects with unacked message | Immediate NAck → requeued (no message loss) |

## Related Examples

- [queues/ack_and_redelivery](../ack_and_redelivery/) — timeout and redelivery behavior
- [events/basic_pubsub](../../events/basic_pubsub/) — fire-and-forget events (no ack)
