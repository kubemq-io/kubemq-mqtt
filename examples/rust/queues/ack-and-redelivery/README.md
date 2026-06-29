# rust — Queues: Ack & Redelivery

Demonstrates KubeMQ Queue ack-on-PUBACK behaviour and redelivery semantics using
MQTT 5.0 and `rumqttc`.  Two redelivery paths are covered:

- **Fast path (demonstrated)**: Consumer 1 receives a message but disconnects
  *without sending PUBACK*.  KubeMQ detects the TCP close and immediately
  requeues the in-flight message.  Consumer 2 then receives and acks it.
- **Slow path (documented, not run)**: No PUBACK within
  `QueueAckTimeoutSeconds` (default 30 s) → KubeMQ NAcks the delivery and
  redelivers to any subscriber on the same `$share` group.

## Prerequisites

- Rust 1.75+ (stable)
- KubeMQ server with MQTT connector enabled on port 1883

## How to Run

```bash
cd examples/rust
KUBEMQ_MQTT_URL=tcp://localhost:1883 cargo run -p ack-and-redelivery
```

## Expected Output

> Regenerated from a live run in the LiveTest stage. Topic strings below are canonical.

```
=== Step 1: Consumer 1 subscribes (manual-ack mode — will NOT send PUBACK) ===
[Consumer 1] Subscribed to "$share/g1/queues/demo/rq" (QoS 1, manual-ack)

=== Step 2: Producer publishes one message ===
[Producer] Published to "queues/demo/rq" (QoS 1)

=== Step 3: Consumer 1 receives the message (no PUBACK sent) ===
[Consumer 1] Received message (PUBACK NOT sent): {"job":1,"note":"will be requeued on disconnect","ts":"2026-06-27T13:50:01Z"}

=== Step 4: Consumer 2 connects and subscribes (auto-ack) ===
  (Ready to pick up the requeued message the moment it is re-enqueued.)
[Consumer 2] Subscribed to "$share/g1/queues/demo/rq" (QoS 1, auto-ack)

=== Step 5: Consumer 1 disconnects WITHOUT acking ===
  KubeMQ detects the TCP close and immediately requeues the in-flight message.
[Consumer 1] Event loop ended: Mqtt state: Connection closed by peer abruptly
[Consumer 1] Disconnected — in-flight message has been requeued by KubeMQ.

=== Step 6: Consumer 2 receives the requeued message ===
[Consumer 2] Received redelivered message (auto-PUBACK): {"job":1,"note":"will be requeued on disconnect","ts":"2026-06-27T13:50:01Z"}

At-least-once delivery confirmed — message survived the disconnect.
  Redelivered payload: {"job":1,"note":"will be requeued on disconnect","ts":"2026-06-27T13:50:01Z"}

--- Ack & Redelivery summary ---
Fast path (demonstrated):   disconnect with pending PUBACK
                            → KubeMQ immediately requeues the message.
Slow path (not run here):   no PUBACK within QueueAckTimeoutSeconds
                            (default 30 s) → NAck → redeliver.
$share group is one pool:   all $share/<g>/queues/<ch> groups compete
                            in ONE KubeMQ queue — NOT per-group copies.
Done.
```

## What's Happening

1. **Consumer 1** connects with `mqttoptions.set_manual_acks(true)`.  When this
   flag is set, `rumqttc` does **not** send PUBACK automatically on QoS 1
   delivery — the application must call `client.ack(&publish).await` explicitly.
2. Consumer 1 subscribes to `$share/g1/queues/demo/rq` at QoS 1.
3. **Producer** publishes one message to `queues/demo/rq`.
4. Consumer 1 receives the message; the event loop delivers it but holds the
   PUBACK pending an explicit `ack()` call.
5. **Consumer 2** connects with `manual_acks=false` (default, auto-PUBACK) and
   subscribes to the same shared topic before Consumer 1 disconnects.
6. Consumer 1 calls `client.disconnect().await` **without** calling
   `client.ack()`.  Because PUBACK was never sent, KubeMQ treats the TCP close
   as an implicit NAck and **immediately requeues** the in-flight message.
7. Consumer 2 receives the redelivered message; `rumqttc` sends PUBACK
   automatically, fully acking it.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 |
| Produce topic | `queues/demo/rq` |
| Consume topic | `$share/g1/queues/demo/rq` |
| QoS | 1 (`AtLeastOnce`) |
| Consumer 1 manual acks | `mqttoptions.set_manual_acks(true)` |
| Consumer 2 manual acks | `false` (default — auto-PUBACK) |
| KubeMQ channel | `demo.rq` |
| KubeMQ pattern | Queues |

### Reason codes to know

| Code | Meaning | When |
|---|---|---|
| `0x83` | impl-specific error | QoS 0 on `$share/…` or plain `queues/…` subscribe |
| `0x80` | broker not ready | publish while broker is initialising |
| `0x87` | not authorised | ACL deny |

## Gotchas

- **`$share` group is one competing pool**: all groups (e.g. `g1`, `g2`, `g99`)
  on `$share/<g>/queues/<ch>` compete in a **single** KubeMQ queue — the group
  name is for audit/metrics only.  Unlike standard MQTT 5 shared subscriptions,
  KubeMQ does NOT create per-group copies.
- **QoS 0 on `$share/…` → SUBACK 0x83**: KubeMQ rejects queue subscriptions at
  QoS 0 because it cannot guarantee delivery without the PUBACK ack loop.
- **Plain `queues/…` subscribe → SUBACK 0x83**: queue consume always requires
  the `$share/<g>/` prefix.
- **`manual_acks` only in MQTT 5.0**: `rumqttc::v5::MqttOptions`.  The v4 path
  also exposes the flag but KubeMQ shared-subscription queues require MQTT 5.
- **Ack timeout slow path**: if neither disconnect nor PUBACK happens within
  `QueueAckTimeoutSeconds` (default 30 s), the broker triggers the slow-path
  NAck and redelivers to any available subscriber.

## Related Examples

- [queues/shared-consume](../shared-consume/) — basic queue consume with auto-ack
- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget events (no ack)
- [events-store/start-new-only](../../events-store/start-new-only/) — persistent events (always StartNewOnly over MQTT)
