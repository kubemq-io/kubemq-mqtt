# rust — Events-Store: Start-New-Only (no replay)

Demonstrates the **StartNewOnly** behavior of KubeMQ Events-Store over MQTT.
Events-Store messages published *before* a subscription was established are
**never** delivered over MQTT — there is no historical replay.

## Prerequisites

- Rust 1.75+ (stable)
- KubeMQ server with MQTT connector enabled (default port 1883)
- `KUBEMQ_MQTT_URL` environment variable (default `tcp://localhost:1883`)

## How to Run

```bash
cd examples/rust
KUBEMQ_MQTT_URL=tcp://localhost:1883 cargo run -p start-new-only
```

## Expected Output

> Regenerated from a live run in the LiveTest stage. Topic strings below are canonical.

```
=== Events-Store: Start-New-Only Demo ===
Broker : tcp://localhost:1883
Channel: store/demo/s  (KubeMQ channel: demo.s)

--- Step 1: publish BEFORE subscribing (StartNewOnly: not delivered) ---
[published-pre] #1: 'pre-subscribe message #1' -> store/demo/s  (will NOT be delivered)
[published-pre] #2: 'pre-subscribe message #2' -> store/demo/s  (will NOT be delivered)
[published-pre] #3: 'pre-subscribe message #3' -> store/demo/s  (will NOT be delivered)

--- Step 2: subscribe to store/demo/s (StartNewOnly active) ---
[subscribed] store/demo/s  QoS=1

--- Step 3: publish AFTER subscribing (this message IS delivered) ---
[published-post] 'post-subscribe message -- the only one you get' -> store/demo/s

--- Step 4: waiting for delivery ---
[received] topic='store/demo/s' payload='post-subscribe message -- the only one you get'

=== Result ===
Messages received : 1 (expected: 1)
  [1] 'post-subscribe message -- the only one you get'

StartNewOnly confirmed: 3 pre-subscribe publishes were NOT delivered.
Only the 1 post-subscribe publish arrived.
EVENTS_STORE_START_NEW_ONLY_OK
```

## What's Happening (Step by Step)

| Step | Action | Outcome |
|------|--------|---------|
| 1 | Connect with MQTT 5.0, unique client ID | TCP session established |
| 2 | Publish 3 messages to `store/demo/s` **before** subscribing | Broker accepts (PUBACK 0x00), KubeMQ stores them — but no subscriber exists yet |
| 3 | Subscribe to `store/demo/s` at QoS 1 | SUBACK 0x01; subscription active from **this moment forward** |
| 4 | Publish 1 message to `store/demo/s` **after** subscribing | Delivered to the subscriber |
| 5 | Assert exactly 1 message received | Confirms StartNewOnly: the 3 pre-subscribe messages were never replayed |

## MQTT Specifics

| Property | Value |
|----------|-------|
| MQTT version | 5.0 |
| Topic | `store/demo/s` |
| KubeMQ channel | `demo.s` (MQTT `/` maps to KubeMQ `.`) |
| KubeMQ pattern | Events-Store |
| QoS (publish) | 1 (AtLeastOnce) |
| QoS (subscribe) | 1 (AtLeastOnce) |
| Retain | Not used (retain is silently dropped by KubeMQ — see gotcha below) |
| MQTT 5.0 properties | None required for basic pub/sub |

## Gotchas

### Gotcha #2 — Events-Store is ALWAYS StartNewOnly over MQTT

KubeMQ Events-Store natively supports six replay types (StartNewOnly,
StartFromFirst, StartFromLast, StartFromSequence, StartFromTime,
StartFromTimeDelta). Over MQTT, **only StartNewOnly is available**.
There is no way to request historical replay from an MQTT client.

If you need replay, use the KubeMQ gRPC or REST API directly.

### Gotcha #1 — Retain is silently dropped

If you publish with the MQTT retain flag set, KubeMQ sends PUBACK 0x00
(success) but **silently discards the message**. It is not stored, not
delivered, and a `publish.error` is written to the audit log. This is
distinct from StartNewOnly — retained publishes vanish entirely.

## Related Examples

- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget Events (not persisted)
- [queues/shared-consume](../../queues/shared-consume/) — persistent queue with competing consumers
- [queues/ack-and-redelivery](../../queues/ack-and-redelivery/) — at-least-once delivery with PUBACK timeout
