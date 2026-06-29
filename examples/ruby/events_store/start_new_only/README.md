# Ruby — Events-Store: Start-New-Only (no replay)

Demonstrates the Events-Store pattern over MQTT, which **always** operates in `StartNewOnly` mode.
Messages published before a subscription is established are **never delivered** — there is no historical replay over MQTT (**gotcha #2**).

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with MQTT connector enabled (port 1883)
- `mqtt` gem ~> 0.7

## How to Run

```bash
cd examples/ruby
bundle install
export KUBEMQ_MQTT_URL=tcp://localhost:1883   # default; adjust as needed
ruby events_store/start_new_only/main.rb
```

## Expected Output

Captured from a live run against `tcp://127.0.0.1:1883`.

```
=== Events-Store: Start-New-Only (no replay) ===
Topic: store/demo/s  (KubeMQ channel: demo.s)
MQTT version: 3.1.1  |  MQTT gem: njh/ruby-mqtt

Teaching: MQTT Events-Store subscriptions ALWAYS start at StartNewOnly.
          There is NO historical replay over MQTT (unlike gRPC/REST).

Step 1: Publishing 3 messages BEFORE subscription is established...
  Published (pre-sub): pre-sub-message-1
  Published (pre-sub): pre-sub-message-2
  Published (pre-sub): pre-sub-message-3
  -> These messages are stored in KubeMQ Events-Store,
     but will NOT be delivered to any subscriber that connects AFTER them.

Step 2: Subscribing to store/demo/s at QoS 1...
  Note: MQTT forces StartNewOnly — no way to request historical replay.
  Subscribed successfully.

Step 3: Publishing 1 message AFTER subscription is established...
  Published (post-sub): post-sub-message-1
  -> This message was published AFTER the subscription; it WILL be delivered.

Step 4: Collecting received messages (timeout: 5s)...

=== Result ===
Received 1 message(s) (expected: 1):
  [1] topic=store/demo/s  payload=post-sub-message-1

OK — StartNewOnly confirmed:
     3 pre-subscription messages: NOT delivered (no replay)
     1 post-subscription message: DELIVERED
     MQTT events-store subscriptions are ALWAYS StartNewOnly — no historical replay.
```

## What's Happening

1. A publisher connects and publishes **3 messages** to `store/demo/s` (QoS 1). No subscriber exists yet, so no one receives them. KubeMQ persists them internally in the Events-Store, but the MQTT connector provides no way to replay them.
2. A subscriber connects and subscribes to `store/demo/s` (QoS 1). Because Events-Store over MQTT is **always `StartNewOnly`**, the 3 pre-existing messages are **not delivered** — they are simply skipped.
3. A second publisher publishes **1 message** to `store/demo/s` after the subscription is active.
4. The subscriber receives exactly **1 message** (the post-subscription one). The 3 pre-subscription messages are never delivered.

The KubeMQ MQTT connector hard-codes `StartNewOnly` for all `store/<ch>` subscriptions. Unlike KubeMQ's native gRPC or REST APIs (which support 6 replay types: `StartNewOnly`, `StartFromFirst`, `StartFromSequence`, `StartFromTime`, `StartFromLastN`), MQTT-based Events-Store clients **always** start from the moment they subscribe.

Canonical integration test: `#4 EventsStoreStartNewOnly` (asserts pre-sub messages are NOT delivered).

## MQTT Specifics

| Property | Value |
|----------|-------|
| Topic | `store/demo/s` |
| KubeMQ channel | `demo.s` |
| QoS | 1 |
| MQTT version | 3.1.1 (mqtt gem — njh/ruby-mqtt) |
| KubeMQ pattern | Events-Store |
| Replay mode | `StartNewOnly` (forced by the connector — no other mode available) |
| MQTT 5.0 properties | None (MQTT 3.1.1 has no User Properties) |

## Gotcha #2 — Events-Store Never Replays Over MQTT

The MQTT connector **always** starts Events-Store subscriptions at `StartNewOnly`. There is no way to configure a different start position (e.g. `StartFromFirst`, `StartFromSequence`, `StartFromTime`) over MQTT. If you need historical replay, use the KubeMQ gRPC or REST APIs instead.

This is a key difference from the cloud-events connector (which supports all 6 replay types via the `ce-kubemq-client-id-store-offset` extension attribute).

## Related Examples

- [events/basic_pubsub](../../events/basic_pubsub/) — fire-and-forget events (no persistence, no replay concept)
- [events/wildcard_subscribe](../../events/wildcard_subscribe/) — wildcard subscription behavior and the N-copies gotcha
- [connectivity/tls](../../connectivity/tls/) — same Events pattern over TLS
