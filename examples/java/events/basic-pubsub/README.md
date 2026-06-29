# java — Events: Basic Pub/Sub

Publish a message to a KubeMQ Events channel and receive it on a subscriber client
using **MQTT 5.0**.  Demonstrates the `events/` topic prefix, a single-level `+`
wildcard subscription, the `"/"` → `"."` channel mapping, and MQTT 5.0 User
Properties being forwarded as KubeMQ Tags.

## Prerequisites

- Java 21+
- Maven 3.8+
- KubeMQ server with the MQTT connector enabled (default port 1883)

## How to Run

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883   # optional; this is the default
cd examples/java/events/basic-pubsub
mvn compile exec:java
```

## Expected Output

```
=== events/basic-pubsub (MQTT 5.0) ===
Broker     : tcp://127.0.0.1:1883
Sub topic  : events/demo/+  (single-level '+' wildcard)
Pub topic  : events/demo/x
Channel    : demo.x  (MQTT '/' maps to KubeMQ '.')

[1] Connecting subscriber ...
[2] Subscribing to 'events/demo/+' at QoS 1 ...
Subscriber connected to tcp://127.0.0.1:1883 (reconnect=false)
    SUBACK granted QoS=1
[3] Connecting publisher ...
Publisher  connected to tcp://127.0.0.1:1883 (reconnect=false)
[4] Publishing to 'events/demo/x' QoS=1, payload='hello', user-props={k1=v1, example=events/basic-pubsub} ...
    PUBACK reason=0x00 (success)
[5] Waiting for message delivery ...
--- Message received ---
  Topic   : events/demo/x
  QoS     : 1
  Payload : hello
  User Properties (KubeMQ Tags on delivery):
    k1 = v1
    example = events/basic-pubsub

Round-trip complete.
  KubeMQ channel : demo.x
  MQTT sub topic : events/demo/+  (single-level '+' wildcard)
  MQTT pub topic : events/demo/x
  User Properties were forwarded as KubeMQ Tags (if subscriber is v5).
[6] Disconnecting ...
Done.
```

## What's Happening

1. **Subscriber connects** — an MQTT 5.0 client (`MqttAsyncClient`) connects with
   `cleanStart=true` and keepalive 30 s.
2. **Subscribe** — subscribes to `events/demo/+` at QoS 1 (the single-level `+`
   wildcard matches a one-segment channel) and waits for SUBACK reason code `0x01`
   (QoS 1 granted).  Wildcards are accepted **only** on Events subscriptions.  The
   subscription must be established **before** publishing; KubeMQ Events is
   fire-and-forget with no persistence.
3. **Publisher connects** — a second MQTT 5.0 client connects with the same options.
4. **Publish** — sends a message with payload `"hello"` at QoS 1 to `events/demo/x`
   (matched by the `events/demo/+` subscription).  Two MQTT 5.0 User Properties
   (`k1=v1`, `example=events/basic-pubsub`) are attached to the MQTT `PUBLISH` packet;
   the KubeMQ connector copies them into the KubeMQ message `Tags` map.
5. **Deliver** — KubeMQ routes the event to all active subscribers and re-attaches the
   `Tags` as MQTT 5.0 User Properties on the `PUBLISH` delivered to the subscriber.
6. **Display** — the subscriber callback prints the payload and the round-tripped User
   Properties.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 |
| Subscribe topic | `events/demo/+` (single-level `+` wildcard; MQTT `+` → KubeMQ `*`) |
| Publish topic | `events/demo/x` |
| Topic prefix | `events/` (selects KubeMQ **Events** pattern) |
| KubeMQ channel | `demo.x` (`"/"` in MQTT topic → `"."` in KubeMQ channel) |
| QoS (subscribe) | 1 |
| QoS (publish) | 1 |
| User Properties | `k1=v1`, `example=events/basic-pubsub` |
| User Properties caps | 32 properties max / 4096 bytes total; exceeding → PUBACK `0x97` |
| Retain | Not set (see Gotchas below) |

## Gotchas

**Retain is silently dropped.**
KubeMQ MQTT connector sets `RetainAvailable=0`.  A PUBLISH with `Retain=true` is
silently dropped at runtime: you receive PUBACK `0x00` (success) but the message is
**never delivered** and a `publish.error` is audited in the server.  Do not set retain.

**Events have no history.**
KubeMQ Events (prefix `events/`) are fire-and-forget.  If no subscriber is connected
when the publisher sends, the message is lost.  Use `events-store/` prefix for
persistent delivery — but note that Events-Store over MQTT is always `StartNewOnly`
(no historical replay regardless of client session settings).

**`"/"` and `"."` are interchangeable in a channel name (lossy).**
MQTT topic `events/site1/temp` and MQTT topic `events/site1.temp` both map to KubeMQ
channel `site1.temp`.  Avoid literal `"."` in your MQTT topic segments to keep
the mapping unambiguous.

**User Properties are MQTT 5.0 only.**
An MQTT 3.1.1 publisher has no user-properties; the connector will not receive any
Tags from a v3.1.1 client.  An MQTT 3.1.1 subscriber will not receive Tags in the
delivered message (v3.1.1 wire format has no user-properties).

## Related Examples

- [events/wildcard-subscribe](../wildcard-subscribe/) — subscribe to multiple channels
  with MQTT `+` / `#` wildcards (mapped to KubeMQ `*` / `>`)
- [events-store/start-new-only](../../events-store/start-new-only/) — persistent
  events via the `store/` topic prefix
- [queues/shared-consume](../../queues/shared-consume/) — competing consumers via
  `$share` group subscriptions (MQTT 5.0 + QoS ≥ 1 required)
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — side-by-side
  comparison showing absent User Properties on v3.1.1
