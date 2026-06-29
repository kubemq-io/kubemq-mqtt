# Ruby — Events: Basic Pub/Sub

Fire-and-forget event pub/sub using the `mqtt` gem (MQTT 3.1.1) and the KubeMQ MQTT connector.

Demonstrates the core Events pattern: subscribe with the single-level wildcard `events/demo/+`, publish to `events/demo/x`, and show that the MQTT `/` separator maps to `.` in the KubeMQ channel name (`demo.x`).

## Prerequisites

- Ruby 3.1+
- `bundle install` in `examples/ruby/`
- KubeMQ server with MQTT connector enabled (default port 1883)

## How to Run

```bash
cd examples/ruby
bundle install
export KUBEMQ_MQTT_URL=tcp://localhost:1883   # default; omit if using localhost:1883
ruby events/basic_pubsub/main.rb
```

## Expected Output

Captured from a live run against `tcp://127.0.0.1:1883` (the `client_id` hex suffixes are randomized per run).

```
[example] events/basic_pubsub
[example]   MQTT sub topic : events/demo/+  (single-level '+' wildcard)
[example]   MQTT pub topic : events/demo/x
[example]   KubeMQ channel : demo.x  ('/' -> '.' mapping)
[example]   MQTT version   : 3.1.1 (mqtt gem — no User Properties)
[example]   Tag note       : tag 'k1=v1' encoded in payload
[example]                    (MQTT 5.0 clients use User Properties for real tag round-trip)

[sub] subscribed to 'events/demo/+' QoS 1 (client_id=ruby-mqtt-events-sub-2a90e03c)
[sub] subscription established — ready to receive
[pub] published to 'events/demo/x' QoS 1 (client_id=ruby-mqtt-events-pub-f4f962a6)
[pub]   payload : "hello|k1=v1"

[recv] message received:
[recv]   topic   : events/demo/x
[recv]   payload : "hello|k1=v1"
[recv]   body    : "hello"
[recv]   tags    : {"k1"=>"v1"}  (simulated; real MQTT 5.0 User Property round-trip)

[ok]  Round-trip complete.
[ok]  Topic 'events/demo/x' -> KubeMQ channel 'demo.x' ('/' -> '.' confirmed).
```

## What's Happening

1. **Subscribe first** — a subscriber thread connects with a unique `client_id` and subscribes to `events/demo/+` (single-level wildcard) at QoS 1. The subscription is signalled via a `Queue` so the publisher does not send until the bridge entry is registered.
2. **Publish** — a separate client (different `client_id`) connects and publishes `"hello|k1=v1"` to `events/demo/x` at QoS 1 with `retain=false`. The `+` wildcard matches the final segment, so the subscriber receives this publish.
3. **Bridge** — the KubeMQ MQTT connector receives the publish, strips the `events/` prefix, converts `/` to `.`, and forwards the message as a KubeMQ Event on channel `demo.x`.
4. **Deliver** — the connector fans the event back to the subscriber (same channel, matching bridge entry).
5. **Round-trip logged** — the example parses the simulated tag out of the payload and prints body and tags separately.

## MQTT Specifics

| Property | Value |
|---|---|
| Topic (subscribe) | `events/demo/+` (single-level `+` wildcard) |
| Topic (publish) | `events/demo/x` |
| QoS | 1 (both directions) |
| MQTT version | 3.1.1 (`mqtt` gem — MQTT 5.0 not supported by this gem) |
| `retain` flag | `false` (mandatory — see gotcha below) |
| KubeMQ pattern | Events |
| KubeMQ channel | `demo.x` (`/` → `.` mapping) |
| User Properties | Canonical sets v5 user-prop `k1=v1` → KubeMQ tag `k1=v1`; here simulated in payload (MQTT 3.1.1 has no User Properties) |

### MQTT 5.0 User Properties / KubeMQ Tags

MQTT 5.0 User Properties map bidirectionally to KubeMQ message Tags (up to 32 properties / 4096 bytes total). The canonical example sets a v5 user property `k1=v1` on the publish, which arrives as the KubeMQ tag `k1=v1`. Because the `mqtt` gem implements only MQTT 3.1.1, this round-trip cannot be demonstrated natively in Ruby. The example encodes the same `k1=v1` tag as a `key=value` suffix in the payload as a substitute.

To observe a real User Property round-trip, use the Go, Python, or JavaScript examples in this repository (all use MQTT 5.0 clients).

## Gotchas

**Gotcha #1 — Retain silently dropped:** The broker forces `RetainAvailable=0`. A retained publish returns PUBACK success (`0x00`) but the message is silently discarded — never delivered, never stored, no error response. Always publish with `retain=false`.

**Gotcha #5 — Literal `.` in a topic segment:** `events/a.b/c` and `events/a/b/c` both map to KubeMQ channel `a.b.c` (lossy). The MQTT `/` separator and the literal `.` character in a segment are indistinguishable after mapping. Avoid dots in individual topic segments.

## Related Examples

- [events/wildcard_subscribe](../wildcard_subscribe/) — overlapping wildcard filters and the N-copies-per-filter behavior (gotcha #6)
- [events_store/start_new_only](../../events_store/start_new_only/) — persistent Events-Store (no historical replay over MQTT)
- [connectivity/auth_jwt](../../connectivity/auth_jwt/) — connecting with a KubeMQ JWT token as the MQTT password
