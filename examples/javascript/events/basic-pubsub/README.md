# JavaScript/TypeScript — Events: Basic Pub/Sub

Demonstrates fire-and-forget event pub/sub using the KubeMQ MQTT connector with MQTT 5.0.
A subscriber connects and subscribes to the single-level wildcard `events/demo/+`; a publisher
sends `"hello"` to `events/demo/x` (which the `+` filter matches) with one MQTT 5.0 User
Property (`k1=v1`), which arrives as a KubeMQ Tag — demonstrating the bidirectional
User-Properties ↔ KubeMQ Tags mapping.

## Prerequisites

- Node.js 18+
- `npm install` run once inside `examples/javascript/`
- KubeMQ server with MQTT connector enabled (default port 1883)

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883   # optional; this is the default
npx tsx events/basic-pubsub/index.ts
```

## Expected Output

```
[subscriber] connected to tcp://localhost:1883
[subscriber] subscribed: events/demo/+  QoS 1
[publisher]  connected to tcp://localhost:1883
[publisher]  published "hello" -> events/demo/x  QoS 1
[publisher]  user-property sent: k1=v1 (-> KubeMQ tag)
[subscriber] message received on topic: events/demo/x
[subscriber]   payload: hello
[subscriber]   KubeMQ channel (topic '/' -> '.'): demo.x
[subscriber]   user-properties (KubeMQ tags round-trip): {"k1":"v1"}
[done]
```

## What's Happening

1. **Connect subscriber** — MQTT 5.0 client (`protocolVersion: 5`, `keepalive: 30`).
2. **Subscribe** — `events/demo/+` at QoS 1.  The `events/` prefix tells the KubeMQ MQTT
   connector to route this through the **Events** pattern (fire-and-forget, no persistence).
   The `+` single-level wildcard matches any single topic level (`events/demo/x`,
   `events/demo/y`, …); wildcards are accepted on the Events pattern only.
3. **Wait 300 ms** — allows the broker to register the subscription before the publisher connects.
4. **Connect publisher** — separate MQTT 5.0 client.
5. **Publish** — payload `"hello"` to `events/demo/x` (matched by the `events/demo/+` filter)
   at QoS 1, with one MQTT 5.0 User Property `k1=v1`.  The broker acknowledges with PUBACK 0x00.
6. **Deliver** — the broker forwards the message to the subscriber.  The User Property
   `k1=v1` is echoed back as a KubeMQ Tag.
7. **Clean disconnect** — both clients call `endAsync()`.

### Topic → KubeMQ Channel mapping

```
MQTT topic:  events / demo / x
               │       └───┘
               │       KubeMQ channel (slashes become dots)
               │
               └─ prefix selects Events pattern

KubeMQ channel:  demo.x
```

The subscriber uses the wildcard filter `events/demo/+`; the `+` matches the final `x`
level so the published `events/demo/x` is delivered.

Every `/` separator inside the channel part becomes `.` in the KubeMQ channel name.
A literal `.` in a topic segment also becomes `.`, which means it is **indistinguishable**
from a `/` — avoid dots inside topic segments (see Gotcha #5 below).

### MQTT 5.0 User Properties ↔ KubeMQ Tags

| Direction | Mechanism |
|-----------|-----------|
| PUBLISH → KubeMQ | `properties.userProperties` on the PUBLISH packet map to `Tags` on the KubeMQ message |
| KubeMQ → SUBSCRIBE | Tags on the KubeMQ event arrive as `userProperties` on the delivered PUBLISH |

Caps: **32 user-properties** per message, **4096 bytes** total.  Exceeding either limit causes
PUBACK reason code `0x97` (quota exceeded) and the message is rejected.

MQTT v3.1.1 clients have **no** User-Properties support — see
[protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) for the comparison.

## MQTT Specifics

| Property | Value |
|----------|-------|
| Protocol version | MQTT 5.0 (`protocolVersion: 5`) |
| Subscribe filter | `events/demo/+` (single-level `+` wildcard) |
| Publish topic | `events/demo/x` |
| QoS | 1 (both subscribe and publish) |
| Payload | `hello` |
| MQTT 5.0 User Property | `k1=v1` — round-trips as KubeMQ Tag |
| KubeMQ channel | `demo.x` (topic slashes → dots) |
| KubeMQ pattern | Events (fire-and-forget) |

## Gotchas

**Gotcha #1 — Retain is silently dropped.**  If `retain: true` is set on a PUBLISH, the broker
returns PUBACK `0x00` (success) but the message is **never delivered** and is not stored.
This example explicitly sets `retain: false`.

**Gotcha #5 — Literal dots in topic segments conflate with slashes.**  The mapping
`events/site1.room` and `events/site1/room` both produce KubeMQ channel `site1.room`, making
them indistinguishable on the broker side.  Keep topic segments free of dots.

## Reason Codes

| Code | Meaning | When |
|------|---------|------|
| `0x00` | Success | SUBACK / PUBACK — normal operation |
| `0x01` | Granted QoS 1 | SUBACK at QoS 1 |
| `0x80` | Broker not ready | Publish gated (broker initialising) |
| `0x83` | Implementation specific | QoS-0 queue subscribe, plain `queues/…` subscribe |
| `0x8F` | Topic filter invalid | Empty topic segment |
| `0x90` | Topic name invalid | Empty channel or no default pattern configured |
| `0x97` | Quota exceeded | User-property cap (32 props / 4096 bytes) |
| `0x9A` | Retain not supported | Will-retain set at CONNECT |
| `0xA2` | Wildcard not supported | Wildcard subscribe on non-Events topics |

## Related Examples

- [events/wildcard-subscribe](../wildcard-subscribe/) — `+` and `#` wildcard filters; N-copy semantics for overlapping subscriptions
- [events-store/start-new-only](../../events-store/start-new-only/) — persistent Events-Store (MQTT = StartNewOnly, no historical replay)
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — MQTT 3.1.1 vs 5.0 feature contrast (User Properties, reason codes)
