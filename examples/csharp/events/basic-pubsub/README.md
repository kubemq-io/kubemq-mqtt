# csharp — Events: Basic Pub/Sub

Basic MQTT 5.0 publish/subscribe over KubeMQ Events using MQTTnet.

Demonstrates:
- How the `events/` topic prefix routes messages to the KubeMQ Events pattern.
- How MQTT topic segments separated by `/` map to `.` in the KubeMQ channel name.
- MQTT 5.0 User Properties round-tripping as KubeMQ Tags (bidirectional `k1=v1`).
- Correct subscribe-before-publish ordering and QoS 1 flow.

## Prerequisites

- .NET 8+
- KubeMQ server with the MQTT connector enabled (default `tcp://localhost:1883`)

## How to Run

```bash
cd examples/csharp/events/basic-pubsub
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run
```

## Expected Output

```
Broker:          tcp://127.0.0.1:1883
Subscribe topic: events/demo/+  (KubeMQ wildcard channel demo.*)
Publish topic:   events/demo/x  (KubeMQ channel demo.x)

[1] Connecting subscriber...
    Subscriber connected  (ResultCode=Success)
    Subscribed to 'events/demo/+'  (SUBACK=GrantedQoS1)

[2] Connecting publisher...
    Publisher connected  (ResultCode=Success)
    Published to 'events/demo/x' with UserProperty k1=v1  (PUBACK=Success)

[3] Waiting for event delivery...
    Received payload:       {"message":"hello","channel":"demo.x","ts":"..."}
    Received UserProperty:  k1=v1
    User-property 'k1=v1' round-tripped as a KubeMQ Tag.

[4] Disconnecting...
    Done.
```

## What's Happening

1. **Subscriber connects** (MQTT 5.0, QoS 1, clean session) and subscribes to `events/demo/+`.
   KubeMQ registers a bridge entry matching all single-level subtopics under `demo.*`.

2. **Subscriber subscribes before publishing** — the 300 ms delay ensures the broker has
   registered the subscription before the publisher sends the message.

3. **Publisher connects** (separate MQTT 5.0 client) and publishes a JSON payload to
   `events/demo/x` at QoS 1 with User Property `k1=v1`.
   - KubeMQ stores `k1=v1` as a Tag on the Events message.
   - PUBACK `0x00` confirms the message was accepted.

4. **KubeMQ routes** the event to all matching bridge registry entries.
   The subscriber's `events/demo/+` filter matches `events/demo/x`, so the message is
   delivered back over MQTT with User Property `k1=v1` echoed from the KubeMQ Tag.

5. **Subscriber receives** the payload and prints the echoed user-property, proving the
   User-Props ↔ Tags round-trip (connector spec §2.9).

6. Both clients disconnect cleanly.

## MQTT Specifics

| Property | Value |
|---|---|
| Subscription topic | `events/demo/+` |
| Publish topic | `events/demo/x` |
| KubeMQ channel | `demo.x` |
| MQTT version | 5.0 |
| QoS | 1 (AtLeastOnce) |
| MQTT 5.0 User Property | `k1=v1` (published) → echoed as KubeMQ Tag |
| User-prop cap | 32 properties / 4096 bytes total; exceeding → PUBACK `0x97` |
| Wildcards | `+` (single-level MQTT) → `*` in KubeMQ; `#` (multi-level) → `>` |

### Reason codes to handle

| Code | Meaning |
|---|---|
| `0x00` | Success |
| `0x80` | Broker not ready — publish gated; retry after a short delay |
| `0x87` | Not authorized — ACL denied the publish or subscribe |
| `0x8F` | Topic filter invalid — empty segment in the topic |
| `0x90` | Topic name invalid — empty channel or `DefaultPattern=none` is configured |
| `0x97` | Quota exceeded — User Property cap (32 props / 4096 B) exceeded |
| `0xA2` | Wildcard subscriptions not supported — non-Events wildcard attempt |

## Gotchas

**Retain is silently dropped.**
A publish with `Retain=true` returns PUBACK `0x00` (success), but the message is never
delivered and an error is audited internally. There is no disconnect. Do NOT set
`Retain=true` on any publish. (A retained Will message at CONNECT receives CONNACK `0x9A`
and the connection is rejected — that is the only visible rejection.)

**Literal `.` in a topic segment conflates with `/`.**
`events/demo.x` and `events/demo/x` both map to KubeMQ channel `demo.x`. Avoid dots in
topic segments to prevent unintended channel aliasing and routing surprises.

**Overlapping filters multiply delivery.**
If the same subscriber (or multiple subscribers) register overlapping filters that all
match a single publish, KubeMQ delivers one copy per matching bridge registry entry.
There is no cross-entry deduplication in the current connector version.

**Events-Store is always StartNewOnly over MQTT.**
The `store/` prefix routes to the KubeMQ Events-Store pattern, but MQTT subscriptions
always start from "new only" — there is no historical replay over MQTT.

## Related Examples

- [events/wildcard-subscribe](../wildcard-subscribe/) — subscribe with `+` and `#` wildcards and observe N-copies delivery
- [events-store/start-new-only](../../events-store/start-new-only/) — persistent events with StartNewOnly semantics
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — compare v3.1.1 (no User Properties) against v5.0
