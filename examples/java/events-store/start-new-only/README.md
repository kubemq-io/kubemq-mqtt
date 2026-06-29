# java — Events-Store: Start-New-Only (no replay)

Demonstrates KubeMQ Events-Store over MQTT using the `store/` topic prefix.
This example explicitly proves the **StartNewOnly** constraint: messages published
before the subscriber connects are **not** delivered. There is no historical replay
available over MQTT.

## Prerequisites

- Java 21+
- Maven 3.8+
- KubeMQ server with the MQTT connector enabled
- `KUBEMQ_MQTT_URL` set to your broker address (default `tcp://localhost:1883`)

## How to Run

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
cd examples/java/events-store/start-new-only
mvn compile exec:java
```

## Expected Output

```
=== java — Events-Store: Start-New-Only (no replay) ===
Broker : tcp://127.0.0.1:1883
Topic  : store/demo/s
Pattern: Events-Store | StartNewOnly (ALWAYS — no replay over MQTT)
QoS    : 1
Proto  : MQTT 5.0

--- Step 1: Publish BEFORE subscribing (will NOT be delivered) ---
[pre-publisher] connected
[pre-publisher] PUBACK received (rc=0) — published to store/demo/s
[pre-publisher] payload: {"event":"login","user":"alice","ts":"2026-06-12T13:49:00.307526Z","note":"pre-subscribe — will NOT arrive"}
[pre-publisher] PUBACK received (rc=0) — published to store/demo/s
[pre-publisher] payload: {"event":"login","user":"bob","ts":"2026-06-12T13:49:00.318146Z","note":"pre-subscribe — will NOT arrive"}
[pre-publisher] PUBACK received (rc=0) — published to store/demo/s
[pre-publisher] payload: {"event":"login","user":"carol","ts":"2026-06-12T13:49:00.326369Z","note":"pre-subscribe — will NOT arrive"}
[pre-publisher] 3 messages published BEFORE subscriber connected.
[pre-publisher] These are stored in Events-Store but are NOT replayed over MQTT.

--- Step 2: Subscribe (StartNewOnly — no historical replay) ---
[subscriber] connected (reconnect=false) to tcp://127.0.0.1:1883
[subscriber] subscribed to store/demo/s (granted QoS 1)
[subscriber] StartNewOnly: only messages published FROM NOW are delivered.
[subscriber] The 3 pre-subscribe messages will NOT arrive — no replay.

--- Step 3: Publish AFTER subscribing (WILL be delivered) ---
[post-publisher] connected
[post-publisher] PUBACK received (rc=0) — published to store/demo/s
[post-publisher] payload: {"event":"login","user":"dave","ts":"2026-06-12T13:49:01.376193Z","note":"post-subscribe — WILL arrive"}

--- Step 4: Waiting for the post-subscribe message to arrive ---
[subscriber] message #1 received on store/demo/s
[subscriber] payload: {"event":"login","user":"dave","ts":"2026-06-12T13:49:01.376193Z","note":"post-subscribe — WILL arrive"}

=== Summary ===
Pre-subscribe messages published : 3  (NOT received — StartNewOnly)
Post-subscribe messages published: 1
Messages received by subscriber  : 1

MQTT Events-Store constraint confirmed:
  - Topic prefix 'store/' selects the KubeMQ Events-Store pattern.
  - Over MQTT, Events-Store is ALWAYS StartNewOnly.
  - Historical replay (StartFromFirst, StartFromSequence, etc.) is
    NOT available via MQTT — use the gRPC SDK for replay.
  - Retained messages are silently dropped (RetainAvailable=0).

Done.
```

## What's Happening

1. **Pre-publisher connects** and publishes **3 messages** to `store/demo/s`
   before any subscriber exists. KubeMQ stores these persistently in Events-Store,
   but because there are no subscribers at that moment, no delivery occurs.

2. **Subscriber connects** and subscribes to `store/demo/s` at QoS 1.
   The SUBACK grants QoS 1, confirming the subscription was accepted.
   A 400 ms settle ensures the subscription is fully registered server-side.

3. **Post-publisher connects** and publishes **1 message** to `store/demo/s`.
   This message is published _after_ the subscriber registered, so it is
   routed and delivered.

4. **Subscriber receives** exactly 1 message — the post-subscribe one.
   The 3 pre-subscribe messages never arrive, proving StartNewOnly semantics.

5. All three clients disconnect cleanly.

## MQTT Specifics

| Property              | Value                                        |
|-----------------------|----------------------------------------------|
| Topic string          | `store/demo/s`                               |
| KubeMQ channel        | `demo.s` (`/` becomes `.`)                   |
| KubeMQ pattern        | Events-Store                                 |
| Replay mode           | **StartNewOnly** (always, hardwired over MQTT)|
| QoS                   | 1                                            |
| MQTT version          | 5.0                                          |
| MQTT 5.0 properties   | None used (User Properties / Tags not needed)|
| Retain                | `false` — retain is forced off (PUBACK 0x00, message silently dropped if set) |

### Relevant SUBACK reason codes

| Code   | Meaning                                                                     |
|--------|-----------------------------------------------------------------------------|
| `0x00` | Success, granted QoS 0                                                      |
| `0x01` | Success, granted QoS 1                                                      |
| `0x02` | Success, granted QoS 2                                                      |
| `0x8F` | Topic filter invalid — empty segment in the channel path                    |
| `0x90` | Topic name invalid — empty channel (e.g. bare `store/` with no channel)     |
| `0xA2` | Wildcard subscriptions not supported — wildcards are only allowed on `events/` |

### Gotcha: StartNewOnly is the only replay mode over MQTT

The KubeMQ gRPC SDK supports 6 replay positions (StartFromFirst, StartFromLast,
StartFromSequence, StartFromTime, StartFromTimeDelta, StartNewOnly). Over MQTT the
connector **hardwires StartNewOnly** — there is no way to request any other start
position from an MQTT client. If you need historical replay, use the gRPC SDK
(e.g. `kubemq-go` or `kubemq-java`).

### Gotcha: Retain is silently dropped

Setting `retained = true` on a publish results in a **PUBACK 0x00** (success), but
the message is **silently discarded** — it is not stored or delivered. The broker
advertises `RetainAvailable = 0` at CONNACK time. Do not rely on retained messages
with this connector.

## Related Examples

- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget events (no persistence, replay N/A)
- [events/wildcard-subscribe](../../events/wildcard-subscribe/) — wildcard subscriptions on Events
- [queues/shared-consume](../../queues/shared-consume/) — competing consumers with MQTT `$share` and ACK
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — MQTT 3.1.1 vs 5.0 comparison
