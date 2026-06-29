# java — Events: Wildcard Subscribe

Demonstrates MQTT wildcard topic filters on KubeMQ Events subscriptions using MQTT 5.0.

Mirrors the Go
reference (`examples/go/events/wildcard-subscribe/main.go`): subscribes three overlapping
event filters — **all scoped to a per-run unique id** for determinism on a busy/shared
broker — publishes **once**, and observes **exactly 3 copies** delivered to each of the two
unique-scoped subscribers, one per matching bridge registry entry, with no cross-entry
deduplication. A bare `#` subscriber is included purely as **informational** context.

## Prerequisites

- Java 21+, Maven 3.8+
- KubeMQ server with the MQTT connector enabled (default port 1883)
- `KUBEMQ_MQTT_URL` pointing to the broker (see below)

## How to Run

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
cd examples/java/events/wildcard-subscribe
mvn compile exec:java
```

The example runs to completion and exits. No external processes required.

## Expected Output

> Regenerated from a real run in the LiveTest stage. The `<id>` below is a per-run
> unique suffix; the two unique-scoped filters (`events/leak/<id>/x` and
> `events/leak/<id>/#`) each receive **exactly 3** copies; the bare `#` filter is
> informational (`>= 3`).

```
=== events/wildcard-subscribe ===
Broker: tcp://localhost:1883

Mirrors integration test #25:
  Three overlapping Events filters -> 3 copies per subscriber per publish.

Channel path   : leak/54dfd8
Publish topic  : events/leak/54dfd8/x
Unique '#'     : events/leak/54dfd8/#

--- Step 1: Connect subscriber clients ---
[wild(#)] connected to tcp://localhost:1883
[sub1(exact)] connected to tcp://localhost:1883
[sub2(unique/#)] connected to tcp://localhost:1883

--- Step 2: Subscribe ---
  Subscribing [sub1(exact)] to filter='events/leak/54dfd8/x' QoS=1 ...
  SUBACK [sub1(exact)]: granted QoS 1 (0x01) — subscription active
  Subscribing [sub2(unique/#)] to filter='events/leak/54dfd8/#' QoS=1 ...
  SUBACK [sub2(unique/#)]: granted QoS 1 (0x01) — subscription active
  Subscribing [wild(#) [informational]] to filter='#' QoS=1 ...
  SUBACK [wild(#) [informational]]: granted QoS 1 (0x01) — subscription active

  Note: '+' -> '*' in KubeMQ (single-level wildcard)
        '#' -> '>' in KubeMQ (multi-level wildcard, must be last segment)
  Wildcards are supported ONLY on Events subscriptions.
  Wildcard on store/queues/commands/queries -> SUBACK 0xA2.

--- Step 3: Demonstrate wildcard rejection on Events-Store (SUBACK 0xA2) ---
[reject-demo] connected to tcp://localhost:1883
  Subscribing [store/# (expect 0xA2)] to filter='store/#' QoS=1 ...
  SUBACK [store/# (expect 0xA2)]: 0xA2 — Wildcard Subscriptions Not Supported (non-Events wildcard rejected; Paho disconnects client on error SUBACK)
[reject-demo] disconnected: Invalid Return code

--- Step 4: Publish one message to events/leak/54dfd8/x ---
[publisher] connected to tcp://localhost:1883
Published: topic='events/leak/54dfd8/x' payload='overlap' QoS=1

--- Step 5: Waiting for 3 matching copies on sub1 and sub2 ---
[sub1(exact)] copy #1 arrived on topic='events/leak/54dfd8/x' payload='overlap'
[sub2(unique/#)] copy #1 arrived on topic='events/leak/54dfd8/x' payload='overlap'
[wild(#)] copy #1 arrived on topic='events/leak/54dfd8/x' payload='overlap'
[sub1(exact)] copy #2 arrived on topic='events/leak/54dfd8/x' payload='overlap'
[sub2(unique/#)] copy #2 arrived on topic='events/leak/54dfd8/x' payload='overlap'
[wild(#)] copy #2 arrived on topic='events/leak/54dfd8/x' payload='overlap'
[sub1(exact)] copy #3 arrived on topic='events/leak/54dfd8/x' payload='overlap'
[sub2(unique/#)] copy #3 arrived on topic='events/leak/54dfd8/x' payload='overlap'
[wild(#)] copy #3 arrived on topic='events/leak/54dfd8/x' payload='overlap'

=== Results ===
sub1 (filter 'events/leak/54dfd8/x') received: 3 matching copy(ies)
sub2 (filter 'events/leak/54dfd8/#') received: 3 matching copy(ies)
wild (filter '#', informational) received: 3 matching copy(ies) (>= 3 expected)

PASS: sub1 and sub2 each received exactly 3 copies — one per matching bridge registry entry. No cross-entry dedup.

--- Step 6: Disconnecting ---
Done.
```

## What's Happening

1. **Three subscriber clients connect** over MQTT 5.0 with distinct client IDs.

2. **Three overlapping subscriptions are created** (each on a separate client so the
   overlapping semantics are unambiguous). The two unique-scoped filters are asserted
   exactly; the bare `#` is informational only:

   | Client | MQTT filter | KubeMQ channel pattern | Asserted? |
   |--------|-------------|------------------------|-----------|
   | `sub1` | `events/leak/<id>/x` | `leak.<id>.x` (exact) | yes — exactly 3 |
   | `sub2` | `events/leak/<id>/#` | `leak.<id>.>` (unique subtree) | yes — exactly 3 |
   | `wild` | `#` | `>` (all channels — DefaultPattern=events) | no — informational (`>= 3`) |

3. **A wildcard subscribe to `store/#`** (Events-Store) is demonstrated on a temporary
   client to return **SUBACK 0xA2** (Wildcard Subscriptions Not Supported) — wildcards
   are only allowed on Events-pattern subscriptions. Paho v5 surfaces this as an
   `MqttException` thrown from `waitForCompletion()` and automatically disconnects the client.

4. **One publish** to `events/leak/<id>/x` hits all three bridge registry entries.
   Each entry independently injects the message back into mochi-mqtt, which fans
   it out to every locally-matching subscriber. Result: **3 copies** on each of the
   two unique-scoped subscribers (the bare `#` client sees `>= 3`, plus any ambient
   `>` traffic on a busy broker).

5. **All clients disconnect** and the program exits cleanly.

## MQTT Specifics

| Property | Value |
|----------|-------|
| Protocol | MQTT 5.0 (Paho `org.eclipse.paho.mqttv5.client`) |
| Transport | `tcp://` (or `ws://` / `ssl://` via `KUBEMQ_MQTT_URL`) |
| QoS | 1 (all subscriptions and the publish) |
| Wildcards used | `events/leak/<id>/x` (exact), `events/leak/<id>/#` (unique multi-level), `#` (bare, informational) |
| KubeMQ `#` maps to | `>` in KubeMQ channel wildcards |
| KubeMQ `+` maps to | `*` in KubeMQ channel wildcards |
| Retain | `false` — retain is forced off (`RetainAvailable=0`); a retained publish would be silently dropped with PUBACK success |
| MQTT version | Level 5 (`cleanStart=true`, keepalive 30 s) |
| User Properties | Not used in this example (v5 only; mapped to KubeMQ Tags) |
| exec plugin | `fork=true` + `-Duser.language=en` to workaround Paho 1.2.5 NLS bundle missing `messages_en.properties` |

## Gotchas

**N overlapping filters = N copies (no dedup)**
Each distinct subscription filter creates an independent entry in the connector's
bridge registry. When a publish matches M entries, M independent copies are injected
into mochi, and every subscriber whose local filter matches that topic receives M
messages. This is intentional (gotcha #6; MQTT 5.0 §3.3.5) and will be
fixed by cross-entry dedup in a future version. Design accordingly: avoid subscribing
the same client with multiple overlapping filters unless you want duplicate delivery.

**`#` on a non-Events topic returns SUBACK 0xA2**
`store/#`, `queues/#`, `commands/#`, `queries/#` all return reason code `0xA2`
(Wildcard Subscriptions Not Supported). Only Events-pattern subscriptions accept
MQTT wildcards. Paho v5 throws `MqttException` and disconnects the client when it
receives an error SUBACK — always check `isConnected()` before calling `disconnect()`
after a potentially-rejected subscribe.

**Literal `.` in a topic segment conflates with `/`**
`events/a.b/c` and `events/a/b/c` both map to KubeMQ channel `a.b.c`. Avoid
dots in MQTT topic segment names when targeting KubeMQ channels.

**Retain is silently dropped**
If you set `MqttMessage.setRetained(true)`, the broker accepts the publish
(PUBACK 0x00), but the message is silently discarded — it is never delivered and
never stored. No error is returned to the publisher.

**Bare `#` subscriber sees Events traffic only — not Events-Store/Queues**
A bare `#` subscription uses DefaultPattern=events and maps to KubeMQ channel `>`.
It only matches Events bridge registry entries. Events-Store traffic published to
`store/<ch>` does not leak to it; the patterns use independent bridge registries.
Because a bare `#` matches **all** Events channels, this example treats it as
informational (`>= 3`) rather than asserting an exact count.

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — single channel publish/subscribe, no wildcards
- [events-store/start-new-only](../../events-store/start-new-only/) — persistent events (always StartNewOnly over MQTT; no historical replay)
- [queues/shared-consume](../../queues/shared-consume/) — `$share/<group>/queues/<ch>` consume pattern
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — v3.1.1 vs v5.0 capability comparison
