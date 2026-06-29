# csharp — Events: Wildcard Subscribe

Demonstrates MQTT wildcard subscriptions over KubeMQ Events (MQTT 5.0).

This example shows the **N-copies-per-matching-entry** behaviour: three overlapping event
filters registered on one client each create an independent bridge registry entry.
Publishing `events/leak/<id>/x` once injects the message through every matching
entry, so the subscriber receives **at least 3 copies** — one per matching bridge
entry, with no cross-entry dedup (V1 behaviour, gotcha #6).

Each run mints a **per-run unique 8-char id** so the filters target a freshly
minted KubeMQ channel that no prior/other run reuses. This keeps the copy count
deterministic on a busy/shared broker (a literal fixed `events/leak/x` would
over-count when another process holds a broad wildcard).

It also shows that wildcards on non-events patterns are **rejected** with
SUBACK `0xA2` (wildcard-subscriptions-not-supported).

## Prerequisites

- .NET 8 or later
- A running KubeMQ server with the MQTT connector enabled
  (default `tcp://localhost:1883`)

## How to Run

```bash
cd examples/csharp/events/wildcard-subscribe
dotnet run
# or with an explicit broker URL:
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run
```

## Expected Output

> Regenerated from a real run in the LiveTest stage. `<id>` is a per-run unique
> 8-char hex value.

```
[sub] Connected to 127.0.0.1:1883
[sub] Per-run unique channel: events/leak/<id>/x
[sub] SUBACK results:
  Filter 'events/leak/<id>/x' -> GrantedQoS1
  Filter 'events/leak/<id>/#' -> GrantedQoS1
  Filter '#' -> GrantedQoS1
[sub] Three overlapping filters registered — each creates an independent bridge entry.
      (bare '#' is INFORMATIONAL: on a busy broker it adds extra matching entries.)

[demo] Attempting non-events wildcard 'store/#' (expect SUBACK 0xA2)...
  Filter 'store/#' -> SUBACK 0xA2 (WildcardSubscriptionsNotSupported) — wildcard-subscriptions-not-supported as expected

[pub] Connected to 127.0.0.1:1883
[pub] Published ONE message to 'events/leak/<id>/x'
[pub] Expecting >= 3 copies (one per matching bridge registry entry)

[sub] Received 3 copy/copies of the published message:
  copy 1: topic='events/leak/<id>/x'  payload={"message":"overlap-test","topic":"events/leak/<id>/x","ts":"..."}
  copy 2: topic='events/leak/<id>/x'  payload={"message":"overlap-test","topic":"events/leak/<id>/x","ts":"..."}
  copy 3: topic='events/leak/<id>/x'  payload={"message":"overlap-test","topic":"events/leak/<id>/x","ts":"..."}

[ok] Received 3 copies (>= 3) — one per matching bridge registry entry (no cross-entry dedup, V1 / gotcha #6).

Wildcard mapping summary:
  MQTT '+'   -> KubeMQ '*'  (single-level wildcard)
  MQTT '#'   -> KubeMQ '>'  (multi-level wildcard)
  Wildcards on non-events patterns -> SUBACK 0xA2
```

## What's Happening

1. **Per-run unique id** is generated so all filters target a fresh KubeMQ
   channel `leak.<id>.*` that no other run reuses.
2. **Subscriber connects** (MQTT 5.0) and sends a single `SUBSCRIBE` packet with
   three overlapping topic filters:
   - `events/leak/<id>/x` — exact match; KubeMQ channel `leak.<id>.x`
   - `events/leak/<id>/#` — scoped multi-level; KubeMQ channel `leak.<id>.>`
   - `#` — bare multi-level; KubeMQ channel `>` — **INFORMATIONAL only** (also
     matches; on a busy broker it adds extra matching entries that vary with
     ambient traffic)
3. Each filter is granted `QoS 1` (SUBACK result `GrantedQoS1`). The connector
   registers **one bridge entry per filter**.
4. **Non-events wildcard rejection demo**: subscribing to `store/#` returns
   SUBACK `0xA2` because wildcards are only permitted on the `events` prefix.
5. **Publisher connects** and publishes a single message to `events/leak/<id>/x`
   at QoS 1.
6. The broker routes the publish through the KubeMQ Events bus. All three bridge
   entries (`leak.<id>.x`, `leak.<id>.>`, `>`) match the channel `leak.<id>.x`.
   Each entry independently injects the message back into the mochi broker, which
   fans it out to every locally-matching subscriber.
7. **The subscriber receives at least 3 copies** — one per matching bridge entry.
   The example counts ONLY copies of its own unique publish topic and asserts
   `>= 3`; extra copies (from a foreign broad-wildcard subscriber on the global
   `>` channel) are reported, not failed — that is gotcha #6 itself. This is
   permitted by MQTT 5.0 §3.3.5 (one copy per matching subscription).
8. The example exits cleanly.

## MQTT Specifics

| Property          | Value                                              |
|-------------------|----------------------------------------------------|
| MQTT version      | 5.0 (`MqttProtocolVersion.V500`)                   |
| Subscribe topic 1 | `events/leak/<id>/x` (exact; KubeMQ `leak.<id>.x`) |
| Subscribe topic 2 | `events/leak/<id>/#` (scoped; KubeMQ `leak.<id>.>`) |
| Subscribe topic 3 | `#` (bare multi-level; KubeMQ `>`; INFORMATIONAL)  |
| Publish topic     | `events/leak/<id>/x`                               |
| QoS               | 1 (`AtLeastOnce`) on both subscribe and publish    |
| MQTT 5.0 props    | None for this example (no User Properties)         |
| Non-events wild   | `store/#` -> SUBACK `0xA2`                         |
| Assertion         | `>= 3` copies of the unique publish topic          |

## Gotchas

- **Overlapping filters deliver N copies (gotcha #6):** Each distinct subscribe
  filter registers an independent bridge entry in the KubeMQ-side registry. When
  a publish matches M entries it is injected M times, so the subscriber receives
  M copies. There is no cross-entry dedup in V1. This is by design and permitted
  by MQTT 5.0 §3.3.5.

- **Per-run unique id keeps the count deterministic:** the bare `#` is treated as
  informational because, on a busy/shared broker, a foreign broad-wildcard
  subscriber maps to the same global `>` channel and adds extra matching entries.
  Scoping the two asserted filters to `events/leak/<id>/...` guarantees at least
  3 copies regardless of ambient traffic.

- **Wildcards are Events-only:** `+` and `#` may only appear in topic filters
  that route to the Events pattern (prefix `events/`, or bare `#`/`#/...`). Any
  wildcard filter on another pattern (`store/`, `queues/`, `commands/`,
  `queries/`) returns SUBACK `0xA2`.

- **`+` maps to `*` and `#` maps to `>`:** KubeMQ uses its own wildcard syntax
  internally. The connector translates: MQTT `+` (single-level) -> KubeMQ `*`;
  MQTT `#` (multi-level) -> KubeMQ `>`.

- **Topic `/` becomes `.` in the KubeMQ channel:** `events/leak/<id>/x` maps to
  KubeMQ channel `leak.<id>.x`. A literal `.` in a topic segment is treated
  identically to `/`, which can cause ambiguity — avoid `.` in topic segments.

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — basic pub/sub without wildcards
- [events-store/start-new-only](../../events-store/start-new-only/) — Events-Store (StartNewOnly; no historical replay over MQTT)
