# rust — Events: Wildcard Subscribe

Demonstrates MQTT wildcard subscriptions (`+` and `#`) on KubeMQ Events using
`rumqttc` (MQTT 5.0).  Three overlapping event filters
each create an independent bridge registry entry; a single publish matching all
three entries delivers **exactly 3 copies** to each subscriber whose filter
matches — one per matching entry, with no cross-entry dedup.

The example uses a **per-run unique channel** `events/leak/<id>/...` (where
`<id>` is a fresh UUID prefix each run) so that a busy or shared broker cannot
over-count this example's copies. Two filters are scoped to that unique id
(asserted **exactly 3**), and a bare `#` filter is **informational** (asserted
**≥ 3**, since it also catches ambient Events traffic).

## Prerequisites

- Rust 1.75+ (stable)
- KubeMQ server with MQTT connector enabled on port 1883

## How to Run

```bash
cd examples/rust
KUBEMQ_MQTT_URL=tcp://localhost:1883 cargo run -p wildcard-subscribe
```

## Expected Output

> Regenerated from a live run in the LiveTest stage. `<id>` is a per-run UUID prefix; topic strings below are canonical.

```
=== KubeMQ MQTT — Events: Wildcard Subscribe ===
Broker: tcp://localhost:1883

Wire contract:
  '+' maps to '*' server-side  (single-level wildcard)
  '#' maps to '>' server-side  (multi-level wildcard)
  Wildcards allowed ONLY on events/... subscriptions.
  Non-events wildcard (e.g. store/#) -> SUBACK 0xA2.

Per-run unique channel: events/leak/<id>/x

[wild ] subscribing '#'                  -> KubeMQ channel filter '>' (informational)
[sub1 ] subscribing 'events/leak/<id>/x' -> KubeMQ exact channel 'leak.<id>.x'
[sub2 ] subscribing 'events/leak/<id>/#' -> KubeMQ channel filter 'leak.<id>.>'

[rej  ] subscribing 'store/#'            -> expect SUBACK 0xA2

Waiting 700 ms for subscriptions to propagate...
[wild ] SUBACK[0] = granted QoS AtLeastOnce
[sub1 ] SUBACK[0] = granted QoS AtLeastOnce
[sub2 ] SUBACK[0] = granted QoS AtLeastOnce
[rej  ] SUBACK[0] = 0xA2 WildcardSubscriptionsNotSupported (expected for non-events wildcard)
Publishing to 'events/leak/<id>/x' ONCE (QoS 1)...
  Three matching bridge entries -> each subscriber gets 3 copies.
Published.

[sub1 ] filter='events/leak/<id>/x' received on 'events/leak/<id>/x': wildcard-overlap-demo
[sub2 ] filter='events/leak/<id>/#' received on 'events/leak/<id>/x': wildcard-overlap-demo
[wild ] filter='#' received on 'events/leak/<id>/x': wildcard-overlap-demo
(... 3 copies each ...)
Results:
  [sub1 ] filter 'events/leak/<id>/x' received 3 copies (want exactly 3)
  [sub2 ] filter 'events/leak/<id>/#' received 3 copies (want exactly 3)
  [wild ] filter '#' (informational)          received 3 copies (want >= 3)

THREE_COPIES_CONFIRMED — one copy per matching bridge registry entry
```

> The order of incoming messages may differ from the listing above.  The SUBACK
> lines from the background event-loop tasks may appear before or after the
> "Waiting…" line. On a busy/shared broker the bare `#` filter may report more
> than 3 copies (ambient Events traffic) — that is informational, not a failure.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 |
| Subscribe — sub1 (UNIQUE) | `events/leak/<id>/x` (exact topic; maps to KubeMQ channel `leak.<id>.x`) |
| Subscribe — sub2 (UNIQUE) | `events/leak/<id>/#` (multi-level; maps to KubeMQ channel filter `leak.<id>.>`) |
| Subscribe — wild (informational) | `#` (matches everything; maps to KubeMQ channel filter `>`) |
| Publish topic | `events/leak/<id>/x` (per-run unique; published ONCE) |
| QoS | 1 (AtLeastOnce) on all subscriptions and publish |
| MQTT 5.0 User Properties | not used in this example |
| KubeMQ pattern | Events |
| Rejection contrast | `store/#` → SUBACK `0xA2` (Wildcard Subscriptions Not Supported) |
| Assertion | sub1 == 3 and sub2 == 3 (exact); wild ≥ 3 (informational) |

## What's Happening

1. **Per-run unique id** — a fresh UUID prefix `<id>` is generated so the three
   overlapping filters target a freshly minted KubeMQ channel
   `leak.<id>.…` that no prior or concurrent run reuses.

2. **Three subscriber clients** connect, each registering one filter:
   - **sub1** (UNIQUE): `events/leak/<id>/x` — exact topic.  KubeMQ channel
     `leak.<id>.x`.
   - **sub2** (UNIQUE): `events/leak/<id>/#` — multi-level wildcard scoped to the
     unique id.  KubeMQ channel filter `leak.<id>.>`.
   - **wild** (INFORMATIONAL): `#` — bare hash with no prefix uses the
     DefaultPattern (events).  Server-side channel filter `>`.  This also catches
     ambient Events traffic on a shared broker, so it is informational only.

3. **Rejection contrast**: a fourth throwaway client subscribes to `store/#`
   (a non-events topic with a wildcard).  The broker returns SUBACK reason code
   `0xA2` (Wildcard Subscriptions Not Supported) because wildcards are only
   permitted on Events subscriptions.

4. After a **700 ms propagation delay** (giving the broker time to register
   all three bridge entries), the publisher sends a single MQTT PUBLISH to
   `events/leak/<id>/x` at QoS 1.

5. The KubeMQ MQTT connector routes the publish through its bridge registry.
   All three entries (`events/leak/<id>/x`, `events/leak/<id>/#`, `#`) match.
   The connector injects the message **once per matching entry** into the mochi
   broker, which then fans it out to all locally-subscribed clients.  Because
   each of the three subscriber clients has a subscription that matches
   `events/leak/<id>/x`, each one receives **3 copies** — one per matching bridge
   registry entry.  There is no cross-entry deduplication in V1 (gotcha #6).

6. The program asserts `received == 3` for the two UNIQUE filters (sub1, sub2)
   and `received >= 3` for the informational bare `#` filter, then prints
   `THREE_COPIES_CONFIRMED` before exiting cleanly.

## Gotchas

- **Gotcha #6 — N overlapping filters → N copies (no cross-entry dedup)**: the
  3-copy behaviour is by design.  Each distinct SUBSCRIBE filter creates its
  own bridge registry entry.  A publish matching M entries injects the message
  M times; every locally-subscribed client matching the re-injected topic
  receives M copies.  This is permitted by MQTT 5.0 §3.3.5.

- **Per-run unique channel for CI determinism**: a fixed channel (e.g. a literal
  `events/leak/x`) plus a bare `#` over-counts on a busy/shared broker because
  any other broad-wildcard subscriber also fans into it.  Scoping the asserted
  filters to a per-run unique `<id>` keeps the exact-3 assertion deterministic;
  the bare `#` is treated as informational (≥ 3).

- **Wildcards on non-events patterns are rejected**: any wildcard (`+` or `#`)
  in a subscribe topic that resolves to a non-events pattern (e.g. `store/#`,
  `queues/+/foo`, `commands/#`) returns SUBACK `0xA2` immediately.

- **`#` must be the final segment**: topics such as `events/#/sub` are
  invalid; the broker returns `0x8F` (topic-filter-invalid).

- **Retain is silently dropped**: a PUBLISH with the Retain flag set receives
  PUBACK `0x00` (success) but the message is silently discarded — it is never
  delivered and is not stored.  This is separate from the wildcard behaviour
  but worth noting for any subscriber that expects retained state.

- **`/` and `.` conflate in channel names**: a topic segment containing a
  literal `.` (e.g. `events/site.a/temp`) maps to the same KubeMQ channel as
  `events/site/a/temp` (`site.a.temp` vs `site.a.temp`).  Use pure
  `/`-delimited topics to avoid this ambiguity.

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — single subscription, single-level `+` wildcard
- [events-store/start-new-only](../../events-store/start-new-only/) — persistent events (StartNewOnly; no historical replay over MQTT)
- [connectivity/tls](../../connectivity/tls/) — same Events flow over TLS
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — protocol version differences
