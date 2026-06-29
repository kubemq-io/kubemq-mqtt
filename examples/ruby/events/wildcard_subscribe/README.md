# Ruby — Events: Wildcard Subscribe

Demonstrates MQTT wildcard subscriptions on the KubeMQ Events pattern, and illustrates **gotcha #6**: overlapping wildcard filters each deliver one copy per matching bridge registry entry — a single publish matching N registered filters produces N delivered copies with no cross-entry deduplication.

This example uses a **per-run unique channel** (`events/leak/<id>/...`) so the copy count is deterministic on a busy/shared broker, mirroring the Go and Java reference examples (integration test #25).

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with MQTT connector enabled (port 1883)
- `mqtt` gem ~> 0.7

## How to Run

```bash
cd examples/ruby
bundle install
export KUBEMQ_MQTT_URL=tcp://localhost:1883
ruby events/wildcard_subscribe/main.rb
```

## Expected Output

Captured from a live run against `tcp://127.0.0.1:1883` (the `<id>` and the payload suffix are randomized per run; here `<id>` = `13a1a320`). Verified deterministically green across 3 consecutive runs.

```
=== Events wildcard overlap: one copy per matching bridge entry ===
Per-run unique channel : events/leak/13a1a320/x
Publish topic          : events/leak/13a1a320/x
Subscribers            : 3 separate clients, one filter each — "events/leak/13a1a320/x", "events/leak/13a1a320/#", "#"
  '+' maps to KubeMQ '*' (single-level wildcard)
  '#' maps to KubeMQ '>' (multi-level wildcard)
  Non-events wildcards (store/#, queues/#, ...) return SUBACK 0xA2.
Each distinct filter is an independent bridge entry; all three match the publish.

Published once to: events/leak/13a1a320/x

Copies of our publish received per subscriber:
  filter="events/leak/13a1a320/x" copies=3  (unique, expected 3)
  filter="events/leak/13a1a320/#" copies=3  (unique, expected 3)
  filter="#"                      copies=3  (informational, >= 3)

OK — gotcha #6 confirmed: 3 matching bridge entries -> 3 copies on each unique filter, no cross-entry dedup.

Contrast: subscribing to a non-events wildcard 'store/#' would return
          SUBACK 0xA2 (wildcard-subscriptions-not-supported). Wildcards are
          accepted ONLY on Events subscriptions; store/#, queues/#,
          commands/#, queries/# are all rejected with 0xA2 (integration test #14).
```

## What's Happening

A per-run unique id scopes a fresh KubeMQ channel so the count is not perturbed by prior/other runs on a shared broker:

- `pub_topic` = `events/leak/<id>/x` — the single publish target.

Three **separate** subscriber clients are used, each registering exactly **one** event filter (distinct `client_id` → distinct bridge entry):

1. `events/leak/<id>/x` — **unique** exact match → KubeMQ `leak.<id>.x`
2. `events/leak/<id>/#` — **unique** multi-level wildcard scoped to `<id>` → KubeMQ `leak.<id>.>`
3. `#` — bare multi-level wildcard, **informational only** → KubeMQ `>`

A single publish to `events/leak/<id>/x` routes through the array to all three matching entries, and each entry re-injects the topic back into the broker. With no cross-entry deduplication, **every** subscriber whose filter matches receives one copy **per matching bridge entry**:

- The two **unique** filters (exact + scoped `#`) match only the three entries above, so each receives **exactly 3 copies** (`wantCopies = 3`, integration test #25, `connector_integration_test.go:1176-1207`).
- The bare `#` filter also matches every Events topic, so it receives the same 3 copies **plus** any ambient Events traffic on a shared broker. It is therefore treated as **informational** (asserted `>= 3`, not `== 3`).

> **Why three clients, not one client with three filters?** MQTT collapses overlapping filters registered on the *same* client into a single delivery (per-client subscription dedup), so stacking the three filters on one client would yield only one copy of the publish. The "one copy per matching bridge entry" contract is about distinct bridge entries (distinct client subscriptions), which is why this example mirrors the canonical test by using three separate clients.

The example also documents the contrast: a wildcard subscribe on a non-events pattern (e.g. `store/#`) is rejected with SUBACK `0xA2` (wildcard subscriptions not supported on non-events patterns — integration test #14). The njh/ruby-mqtt gem (MQTT 3.1.1) issues `SUBSCRIBE` fire-and-forget and does not surface the SUBACK reason code, so this contrast is **documented** here rather than asserted; the Go, Java, Python, JavaScript, C#, and Rust examples assert the `0xA2` reject directly.

## MQTT Specifics

| Property | Value |
|----------|-------|
| Filters (one per client) | `events/leak/<id>/x` (unique exact), `events/leak/<id>/#` (unique multi-level), `#` (informational) |
| Subscriber clients | 3 (one filter each) |
| Publish topic | `events/leak/<id>/x` (per-run unique) |
| QoS | 1 |
| MQTT version | 3.1.1 (mqtt gem) |
| KubeMQ pattern | Events |
| Copies per unique filter | exactly 3 (asserted) |
| Copies on bare `#` | ≥ 3 (informational — varies with ambient traffic) |
| Non-events wildcard SUBACK | `0xA2` |
| `+` → KubeMQ `*` | single-level wildcard |
| `#` → KubeMQ `>` | multi-level wildcard |

## Gotcha #6 — Overlapping Events Wildcard Filters

Each distinct subscribe filter (on a distinct client) creates an independent bridge registry entry in the connector. A publish matching M entries injects M times — so every subscriber whose filter matches that publish receives M copies. There is **no cross-entry deduplication in V1**. (Overlapping filters stacked on a *single* client are collapsed by MQTT into one delivery, so distinct bridge entries require distinct client subscriptions.) Scoping the filters to a per-run unique `<id>` keeps the asserted count deterministic; the bare `#` filter additionally absorbs ambient Events traffic on a shared broker, which is gotcha #6 itself, not an error.

## Related Examples

- [events/basic_pubsub](../basic_pubsub/) — single filter pub/sub
- [events_store/start_new_only](../../events_store/start_new_only/) — persistent events (no replay)
