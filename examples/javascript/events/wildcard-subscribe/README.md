# JavaScript/TypeScript — Events: Wildcard Subscribe

Demonstrates overlapping events wildcard subscriptions and the per-bridge-entry copy semantics (gotcha #6), scoped to a per-run **unique** channel `events/leak/<id>/...` for deterministic behavior on a shared/busy broker. Also shows that non-events wildcard subscribes are rejected with SUBACK reason code 0xA2.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server with MQTT connector enabled on port 1883

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
npx tsx events/wildcard-subscribe/index.ts
```

## Expected Output

> Regenerated from a live run during the LiveTest stage. `<id>` is a per-run unique hex string (a fresh value each run).

```
[client] connected
[client] per-run unique channel: events/leak/<id>/x
[client] subscribed unique filters: events/leak/<id>/x qos=1, events/leak/<id>/# qos=1, events/leak/<id>/+ qos=1
[client] attempting non-events wildcard subscribe (store/#) — expect SUBACK 0xA2...
[client] store/# correctly rejected with SUBACK 0xA2 (wildcard-subscriptions-not-supported)
[client] published once to events/leak/<id>/x
[copy #1] topic: events/leak/<id>/x | payload: {"event":"overlap-test","topic":"events/leak/<id>/x"}
[copy #2] topic: events/leak/<id>/x | payload: {"event":"overlap-test","topic":"events/leak/<id>/x"}
[copy #3] topic: events/leak/<id>/x | payload: {"event":"overlap-test","topic":"events/leak/<id>/x"}

=== Results ===
[PASS] received 3 copies of events/leak/<id>/x (>= 3 — one per matching bridge registry entry)
       No cross-entry dedup in V1 (gotcha #6 / test #25).
```

## What's Happening

- One subscriber registers **three overlapping filters**, all scoped to a **per-run unique** id `events/leak/<id>/...` so no prior or concurrent run reuses the channel (deterministic on a shared/busy broker):
  - `events/leak/<id>/x` (exact) → KubeMQ `leak.<id>.x`
  - `events/leak/<id>/#` (multi-level, `#` → `>`) → KubeMQ `leak.<id>.>`
  - `events/leak/<id>/+` (single-level, `+` → `*`) → KubeMQ `leak.<id>.*`
- Each distinct subscribe filter creates an independent bridge registry entry.
- A single publish to `events/leak/<id>/x` matches all three filters, so it injects once per entry into the KubeMQ Events bus — the subscriber receives **3 copies** (test #25: `wantCopies = 3`). The example counts only copies of its own unique publish topic and asserts `>= 3`; on a busy broker a foreign broad-wildcard subscriber may add extra copies (gotcha #6, reported, not failed).
- MQTT wildcards `+` and `#` map to KubeMQ wildcards `*` and `>` respectively, but only on the Events pattern.
- Subscribing `store/#` (non-Events) returns SUBACK reason 0xA2 (`wildcard-subscriptions-not-supported`).

## MQTT Specifics

| Property | Value |
|----------|-------|
| Protocol version | MQTT 5.0 |
| Publish topic | `events/leak/<id>/x` (per-run unique id) |
| Subscribe filters | `events/leak/<id>/x`, `events/leak/<id>/#`, `events/leak/<id>/+` (3 overlapping, each QoS 1) |
| KubeMQ channels | `leak.<id>.x`, `leak.<id>.>`, `leak.<id>.*` |
| QoS | 1 |
| Expected copies | 3 (one per matching bridge registry entry; `>= 3` on a busy broker) |
| Negative path | `store/#` subscribe → SUBACK 0xA2 |

## Gotchas Referenced

- Gotcha #6: overlapping events wildcard filters — N matching bridge entries → N copies delivered; no cross-entry dedup in V1.

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — single subscription, v5 user-props
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — wildcard behavior is the same across protocol versions for Events
