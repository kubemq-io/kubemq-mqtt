# python — Events: Wildcard Subscribe

Demonstrates MQTT wildcard topic subscriptions on the KubeMQ Events pattern,
including the critical **overlapping-filter duplication** behaviour (gotcha #6).

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` (from `examples/python/`)
- KubeMQ server with MQTT connector enabled

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
export KUBEMQ_MQTT_URL=tcp://localhost:1883
python events/wildcard_subscribe/main.py
```

## Expected Output

Captured from a live run against `tcp://127.0.0.1:1883`. `<id>` is a per-run
unique id (here `3f545b`); the line order may vary, and the copy count is `>= 3`
— in this run the three unique filters each deliver one copy and the bare `#`
filter adds exactly one more (4 total). A busier broker with other broad-wildcard
subscribers on the global `>` channel can add still more.

```
[subscriber] subscribe('events/leak/3f545b/x', qos=1)  mid=1  rc=0
[subscriber] subscribe('events/leak/3f545b/#', qos=1)  mid=2  rc=0
[subscriber] subscribe('events/leak/3f545b/+', qos=1)  mid=3  rc=0
[subscriber] subscribe('#', qos=1)  mid=4  rc=0
[subscriber] SUBACK mid=1  code=0x01 (QoS granted)  (1/4 registered)
[subscriber] SUBACK mid=2  code=0x01 (QoS granted)  (2/4 registered)
[subscriber] SUBACK mid=3  code=0x01 (QoS granted)  (3/4 registered)
[subscriber] SUBACK mid=4  code=0x01 (QoS granted)  (4/4 registered)
[subscriber] all 4 filters registered (3 unique + bare '#' informational) — each creates an independent bridge registry entry

[contrast] Attempting wildcard subscribe on 'store/#' (Events-Store) ...
[contrast] SUBACK 0xA2 — wildcard on non-events (Events-Store) correctly rejected (as expected)

[publisher] published ONE message to 'events/leak/3f545b/x'  mid=1
[publisher] expecting >= 3 deliveries (one per matching unique bridge registry entry; the bare '#' may add more)
[subscriber] copy #1 received on 'events/leak/3f545b/x': {"event": "overlap-test"}
[subscriber] copy #2 received on 'events/leak/3f545b/x': {"event": "overlap-test"}
[subscriber] copy #3 received on 'events/leak/3f545b/x': {"event": "overlap-test"}
[subscriber] copy #4 received on 'events/leak/3f545b/x': {"event": "overlap-test"}

[result] received 4 copies of 'events/leak/3f545b/x' (>= 3) — one per matching bridge registry entry (no cross-entry dedup)
[result] 1 extra copy(ies) — the bare '#' (or a foreign broad-wildcard subscriber on the global '>' channel) added matching bridge entries; that is gotcha #6, not an error.

Done.
```

## What's Happening

The example mints a **per-run unique id** so its overlapping filters target a
freshly minted KubeMQ channel `leak.<id>.…` that no prior/other run reuses. This
keeps the assertion deterministic on a busy/shared broker (matching the Go/Java
reference examples) instead of using the literal fixed `events/leak/x` /
`events/#`, which over-count when other clients hold broad wildcards.

1. **Connect subscriber** — a single MQTT 5.0 client connects and registers
   **three overlapping** event filters, all scoped to the unique id:
   - `events/leak/<id>/x` — exact topic; maps to KubeMQ channel `leak.<id>.x`.
   - `events/leak/<id>/#` — multi-level wildcard; maps to `leak.<id>.>`
     (`#` → `>`).
   - `events/leak/<id>/+` — single-level wildcard; maps to `leak.<id>.*`
     (`+` → `*`).

   It also subscribes to a bare `#`, but that filter is **informational only**:
   it counts ambient broker traffic, so it varies and is never asserted on.

2. **Bridge registry entries** — each distinct subscribe filter registers an
   independent entry in the connector's bridge registry. The three unique
   filters create three entries, all of which match `events/leak/<id>/x`.

3. **Non-events wildcard contrast** — a separate client attempts
   `subscribe('store/#', qos=1)`. The connector rejects it with
   **SUBACK `0xA2`** (`wildcard-subscriptions-not-supported`) because wildcard
   subscribes are only permitted on the Events pattern.

4. **Publish once** — the publisher sends exactly one message to
   `events/leak/<id>/x` at QoS 1.

5. **At least three copies received** — the connector routes the publish through
   all matching bridge entries. Each entry independently injects the message into
   mochi-mqtt, which fans it out to every locally-matching subscriber. The
   subscriber receives **at least 3 copies** — one per matching unique entry.
   There is **no cross-entry deduplication** in V1 (MQTT 5.0 §3.3.5 permits one
   copy per matching subscription). The bare `#` (or a foreign broad-wildcard
   subscriber) can add extra copies — that **is** gotcha #6, not an error.

6. **Cleanup** — all clients disconnect cleanly.

## MQTT Specifics

| Item | Value |
|------|-------|
| MQTT version | 5.0 (`MQTTv5`) |
| Subscribe QoS | 1 |
| Publish QoS | 1 |
| Topic published | `events/leak/<id>/x` (per-run unique) |
| Wildcard filter 1 | `events/leak/<id>/x` (exact match; KubeMQ channel `leak.<id>.x`) |
| Wildcard filter 2 | `events/leak/<id>/#` (multi-level; KubeMQ channel `leak.<id>.>`) |
| Wildcard filter 3 | `events/leak/<id>/+` (single-level; KubeMQ channel `leak.<id>.*`) |
| Informational filter | bare `#` (counts ambient traffic; never asserted on) |
| MQTT 5.0 properties used | None (no user properties in this example) |
| Callback API | `CallbackAPIVersion.VERSION2` (paho-mqtt 2.x) |

## Wildcard Translation

| MQTT wildcard | KubeMQ wildcard | Notes |
|---------------|-----------------|-------|
| `+` | `*` | Single-level — matches exactly one path segment |
| `#` | `>` | Multi-level — must be the final segment; matches zero or more trailing segments |

## Gotchas

**Gotcha #6 — Overlapping wildcard filters deliver N copies (no dedup).**
Each distinct subscribe filter creates an independent bridge registry entry. A
publish to `events/leak/<id>/x` that matches N entries is injected N times, so a
subscriber receives N copies. There is **no cross-entry deduplication** in V1.
This is intentional (MQTT 5.0 §3.3.5). If you subscribe with two overlapping
filters such as `events/leak/<id>/#` and `events/leak/<id>/x`, any message
published to `events/leak/<id>/x` arrives **twice**. Three overlapping filters
produce (at least) three copies, as this example demonstrates live; a bare `#`
or foreign broad-wildcard subscriber adds more.

**Wildcards are Events-only.**
A wildcard subscribe on any non-Events pattern (`store/#`, `queues/+`,
`commands/#`, `queries/#`) returns **SUBACK `0xA2`**
(`wildcard-subscriptions-not-supported`). Shown live in the contrast step above.

**Literal `.` in a topic segment conflates with `/`.**
`events/a.b/c` and `events/a/b/c` both map to KubeMQ channel `a.b.c`. Avoid
dots in topic segments when you need distinct channels.

**Retain is silently dropped.**
Publishing with `retain=True` returns PUBACK `0x00` (success) but the message
is never delivered and never stored. The connector forces `RetainAvailable=0`.

## Related Examples

- [events/basic_pubsub](../basic_pubsub/) — simple pub/sub without wildcards
- [events_store/start_new_only](../../events_store/start_new_only/) — persistent events (always `StartNewOnly` over MQTT)
- [protocol/mqtt_v311_vs_v5](../../protocol/mqtt_v311_vs_v5/) — MQTT 3.1.1 vs 5.0 comparison
