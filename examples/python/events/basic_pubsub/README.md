# python — Events: Basic Pub/Sub

Demonstrates fire-and-forget event pub/sub using the KubeMQ MQTT connector.

A subscriber connects with MQTT 5.0 and subscribes to the single-level wildcard
`events/demo/+` at QoS 1.  A publisher connects and publishes a single message to the
concrete topic `events/demo/x` (which the `+` wildcard matches) with payload `"hello"`
and one MQTT 5.0 User Property (`k1=v1`).  The subscriber receives the message, prints the
payload and user properties, and the example exits cleanly.

The `+` single-level wildcard is the canonical Events subscribe form: one subscription on
`events/demo/+` receives every event published under `events/demo/<anything>`.  Wildcard
subscriptions are permitted on the Events pattern **only** (`+` → KubeMQ `*`, `#` → `>`).

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` (from `examples/python/`) — installs `paho-mqtt>=2.1.0`
- KubeMQ server with MQTT connector enabled on `tcp://localhost:1883`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
export KUBEMQ_MQTT_URL=tcp://localhost:1883   # default; adjust as needed
python events/basic_pubsub/main.py
```

## Expected Output

```
Broker: tcp://127.0.0.1:1883  (scheme=tcp, host=127.0.0.1, port=1883)
MQTT subscribe : events/demo/+  (single-level '+' wildcard -> KubeMQ demo.*)
MQTT publish   : events/demo/x
KubeMQ ch      : demo.x  (/ -> . translation)
[subscriber] SUBACK mid=1 reason_code=1
[publisher]  published to events/demo/x  payload='hello'  user-prop k1=v1  mid=1
[subscriber] received on events/demo/x
             payload       : 'hello'
             user-props    : [('k1', 'v1')]  (forwarded as KubeMQ Tags)
[ok] payload and user-property (KubeMQ tag) round-trip verified
Done.
```

Captured from a live run against `tcp://127.0.0.1:1883`.

## What's Happening

1. **Connect (subscriber)** — a `MQTTv5` client connects to the broker with `keepalive=30`.
2. **Subscribe** — subscribes to the wildcard `events/demo/+` at QoS 1.  The `events/`
   prefix selects the KubeMQ **Events** pattern (fire-and-forget); the `+` single-level
   wildcard maps to KubeMQ channel `demo.*` and matches any publish under `events/demo/<x>`.
   The connector logs a SUBACK `0x01`.
3. **Wait** — a short `sleep(0.5)` lets the subscription register before publishing.
4. **Connect (publisher)** — a second `MQTTv5` client connects.
5. **Publish** — sends `"hello"` to `events/demo/x` at QoS 1 with User Property `k1=v1`.
   The MQTT `"/"` separator is translated to `"."` in the KubeMQ channel name:
   `events/demo/x` → channel **`demo.x`**.
6. **Receive** — the subscriber's `on_message` callback fires; the payload and user
   properties (forwarded as KubeMQ Tags) are printed and asserted.
7. **Disconnect** — both clients call `loop_stop()` / `disconnect()` for a clean exit.

## MQTT Specifics

| Item | Value |
|------|-------|
| MQTT version | 5.0 (`MQTTv5`) |
| Subscribe topic | `events/demo/+` (single-level `+` wildcard → KubeMQ `demo.*`) |
| Publish topic | `events/demo/x` |
| QoS | 1 (both directions) |
| KubeMQ channel | `demo.x` (`/` → `.`); subscribe filter → `demo.*` |
| User Property | `k1=v1` (forwarded as KubeMQ Tag `Tags["k1"] = "v1"`) |
| Payload | `hello` (raw bytes) |

### User Properties ↔ KubeMQ Tags (MQTT 5.0 only)

| MQTT User Property | KubeMQ Tag |
|--------------------|------------|
| `k1=v1` | `Tags["k1"] = "v1"` |

Cap: 32 properties / 4 096 bytes total.  Exceeding the cap returns PUBACK `0x97`.
MQTT 3.1.1 clients cannot attach User Properties — the tag mapping is v5-only.

## Gotchas

- **Retain is silently dropped**: publishing with `retain=True` succeeds (PUBACK `0x00`)
  but the message is never delivered and is not stored.  The broker forces
  `RetainAvailable=0`; a Will-retain at CONNECT would return CONNACK `0x9A`.
- **Literal `.` in a topic segment conflates with `/`**: `events/a.b/c` and
  `events/a/b/c` both map to KubeMQ channel `a.b.c` — these are the **same** channel.
- **Events-Store is always StartNewOnly over MQTT**: if you switch to the `store/` prefix
  you will never receive messages published before your subscribe call.
- **N-copies from overlapping bridge registry entries**: each MQTT subscription that
  matches a published topic creates an independent KubeMQ bridge registry entry.  If
  other connected clients hold overlapping wildcard subscriptions (e.g. `events/#` or
  `#`), their bridge entries also fan the same event back to all MQTT subscribers on
  matching topics.  A single publish can therefore be received more than once.  This
  example uses `received_event.set()` on the first arrival and exits after the first
  verified message — extra copies after the assert are harmless.

## Related Examples

- [events/wildcard_subscribe](../wildcard_subscribe/) — `+` and `#` wildcard filters
- [events_store/start_new_only](../../events_store/start_new_only/) — persistent events (StartNewOnly)
- [protocol/mqtt_v311_vs_v5](../../protocol/mqtt_v311_vs_v5/) — v3.1.1 vs v5 comparison (v3.1.1 has no User Properties)
