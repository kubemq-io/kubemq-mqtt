# python — Events-Store: Start-New-Only (no replay)

Demonstrates the `StartNewOnly` constraint for Events-Store pub/sub over the
KubeMQ MQTT connector.  This is the single most important behavioral difference
between KubeMQ Events-Store over MQTT and via the gRPC/REST APIs.

## Prerequisites

- Python 3.10+
- Install dependencies from the shared manifest:
  ```bash
  pip install -r examples/python/requirements.txt
  ```
- KubeMQ server with the MQTT connector enabled (default port `1883`)

## How to Run

```bash
cd examples/python
export KUBEMQ_MQTT_URL=tcp://localhost:1883
python events_store/start_new_only/main.py
```

## Expected Output

```
Connecting to broker at tcp://127.0.0.1:1883
Topic: store/demo/s  ->  KubeMQ channel: demo.s

--- Phase 1: Publishing 3 messages BEFORE subscriber exists ---
[publisher] pre-subscribe publish: {"event": "login", "user": "alice", "seq": 1}
[publisher] pre-subscribe publish: {"event": "login", "user": "bob",   "seq": 2}
[publisher] pre-subscribe publish: {"event": "login", "user": "carol", "seq": 3}
[publisher] 3 pre-subscribe messages stored in Events-Store

--- Phase 2: Subscriber connects (StartNewOnly — NO replay of pre-subscribe msgs) ---
[subscriber] subscribed to store/demo/s
[subscriber] CONSTRAINT: StartNewOnly — pre-subscribe messages are NOT replayed

--- Phase 3: Publishing 1 message AFTER subscriber is live ---
[publisher] post-subscribe publish: {"event": "login", "user": "dave", "seq": 4, "note": "post-subscribe"}

[subscriber] received on store/demo/s: {"event": "login", "user": "dave", "seq": 4, "note": "post-subscribe"}
--- Results ---
[assertion] pre-subscribe messages published : 3
[assertion] pre-subscribe messages received  : 0 (StartNewOnly — not replayed)
[assertion] post-subscribe messages received : 1
[assertion] PASS — only the post-subscribe message was delivered

Done.
```

Captured from a live run against `tcp://127.0.0.1:1883`.

## What's Happening

1. **Phase 1 — Pre-subscribe publishes (not replayed).**
   A publisher connects and sends 3 messages to `store/demo/s`.
   The `store/` prefix selects the KubeMQ **Events-Store** pattern, so the
   messages are durably persisted in the KubeMQ store.  No subscriber exists yet.

2. **Phase 2 — Subscriber connects.**
   The subscriber connects with MQTT 5.0 and calls `SUBSCRIBE store/demo/s`
   at QoS 1.  Because Events-Store over MQTT is **always `StartNewOnly`**, no
   replay of the 3 stored messages occurs.  The subscription is now live.

3. **Phase 3 — Post-subscribe publish (delivered).**
   The publisher sends one more message.  This is the only message the subscriber
   receives, because it was published after the subscription became active.

4. **Assertion.**
   The example asserts exactly 1 message arrived and that it is the
   post-subscribe payload — proving the pre-subscribe messages were never
   delivered.

## MQTT Specifics

| Property             | Value                                          |
|----------------------|------------------------------------------------|
| Topic (publish)      | `store/demo/s`                            |
| Topic (subscribe)    | `store/demo/s`                            |
| KubeMQ channel       | `demo.s` (`/` → `.` in channel name)           |
| QoS                  | 1 (both publish and subscribe)                 |
| MQTT version         | 5.0 (`MQTTv5`)                                 |
| MQTT 5.0 properties  | None used in this example                      |
| Replay mode          | `StartNewOnly` (the only mode available)       |

## Critical Gotcha — No Historical Replay (gotcha #2)

> **Events-Store over MQTT is ALWAYS `StartNewOnly`.**
>
> Unlike the KubeMQ gRPC API (which supports `StartFromFirst`,
> `StartFromSequence`, `StartFromTime`, and `StartFromLast`), the MQTT connector
> does not expose any replay configuration.  A subscriber that connects after
> messages are stored will **never** receive those messages — regardless of QoS,
> clean-session flag, or any other setting.
>
> This is a permanent V1 constraint of the MQTT connector.  If historical replay
> is required, use the KubeMQ gRPC or REST API directly.

## Related Examples

- [`events/basic_pubsub`](../../events/basic_pubsub/) — fire-and-forget events
  (no persistence at all)
- [`queues/shared_consume`](../../queues/shared_consume/) — guaranteed delivery
  with ack-on-PUBACK and redelivery on timeout
