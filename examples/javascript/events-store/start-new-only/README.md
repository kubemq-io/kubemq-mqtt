# JavaScript/TypeScript — Events-Store: Start New Only

Demonstrates that Events-Store over MQTT is **always StartNewOnly** — there is no historical replay (gotcha #2). Messages published before a subscriber connects are NOT delivered.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server with MQTT connector enabled on port 1883

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
npx tsx events-store/start-new-only/index.ts
```

## Expected Output

```
[publisher] connected
[publisher] publishing 3 pre-subscription messages...
[publisher] sent pre-sub message 1
[publisher] sent pre-sub message 2
[publisher] sent pre-sub message 3
[subscriber] connected
[subscriber] subscribed to store/demo/s
[publisher] sent post-sub message (seq=4)
[subscriber] received: {"seq":4,"phase":"post-sub"}
[result] received 1 message(s) total
[result] pre-sub messages received: 0 (StartNewOnly — no replay)
[result] post-sub messages received: 1
[PASS] Events-Store correctly delivers only post-subscribe messages
```

## What's Happening

- The `store/<ch>` topic prefix routes to the KubeMQ Events-Store pattern (persistent pub/sub).
- Unlike gRPC/REST subscribers, an MQTT subscriber on `store/<ch>` always gets `StartNewOnly` — the connector does not support `StartFromFirst`, `StartFromLast`, `StartAtSequence`, or `StartAtTime` over MQTT.
- Messages published before the subscription are stored in KubeMQ but are NOT delivered to the MQTT subscriber.

## MQTT Specifics

| Property | Value |
|----------|-------|
| Protocol version | MQTT 5.0 |
| Topic | `store/demo/s` |
| QoS | 1 |
| Pre-sub messages published | 3 (not delivered) |
| Post-sub message published | 1 (delivered) |

## Gotchas Referenced

- Gotcha #2: Events-Store over MQTT is always StartNewOnly — no historical replay. Pre-subscribe messages are not delivered.

## Related Examples

- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget events (no persistence)
- [queues/shared-consume](../../queues/shared-consume/) — persistent queue with replay-equivalent (consume any pending)
