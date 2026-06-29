# JavaScript/TypeScript — Queues: Shared-Subscription Consume

Demonstrates KubeMQ Queues over MQTT: plain-publish to produce and `$share` subscription to consume. Includes negative-path verification for disallowed subscribe forms.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server with MQTT connector enabled on port 1883

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
npx tsx queues/shared-consume/index.ts
```

## Expected Output

```
[probe] connected (negative-path checks)
[probe] testing QoS 0 shared-queue subscribe (expect 0x83)...
[probe] QoS 0 correctly rejected with SUBACK 0x83 (impl-specific error)
[probe] testing plain queues/demo/q subscribe (expect 0x83)...
[probe] plain queues/ correctly rejected with SUBACK 0x83 (impl-specific error)
[probe] disconnected
[consumer] connected
[consumer] subscribed to $share/g1/queues/demo/q qos=1
[producer] connected
[producer] enqueued message to queues/demo/q
[consumer] received from queues/demo/q: {"orderId":"ORD-001","item":"widget","qty":3}
[done]
```

## What's Happening

1. A short-lived **probe** client connects (MQTT 5.0) and exercises two negative paths to confirm the broker enforces the queue-subscription rules. (A separate probe client is used so the rejected filters never touch the real consumer's queue-bridge registration; a real consumer never mixes rejected and valid queue subscriptions.)
2. Negative path — QoS 0 shared subscribe: `$share/g1/queues/demo/q` at QoS 0 is attempted; the broker replies with SUBACK reason code `0x83` (implementation-specific error). QoS 0 queue subscriptions are rejected per M-3.
3. Negative path — plain `queues/` subscribe: `queues/demo/q` at QoS 1 (no `$share` prefix) is attempted; the broker replies with SUBACK `0x83`. Only `$share/<group>/queues/<ch>` is accepted for queue consumption.
4. The probe disconnects. A clean **consumer** client then subscribes correctly: `$share/g1/queues/demo/q` at QoS 1. The SUBACK returns the granted QoS (0 or 1 — values ≤ 2 indicate success).
5. A producer connects and publishes `{"orderId":"ORD-001","item":"widget","qty":3}` to `queues/demo/q` at QoS 1. The `queues/` topic prefix routes the message into the KubeMQ Queues pattern (produce).
6. The broker delivers the message to the consumer via the shared subscription. MQTT.js automatically sends PUBACK for QoS 1, which acks the message in the KubeMQ queue (ack-on-PUBACK model).
7. All clients disconnect cleanly.

## MQTT Specifics

| Property | Value |
|---|---|
| Protocol version | MQTT 5.0 (required for `$share` subscriptions) |
| Produce topic | `queues/demo/q` |
| Consume topic | `$share/g1/queues/demo/q` |
| QoS (produce) | 1 |
| QoS (consume) | 1 (must be ≥ 1; QoS 0 → SUBACK `0x83`) |
| Ack model | ack-on-PUBACK (MQTT.js sends PUBACK automatically for QoS 1) |
| Negative path: QoS 0 shared sub | SUBACK `0x83` (test #8) |
| Negative path: plain `queues/` sub | SUBACK `0x83` (test #9) |
| No-PUBACK timeout | 30 s default (`QueueAckTimeoutSeconds`) → NAck → redeliver |

## Gotchas

**`$share` group name is audit/metrics-only.** The group label in `$share/<group>/queues/<ch>` (here `g1`) is used only for audit and metrics — it does NOT create per-group message copies. All groups compete in **one shared KubeMQ queue pool**. This is the opposite of standard MQTT shared-subscription semantics where each group gets its own copy. Two subscribers using `$share/g1/...` and `$share/g2/...` on the same channel will _compete_ for the same messages, not each receive a separate copy.

**No historical replay.** Queues deliver messages to the _first available_ consumer; there is no replay or resume-from-offset. For persistent replay semantics, see the Events Store pattern.

**Reason code `0x83` (Implementation Specific Error).** Both illegal subscribe forms (QoS 0 `$share` and plain `queues/`) return this code in the SUBACK. It is not a transient error — retrying with the same parameters will always fail.

## Related Examples

- [queues/ack-and-redelivery](../ack-and-redelivery/) — manual ack, ack-timeout, disconnect-requeue
- [events-store/start-new-only](../../events-store/start-new-only/) — persistent pub/sub with StartNewOnly replay
