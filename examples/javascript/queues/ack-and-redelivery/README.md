# JavaScript/TypeScript — Queues: Ack & Redelivery

Demonstrates the KubeMQ Queues ack model over MQTT: ack-on-PUBACK, disconnect-without-ack requeue, and (by extension) ack-timeout redelivery.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server with MQTT connector enabled on port 1883

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
npx tsx queues/ack-and-redelivery/index.ts
```

## Expected Output

```
[producer] connected
[producer] enqueued message to queues/demo/rq
[consumerA] connected
[consumerA] subscribed, awaiting delivery before disconnecting...
[consumerA] received (will NOT ack): {"job":"requeue-demo","attempt":1}
[consumerA] socket destroyed (no PUBACK sent) -> broker requeues
[consumerB] connected
[consumerB] subscribed, waiting for redelivered message...
[consumerB] received (redelivered): {"job":"requeue-demo","attempt":1}
[consumerB] PUBACK sent automatically (ack-on-PUBACK)
[done] message redelivered and acked successfully
```

## What's Happening

1. The producer publishes one message to `queues/demo/rq` at QoS 1 and disconnects cleanly.
2. Consumer A subscribes to `$share/g1/queues/demo/rq` at QoS 1 and **waits to actually receive the delivery** (`[consumerA] received (will NOT ack)`). The instant the message arrives, the raw socket is destroyed synchronously — before MQTT.js can flush the PUBACK. The broker now has a tracked in-flight (unacked) delivery, so on the TCP disconnect it immediately NAcks and requeues it (no message loss). Receiving first is critical: destroying the socket before any delivery would leave nothing in-flight to requeue, and the message would instead wait the full 30 s ack-timeout.
3. Consumer B connects, subscribes to the same `$share/g1/queues/demo/rq` topic at QoS 1, and receives the redelivered message. MQTT.js sends PUBACK automatically for QoS 1, which acks the message and removes it from the queue.
4. The second redelivery trigger — no PUBACK within `QueueAckTimeoutSeconds` (default 30 s) — is equivalent: the broker NAcks and redelivers after the timeout expires even if the socket stays open.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 (required; QoS 1 shared subscription is v5-only on this connector) |
| Produce topic | `queues/demo/rq` |
| Consume topic | `$share/g1/queues/demo/rq` |
| QoS | 1 (consume requires QoS >= 1; QoS 0 shared-queue subscribe returns SUBACK 0x83) |
| Ack model | ack-on-PUBACK — MQTT.js sends PUBACK automatically for every QoS 1 message |
| Redelivery trigger 1 | disconnect before PUBACK — broker immediately requeues |
| Redelivery trigger 2 | no PUBACK within `QueueAckTimeoutSeconds` (default 30 s) — broker NAcks and redelivers |
| MQTT 5.0 properties used | none beyond protocolVersion:5 |

## Gotchas

- **`$share` group is one competing pool (gotcha #3):** the group name (`g1`) is audit/metrics-only. ALL groups compete in ONE shared KubeMQ queue pool — this is NOT MQTT-style per-group-copy semantics. Two consumers in different groups do NOT each receive a copy; they compete for the same messages.
- **Plain `queues/<ch>` subscribe is rejected:** subscribing to `queues/demo/rq` directly (without `$share`) returns SUBACK 0x83. You must use the `$share/<group>/queues/<ch>` form.
- **Ack-timeout redelivery:** to observe timeout-based redelivery, set `CONNECTORSMQTT_QUEUE_ACK_TIMEOUT_SECONDS=5` on the server and hold the connection open without processing (or commenting out the message handler resolve) for 5 s. The broker NAcks and redelivers without requiring a disconnect.

## Related Examples

- [queues/shared-consume](../shared-consume/) — basic produce + consume with negative-path verification (QoS 0 and plain subscribe rejection)
