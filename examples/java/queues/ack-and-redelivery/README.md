# java — Queues: Ack & Redelivery

Demonstrates KubeMQ Queue **ack-on-PUBACK** semantics and **automatic redelivery**
over MQTT 5.0 shared subscriptions. A consumer that receives a message but
does not send PUBACK causes the broker to NAck and requeue it, delivering it to
another waiting consumer with no message loss.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Java | 21+ |
| Maven | 3.8+ |
| Paho MQTT v5 client | `org.eclipse.paho.mqttv5.client` 1.2.5 |
| KubeMQ server | MQTT connector enabled (default port 1883) |

---

## How to Run

```bash
# Default broker (tcp://localhost:1883)
cd examples/java/queues/ack-and-redelivery
mvn compile exec:java

# Override broker URL
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 mvn compile exec:java
```

---

## Expected Output

```
=== queues/ack-and-redelivery ===
Broker:        tcp://127.0.0.1:1883
Produce topic: queues/demo/rq
Consume topic: $share/g1/queues/demo/rq

[Step 1] Connecting Consumer-1 (manual ack — will withhold PUBACK) ...
[Consumer-1] connected to tcp://127.0.0.1:1883 (reconnect=false)
[Consumer-1] subscribed to $share/g1/queues/demo/rq (QoS 1, manual ack)
[Step 2] Connecting Consumer-2 (auto ack) ...
[Consumer-2] connected to tcp://127.0.0.1:1883 (reconnect=false)
[Consumer-2] subscribed to $share/g1/queues/demo/rq (QoS 1, auto ack)
[Step 3] Producing one message to queues/demo/rq ...
[Producer]    PUBACK received — message enqueued.
[Step 4] Waiting for Consumer-1 to receive the message ...
[Consumer-1] messageArrived: topic=queues/demo/rq  payload=requeue-me  id=1  qos=1
[Consumer-1] received: "requeue-me" (id=1) — withholding PUBACK intentionally.
             (Normally, calling messageArrivedComplete(id, qos) here would
              send the PUBACK and ack the message. We skip it on purpose.)
             (Ack-timeout path: if we stayed connected and never called
              messageArrivedComplete(), KubeMQ would NAck and redeliver
              after QueueAckTimeoutSeconds — 30 s by default.)
[Step 5] Consumer-1 disconnecting (unacked message → immediate NAck → requeue) ...
[Consumer-1] disconnected.
[Step 6] Waiting for Consumer-2 to receive the redelivered message ...
[Consumer-2] messageArrived: topic=queues/demo/rq  payload=requeue-me  id=1  qos=1
[Consumer-2] received (redelivered): "requeue-me" — PUBACK sent automatically.
             Message is now acked and removed from the queue.

=== Done: at-least-once delivery demonstrated ===
  1. Message produced to queues/demo/rq
  2. Consumer-1 received it, withheld PUBACK, disconnected
  3. KubeMQ immediately requeued (NAck on disconnect)
  4. Consumer-2 received the redelivered message and acked it
     (no message loss; at-least-once guarantee fulfilled)
```

---

## What's Happening

1. **Consumer-1 connects with manual ack** (`setManualAcks(true)`).
   Paho's manual-ack mode suppresses the automatic PUBACK: the library
   delivers the message to `messageArrived()` but does **not** send PUBACK
   until the application calls `messageArrivedComplete(id, qos)`.

2. **Consumer-2 connects with auto-ack** (default, `setManualAcks(false)`).
   The library sends PUBACK automatically once `messageArrived()` returns.

3. **Both consumers subscribe** to `$share/g1/queues/demo/rq` at QoS 1.
   The `$share/<group>/queues/<channel>` format is required for Queues consume
   over MQTT 5.0 — all other subscribe forms are rejected (SUBACK 0x83).

4. **Producer publishes** to `queues/demo/rq`.
   The PUBACK on the publish side confirms the message is enqueued; it says
   nothing about consumer delivery.

5. **Consumer-1 receives the message** and withholds PUBACK.
   In a real application this is where you would do work. Here we deliberately
   skip `messageArrivedComplete()` to simulate the "did not finish" scenario.

6. **Consumer-1 disconnects** (reason code 0, clean disconnect).
   The KubeMQ MQTT connector's `OnDisconnect` hook fires
   `nackPendingForClient`, which issues an immediate `NAckRange` for every
   unacked inflight message on that client. The message is requeued instantly
   with no loss — this is the **fast-path** redelivery.

7. **Consumer-2 receives the requeued message** and the library automatically
   sends PUBACK. The message is now acked and removed from the queue.

### Ack-timeout path (slower, not run live)

If Consumer-1 had stayed connected but never called `messageArrivedComplete()`,
KubeMQ's ack sweeper would NAck and requeue the message after
`QueueAckTimeoutSeconds` (default **30 s**). This triggers the same redelivery
path as the disconnect case — it is just slower. The disconnect path is
demonstrated here because it avoids a 30-second wait in the example.

---

## MQTT Specifics

| Property | Value |
|---|---|
| Protocol version | MQTT 5.0 (required for shared subscriptions) |
| Produce topic | `queues/demo/rq` |
| Consume topic | `$share/g1/queues/demo/rq` |
| QoS (both produce and consume) | 1 |
| Ack mechanism | PUBACK — sent by `messageArrivedComplete(id, qos)` |
| Manual ack (Consumer-1) | `MqttAsyncClient.setManualAcks(true)` |
| Auto ack (Consumer-2) | `MqttAsyncClient.setManualAcks(false)` (default) |
| Fast-path redelivery trigger | Disconnect with unacked message → immediate NAck |
| Slow-path redelivery trigger | No PUBACK within `QueueAckTimeoutSeconds` (30 s default) |
| MQTT 5.0 properties used | None beyond standard QoS 1 mechanics |

### Reason codes to know

| Code | Packet | Meaning |
|---|---|---|
| 0x01 | SUBACK | Granted QoS 1 — subscription accepted (this example subscribes at QoS 1) |
| 0x83 | SUBACK | QoS 0 shared queue subscribe rejected; or plain `queues/` subscribe rejected |
| 0xA2 | SUBACK | Wildcard subscribe on non-events topic rejected |

### Gotcha: `$share` group is audit/metrics-only

The group name in `$share/<group>/queues/<channel>` is **not** a separate
queue — all groups compete in the **same** KubeMQ queue pool. There are no
per-group copies of messages. Use different channel names if you need
independent queues.

---

## Related Examples

- [`queues/shared-consume`](../shared-consume/) — basic competing consumers (auto ack)
- [`events/basic-pubsub`](../../events/basic-pubsub/) — fire-and-forget events (no ack required)
- [`rpc/query-round-trip`](../../rpc/query-round-trip/) — request/reply pattern (MQTT 5.0 RPC)
