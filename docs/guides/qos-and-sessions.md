# QoS and Sessions

## Overview

The KubeMQ MQTT connector supports QoS 0, 1, and 2. The effective QoS level interacts with KubeMQ pattern semantics in non-obvious ways — notably, Queues consumption requires QoS ≥ 1 and uses PUBACK as the acknowledgement signal. Sessions are in-memory and node-local; a `clean_session=false` reconnect on the **same** broker node restores subscriptions.

---

## QoS Levels

| QoS | Wire semantics | KubeMQ pattern constraints |
|-----|---------------|---------------------------|
| 0 — at-most-once | Fire-and-forget; no PUBACK | Events / Events-Store publish and subscribe: allowed |
| 1 — at-least-once | Publish: PUBACK required; subscribe: PUBACK on each delivery | Required for Queues consume (`$share`); recommended for RPC |
| 2 — exactly-once | Full PUBREC/PUBREL/PUBCOMP handshake | Supported at the MQTT layer; KubeMQ treats it identically to QoS 1 for routing purposes |

**Server capability:** `MaxQos = 2` (configurable via `CONNECTORSMQTT_CAPABILITIES_MAX_QOS`).

---

## QoS Per Pattern

### Events and Events-Store

Any QoS is permitted. QoS 0 is common for high-throughput telemetry where occasional loss is acceptable; QoS 1 is recommended for reliable delivery.

Events-Store always stores messages durably on the KubeMQ side regardless of MQTT QoS. However:
- Over MQTT, events-store subscriptions are **always `StartNewOnly`** — historical replay is not available regardless of QoS or session state (see gotcha #2 in [patterns/events-store.md](../patterns/events-store.md)).

### Queues

| Operation | Required QoS | Notes |
|-----------|-------------|-------|
| Produce (publish to `queues/<ch>`) | Any | QoS 0 produce is accepted; no acknowledgement guarantee at the MQTT layer |
| Consume (`$share/<g>/queues/<ch>`) | **≥ 1** | QoS 0 shared-queue subscribe → SUBACK `0x83`; the connector enforces this |

**Ack-on-PUBACK model:** a Queues message is acknowledged to KubeMQ when the MQTT broker receives the PUBACK from the consuming client. Until PUBACK is received the message remains in-flight. On disconnect with pending messages, or on ack-timeout, the message is NAcked and requeued.

### Commands and Queries (RPC)

RPC is **MQTT 5.0 only**. QoS 1 is required on both the `$reply/<clientID>/...` subscribe and the `commands/` or `queries/` publish. See [patterns/commands.md](../patterns/commands.md) and [patterns/queries.md](../patterns/queries.md).

---

## Queue Ack Timeout and Redelivery

The connector tracks an in-flight Queues message per MQTT client. If no PUBACK is received within `QueueAckTimeoutSeconds` (default 30 s, configurable via `CONNECTORSMQTT_QUEUE_ACK_TIMEOUT_SECONDS`), the connector issues a NAck to KubeMQ and the message is redelivered to the next available consumer.

| Event | KubeMQ action |
|-------|--------------|
| PUBACK received within timeout | Ack — message removed from queue |
| No PUBACK within `QueueAckTimeoutSeconds` | NAck → redeliver to another consumer |
| Client disconnects with pending delivery | Immediate NAck → requeue (no message loss) |

The disconnect-with-pending behaviour means **clean shutdown without PUBACK causes immediate requeue**, which is the correct durability guarantee.

---

## Graceful Shutdown

When the KubeMQ server shuts down, the connector sends DISCONNECT reason code `0x8B` (server-shutting-down) to all connected clients. Any queue messages that are in-flight at that moment are immediately NAcked and requeued. Integration test #21 verifies this flow.

---

## Sessions

### In-Memory, Node-Local

Sessions are stored in memory only, on the node that holds the connection. There is no cross-node session replication.

| Property | Value |
|----------|-------|
| Storage | In-memory (mochi-mqtt) |
| Scope | Node-local |
| Max session expiry | 3600 s (configurable via `CONNECTORSMQTT_CAPABILITIES_MAX_SESSION_EXPIRY_SECONDS`) |
| Max message expiry | 86400 s (configurable via `CONNECTORSMQTT_CAPABILITIES_MAX_MESSAGE_EXPIRY_SECONDS`) |

### `clean_session = false` (MQTT 3.1.1) / `CleanStart = false` (MQTT 5.0)

When a client reconnects with `clean_session=false` to the **same node**, the broker restores:
- Active subscriptions from the previous session
- Any pending (undelivered) messages for those subscriptions

> **Important:** reconnecting to a **different node** in a multi-node cluster does not restore the session — sessions are node-local. Clients that require durable sessions in a clustered environment should pin to the same node or use the Events-Store pattern for replay.

### `clean_session = true` / `CleanStart = true`

The broker discards the previous session on connect. All subscriptions and pending deliveries are cleared. This is the recommended setting for stateless consumers and for the examples in this repository.

### Session Takeover

If a client reconnects with the same `ClientID` while the previous connection is still active, the broker performs a session takeover: the old connection is closed and the new connection inherits the session state (when `clean_session=false`).

---

## ReceiveMaximum and Inflight Limits

| Setting | Default | Env var |
|---------|---------|---------|
| `ReceiveMaximum` | 1024 | `CONNECTORSMQTT_CAPABILITIES_RECEIVE_MAXIMUM` |
| `MaxInflight` | 8192 | `CONNECTORSMQTT_CAPABILITIES_MAX_INFLIGHT` |

These caps apply per broker, not per client. When `ReceiveMaximum` is reached on a connection the broker stops delivering new messages until outstanding PUBACKs are received.

---

## QoS Downgrade

The broker advertises `MaxQos = 2`. If a client subscribes at a higher QoS than the publisher used, delivery QoS is `min(subscribe QoS, publish QoS)` per MQTT semantics. The KubeMQ routing layer is not affected by this; the QoS downgrade is a pure MQTT-layer operation.

---

## See Also

- [patterns/queues.md](../patterns/queues.md) — ack-on-PUBACK, redelivery, shared-consume
- [patterns/events-store.md](../patterns/events-store.md) — StartNewOnly, no historical replay over MQTT
- [reference/reason-codes.md](../reference/reason-codes.md) — `0x83` (QoS 0 queue), `0x8B` (shutdown)
- [reference/capabilities.md](../reference/capabilities.md) — forced broker capability values
- [examples/go/queues/ack-and-redelivery/](../../examples/go/queues/ack-and-redelivery/)
