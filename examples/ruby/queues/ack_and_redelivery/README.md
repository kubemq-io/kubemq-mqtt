# Ruby — Queues: Ack and Redelivery — N/A

> **This example is not available for Ruby.**

## Why N/A

Queue consume (and therefore ack/redelivery) over MQTT requires MQTT 5.0 shared subscriptions. The queue-consume ack model is: ack-on-PUBACK at QoS 1. If no PUBACK is received within `QueueAckTimeoutSeconds` (default 30s), the broker NAcks and redelivers. Disconnecting with unacknowledged messages triggers an immediate NAck and requeue (no message loss).

All of this is gated on MQTT 5.0 `$share/<group>/queues/<ch>` at QoS >= 1. The Ruby `mqtt` gem implements **MQTT 3.1.1 only**, so queue consume (and hence ack/redelivery) is unavailable.

## What You Need

To use this pattern, choose a v5-capable MQTT client library:
- **Go**: `github.com/eclipse/paho.golang` — see `examples/go/queues/ack_and_redelivery/`
- **Python**: `paho-mqtt` >= 2.1.0 — see `examples/python/queues/ack_and_redelivery/`
- **JavaScript**: `mqtt` (MQTT.js) >= 5.10.0 — see `examples/js/queues/ack_and_redelivery/`
- **Java**: `org.eclipse.paho.mqttv5.client` — see `examples/java/queues/ack_and_redelivery/`
- **C#**: `MQTTnet` >= 4.3.7 — see `examples/csharp/queues/ack_and_redelivery/`
- **Rust**: `rumqttc` >= 0.24.0 — see `examples/rust/queues/ack_and_redelivery/`

## Spec Reference

Spec decision D6 and §2.2 Queues: *"CONSUME: MQTT 5.0 shared subscription `$share/<group>/queues/<ch>` at QoS >= 1."*

## Related Examples

- [queues/shared_consume](../shared_consume/) — also N/A (needs v5)
- [events/basic_pubsub](../../events/basic_pubsub/) — available Ruby pattern
