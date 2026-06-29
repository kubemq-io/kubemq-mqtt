# Ruby — Queues: Shared Consume — N/A

> **This example is not available for Ruby.**

## Why N/A

Consuming from a KubeMQ queue over MQTT requires MQTT 5.0 shared subscriptions (`$share/<group>/queues/<channel>` at QoS >= 1). The Ruby `mqtt` gem (njh/ruby-mqtt) implements **MQTT 3.1.1 only** and has no support for MQTT 5.0. Without MQTT 5.0, the required shared-subscription semantics are unavailable.

Attempting a plain `queues/<ch>` subscribe (without `$share`) returns SUBACK `0x83` (implementation-specific error — plain queue subscribe is rejected). A QoS 0 `$share` subscribe also returns SUBACK `0x83`.

## What You Need

To use this pattern, choose a v5-capable MQTT client library:
- **Go**: `github.com/eclipse/paho.golang` — see `examples/go/queues/shared_consume/`
- **Python**: `paho-mqtt` >= 2.1.0 — see `examples/python/queues/shared_consume/`
- **JavaScript**: `mqtt` (MQTT.js) >= 5.10.0 — see `examples/js/queues/shared_consume/`
- **Java**: `org.eclipse.paho.mqttv5.client` — see `examples/java/queues/shared_consume/`
- **C#**: `MQTTnet` >= 4.3.7 — see `examples/csharp/queues/shared_consume/`
- **Rust**: `rumqttc` >= 0.24.0 — see `examples/rust/queues/shared_consume/`

## Queue Produce from Ruby

While consume is N/A, Ruby CAN **produce** queue messages by publishing to `queues/<ch>`:

```ruby
client.publish("queues/demo/q", payload, false, 1)
```

This is a standard MQTT 3.1.1 publish and works with the `mqtt` gem.

## Spec Reference

Spec decision D6: *"queue CONSUME needs v5; `$share/<group>/queues/<ch>` at QoS >= 1 is MQTT 5.0 shared subscription syntax."*

## Related Examples

- [queues/ack_and_redelivery](../ack_and_redelivery/) — also N/A (needs v5)
- [events/basic_pubsub](../../events/basic_pubsub/) — available Ruby pattern
