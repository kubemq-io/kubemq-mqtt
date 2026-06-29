# Ruby — RPC: Query Round-Trip — N/A

> **This example is not available for Ruby.**

## Why N/A

RPC (Commands and Queries) over MQTT is **MQTT 5.0 only**. The protocol requires:
- `Properties.ResponseTopic` (MQTT 5.0 publish property) pointing to the client's own `$reply/<clientID>/<suffix>` topic.
- `Properties.CorrelationData` (MQTT 5.0 publish property) to match requests to responses.

These properties do not exist in MQTT 3.1.1. A v3.1.1 publish to `queries/<ch>` returns PUBACK `0x00` (wire success) but the message is **silently dropped** by the connector and a `publish.error` is audited server-side. The RPC request is never executed (**gotcha #4**).

The Ruby `mqtt` gem implements **MQTT 3.1.1 only**, so RPC patterns are unavailable.

## What You Need

To use this pattern, choose a v5-capable MQTT client library:
- **Go**: `github.com/eclipse/paho.golang` — see `examples/go/rpc/query_round_trip/`
- **Python**: `paho-mqtt` >= 2.1.0 — see `examples/python/rpc/query_round_trip/`
- **JavaScript**: `mqtt` (MQTT.js) >= 5.10.0 — see `examples/js/rpc/query_round_trip/`
- **Java**: `org.eclipse.paho.mqttv5.client` — see `examples/java/rpc/query_round_trip/`
- **C#**: `MQTTnet` >= 4.3.7 — see `examples/csharp/rpc/query_round_trip/`
- **Rust**: `rumqttc` >= 0.24.0 — see `examples/rust/rpc/query_round_trip/`

Note: RPC also requires a gRPC-side responder (MQTT clients cannot subscribe as RPC responders — SUBACK `0x83`). See `examples/go/rpc/` for the embedded gRPC responder pattern.

## Spec Reference

Spec §2.2 Commands/Queries (RPC): *"MQTT 5.0 ONLY — gotcha #4. A v3.1.1 publish to `commands/`/`queries/` is silently dropped + `publish.error` audited."*

## Related Examples

- [rpc/command_round_trip](../command_round_trip/) — also N/A (needs v5)
- [protocol/mqtt_v311_vs_v5](../../protocol/mqtt_v311_vs_v5/) — explains v3.1.1 limitations
