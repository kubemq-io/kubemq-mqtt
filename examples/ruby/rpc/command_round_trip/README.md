# Ruby — RPC: Command Round-Trip — N/A

> **This example is not available for Ruby.**

## Why N/A

RPC (Commands) over MQTT is **MQTT 5.0 only**. The protocol requires MQTT 5.0 publish properties (`ResponseTopic`, `CorrelationData`) which do not exist in MQTT 3.1.1. A v3.1.1 publish to `commands/<ch>` returns PUBACK `0x00` (wire success) but the message is **silently dropped** and `publish.error` is audited (**gotcha #4**).

The Ruby `mqtt` gem implements **MQTT 3.1.1 only**, so RPC patterns are unavailable.

Additionally, MQTT clients cannot subscribe as RPC responders — a `commands/` subscribe returns SUBACK `0x83`. Responders must be implemented via KubeMQ's gRPC API.

## What You Need

To use this pattern, choose a v5-capable MQTT client library:
- **Go**: `github.com/eclipse/paho.golang` — see `examples/go/rpc/command_round_trip/`
- **Python**: `paho-mqtt` >= 2.1.0 — see `examples/python/rpc/command_round_trip/`
- **JavaScript**: `mqtt` (MQTT.js) >= 5.10.0 — see `examples/js/rpc/command_round_trip/`
- **Java**: `org.eclipse.paho.mqttv5.client` — see `examples/java/rpc/command_round_trip/`
- **C#**: `MQTTnet` >= 4.3.7 — see `examples/csharp/rpc/command_round_trip/`
- **Rust**: `rumqttc` >= 0.24.0 — see `examples/rust/rpc/command_round_trip/`

## Command Response Format (for reference)

When using a v5-capable client, the command response contains:
- **No body** (empty payload)
- User properties: `kubemq-executed` = `"true"` or `"false"`; optional `kubemq-error` on failure
- `CorrelationData` echoed from the request

## Spec Reference

Spec §2.2 Commands/Queries (RPC): *"MQTT 5.0 ONLY — gotcha #4."*

## Related Examples

- [rpc/query_round_trip](../query_round_trip/) — also N/A (needs v5)
- [protocol/mqtt_v311_vs_v5](../../protocol/mqtt_v311_vs_v5/) — explains v3.1.1 limitations
