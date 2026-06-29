# Ruby — Connectivity: WebSocket — N/A

> **This example is not available for Ruby.**

## Why N/A

The Ruby `mqtt` gem (njh/ruby-mqtt) supports only **TCP** and **TLS over TCP** transport. It has **no WebSocket transport** implementation. The KubeMQ MQTT WebSocket listener (`ws://host:8083/`) requires a client library that can upgrade an HTTP connection to WebSocket and then layer MQTT on top — the `mqtt` gem cannot do this.

## What You Need

To connect over WebSocket, choose a library with WebSocket support:
- **Go**: `github.com/eclipse/paho.golang` with a WebSocket dialer — see `examples/go/connectivity/websocket/`
- **Python**: `paho-mqtt` >= 2.1.0 with `transport="websockets"` — see `examples/python/connectivity/websocket/`
- **JavaScript**: `mqtt` (MQTT.js) >= 5.10.0 (native WebSocket) — see `examples/js/connectivity/websocket/`
- **Java**: Eclipse Paho v5 client with WebSocket URI — see `examples/java/connectivity/websocket/`
- **C#**: `MQTTnet` WebSocket channel — see `examples/csharp/connectivity/websocket/`
- **Rust**: `rumqttc` with WebSocket feature — see `examples/rust/connectivity/websocket/`

## WebSocket Endpoint

The KubeMQ MQTT WebSocket listener listens on port `8083`, path `/`:

```
ws://host:8083/
```

Both MQTT 3.1.1 and 5.0 are supported over WebSocket (per D7, examples default to MQTT v5).

## Spec Reference

Spec decision D6: *"WebSocket transport is unavailable in the Ruby `mqtt` gem (TCP + TLS only)."*

## Related Examples

- [connectivity/tls](../tls/) — TLS over TCP (available for Ruby)
- [connectivity/auth_jwt](../auth_jwt/) — JWT authentication (available for Ruby)
