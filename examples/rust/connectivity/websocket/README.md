# rust — Connectivity: WebSocket

Demonstrates MQTT 5.0 over WebSocket (`ws://host:8083/`) connecting to KubeMQ, performing a basic Events pub/sub round-trip using `rumqttc` with the `websocket` feature.

## Prerequisites

- Rust 1.75+ (stable toolchain)
- KubeMQ server with the WebSocket listener active on port 8083 (default path `/`)
- `rumqttc` compiled with `features = ["websocket"]` — already included in the workspace `Cargo.toml`; no extra dependency needed

## How to Run

```bash
cd examples/rust
KUBEMQ_MQTT_URL=ws://127.0.0.1:8083/ cargo run -p connectivity-websocket
```

The env var defaults to `ws://localhost:8083/` when unset. Pass `tcp://…` to fall back to plain TCP (the WebSocket framing is skipped, but the Events flow is otherwise identical).

## Expected Output

```
=== KubeMQ MQTT 5.0 over WebSocket ===
Client ID : mqtt-rust-ws-<8 hex chars>
Transport : WebSocket
URL       : ws://127.0.0.1:8083/
Subscribed to 'events/demo/ws' (QoS 1)
[event-loop] Connected (CONNACK received)
[event-loop] Subscribed (SUBACK pkid=1)
Published  to 'events/demo/ws' (QoS 1, 31 bytes)
[event-loop] Received on 'events/demo/ws': Hello over WebSocket from Rust!
WS_EVENT_RECEIVED
=== Success: message round-tripped over WebSocket ===
```

## What's Happening

1. **Build options** — `KUBEMQ_MQTT_URL` scheme `ws://` triggers `Transport::Ws` on `MqttOptions`. The full URL is passed as the `host` argument (rumqttc requires this for WebSocket; the port argument is ignored when the host contains a URL).
2. **Connect** — `AsyncClient::new` starts an async event loop. The first `ConnAck` confirms the broker accepted the MQTT 5.0 handshake over the HTTP-Upgrade WebSocket connection.
3. **Subscribe** — client subscribes to `events/demo/ws` at QoS 1 **before** publishing. The `events/` prefix routes to the KubeMQ Events pattern; `/` in topics maps to `.` in KubeMQ channel names (`events/demo/ws` → channel `demo.ws`).
4. **Short delay** — 300 ms pause ensures the broker has registered the subscription before the publish arrives.
5. **Publish** — a 31-byte payload is sent to the same topic at QoS 1 with `retain = false`.
6. **Receive** — the broker delivers the message back; the event loop prints the topic and payload, then exits.
7. **Clean exit** — the main task times out waiting after 10 s if no message arrives (exits non-zero).

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 |
| Transport | WebSocket — `ws://host:8083/` |
| Subscribe topic | `events/demo/ws` |
| Publish topic | `events/demo/ws` |
| QoS | 1 (AtLeastOnce) |
| Retain | false (must never set `true` — KubeMQ silently drops retained publishes with PUBACK 0x00) |
| KubeMQ pattern | Events |
| KubeMQ channel | `demo.ws` (topic `/` → channel `.`) |
| MQTT 5.0 properties | None used (ConnectProperties / PublishProperties are default) |

## Gotchas

**retain=false is mandatory.** KubeMQ MQTT sets `RetainAvailable=0` in CONNACK. If you publish with `retain=true` at runtime, the broker issues a successful PUBACK (0x00) but silently drops the message — it is never delivered. The error is audited server-side. This example always publishes with `retain=false`.

**rumqttc WebSocket URL format.** When `Transport::Ws` is used, the `host` argument in `MqttOptions::new` must be the full `ws://…` URL (scheme + host + port + path). The numeric port argument to `new` is ignored. Passing only the hostname triggers an "Invalid Url: Couldn't parse host from url" error in the event loop.

**Events-Store is always StartNewOnly over MQTT.** Subscribing to `store/…` topics provides no historical replay — messages are delivered from the point of subscription only.

## WebSocket vs TCP

WebSocket transport is useful when MQTT/TCP port 1883 is blocked by a firewall but HTTP/WebSocket port 8083 is allowed. The MQTT framing is identical; only the transport layer changes.

## wss:// (WebSocket over TLS)

For `wss://` extend with `Transport::Wss(TlsConfiguration::Rustls(…))` and point to the TLS listener (port 8883). See the [connectivity/tls](../tls/) example for the TLS configuration pattern.

## Related Examples

- [events/basic-pubsub](../../events/basic-pubsub/) — same Events flow over plain TCP
- [connectivity/tls](../tls/) — TLS transport
- [connectivity/auth-jwt](../auth-jwt/) — JWT authentication
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — comparing MQTT 3.1.1 and 5.0 features
