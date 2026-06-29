# rust — Connectivity: TLS

Demonstrates MQTT over TLS (port 8883) connecting to KubeMQ using `rumqttc` with `rustls`.

## Prerequisites

- Rust 1.75+ (stable)
- KubeMQ server with TLS listener configured and active on port 8883
- TLS requires Security configuration in KubeMQ (`tls://host:8883`, min TLS 1.2)

> **Live-test note**: The TLS listener is closed on the default local test broker. This example compiles and runs when TLS is configured. Mark live-test as N/A when the TLS listener is unavailable.

## How to Run

```bash
cd examples/rust
KUBEMQ_MQTT_URL=tls://localhost:8883 cargo run -p connectivity-tls
```

## Expected Output

### TLS listener active

```
Connecting over TLS to tls://localhost:8883
Published to events/demo/tls (QoS 1)
Received on 'events/demo/tls': Hello over TLS from Rust!
TLS_EVENT_RECEIVED
```

### TLS listener closed (current local test broker — port 8883 not listening)

The example code is correct, but the local broker exposes no TLS listener on
8883, so live-testing is N/A. The connection is refused and the example exits
non-zero after surfacing the error:

```
Connecting over TLS to tls://127.0.0.1:8883
Connection/event loop error: I/O: Connection refused (os error 61)
(TLS listener may not be active on this broker — expected for local test setup)
Error: Request(Publish(Publish { ... topic: b"events/demo/tls" ... }))
```

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 |
| Transport | TLS (`tls://host:8883`) |
| Subscribe topic | `events/demo/tls` |
| Publish topic | `events/demo/tls` |
| QoS | 1 (AtLeastOnce) |
| TLS minimum | 1.2 |
| mTLS | Supported (add client cert to `ClientConfig`) |
| KubeMQ pattern | Events |

## What's Happening

1. `KUBEMQ_MQTT_URL` scheme `tls://` triggers `Transport::Tls(TlsConfiguration::Rustls(...))`.
2. `rustls-native-certs` loads system trust roots — no manual CA bundle needed for most setups.
3. For mTLS (client certificates), extend `ClientConfig` with `.with_client_auth_cert(...)`.
4. Pub/sub flow is identical to the plain-TCP `events/basic-pubsub` example.

## Security Note

JWT authentication in `Password` is sent in cleartext at the MQTT layer. **Always use TLS in production** when auth is enabled to protect credentials in transit.

## Related Examples

- [events/basic-pubsub](../../events/basic-pubsub/) — same flow over plain TCP
- [connectivity/websocket](../websocket/) — WebSocket transport
- [connectivity/auth-jwt](../auth-jwt/) — JWT authentication (must use TLS in production)
