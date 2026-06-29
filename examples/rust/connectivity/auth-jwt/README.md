# rust — Connectivity: Auth (password-as-JWT)

Demonstrates MQTT CONNECT with a KubeMQ JWT token in the `Password` field using `rumqttc` (MQTT 5.0).

## Prerequisites

- Rust 1.75+ (stable)
- KubeMQ server with MQTT connector enabled (default: `tcp://localhost:1883`)
- A valid KubeMQ JWT token (for full enforcement, an auth-enabled broker is required)

> **Live-test note**: Auth is disabled on the default local test broker. The example connects and completes the pub/sub round-trip successfully, but JWT rejection (CONNACK `0x86`) requires a broker with `Security.Mode` configured. Mark the rejection path as partial/NA for local testing.

## How to Run

```bash
cd examples/rust

# Auth-disabled broker (connects with any token value):
KUBEMQ_MQTT_URL=tcp://localhost:1883 cargo run -p connectivity-auth-jwt

# Auth-enabled broker with a real JWT:
KUBEMQ_MQTT_URL=tcp://auth-broker:1883 \
  KUBEMQ_MQTT_JWT=<your-kubemq-jwt-token> \
  cargo run -p connectivity-auth-jwt
```

## Expected Output

### Auth-disabled broker

```
Connecting with JWT in Password field (Username is display-only)
CONNACK: Success
Published to events/demo/auth (QoS 1)
Received on 'events/demo/auth': Hello from auth-jwt example!
AUTH_EVENT_RECEIVED
```

### Auth-enabled broker, invalid JWT

```
Connecting with JWT in Password field (Username is display-only)
Connection error: ... (CONNACK 0x86 Bad Credentials)
(Expected on auth-enabled broker with invalid JWT — CONNACK 0x86)
```

## What's Happening

1. The JWT token is read from `KUBEMQ_MQTT_JWT` (defaults to `"placeholder-jwt-token"` if unset).
2. `MqttOptions::set_credentials("kubemq-client", &jwt)` sets `Username` and `Password` on the MQTT CONNECT packet. KubeMQ uses only the `Password` field for auth; `Username` is display-only.
3. The `AsyncClient` event loop is started and the `ConnAck` packet is logged — `0x00` means the broker accepted the connection (or auth is disabled).
4. The client subscribes to `events/demo/auth` at QoS 1, mapping to KubeMQ channel `demo.auth` via the Events pattern.
5. The client publishes `"Hello from auth-jwt example!"` to the same topic.
6. The event loop receives the echoed publish, prints `AUTH_EVENT_RECEIVED`, and exits cleanly.
7. On an auth-enabled broker with an empty or invalid password, the broker returns CONNACK `0x86` and the event loop reports an error before any subscribe or publish can occur.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 |
| `Password` field | KubeMQ JWT token (`PasswordFlag` set implicitly by `set_credentials`) |
| `Username` field | Display-only (`"kubemq-client"` — not used for auth) |
| Subscribe topic | `events/demo/auth` |
| Publish topic | `events/demo/auth` |
| QoS | 1 (AtLeastOnce) |
| KubeMQ pattern | Events (`events/<ch>` prefix) |
| KubeMQ channel | `demo.auth` (topic `/` becomes `.`) |

## Auth Behaviour

| Scenario | Result |
|---|---|
| Auth disabled | Any `Password` value is accepted — CONNACK `0x00` |
| Auth enabled, valid JWT | CONNACK `0x00` Success |
| Auth enabled, empty `Password` | CONNACK `0x86` Bad Credentials |
| Auth enabled, invalid/expired JWT | CONNACK `0x86` Bad Credentials |
| ACL deny on publish | PUBACK `0x87` Not Authorized |

Auth is checked **at connect time only** — there is no mid-connection recheck.

## Security Warning

The JWT is transmitted in the MQTT `Password` field as cleartext at the MQTT layer. **Always use TLS (`tls://host:8883`) in production** to protect the token in transit. See the [connectivity/tls](../tls/) example.

## Related Examples

- [connectivity/tls](../tls/) — TLS transport (required to protect JWT in production)
- [connectivity/websocket](../websocket/) — WebSocket transport
- [events/basic-pubsub](../../events/basic-pubsub/) — same pub/sub flow without auth
