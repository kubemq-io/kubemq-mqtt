# JavaScript/TypeScript — Connectivity: TLS

Demonstrates Events pub/sub over TLS (`tls://host:8883`) using the KubeMQ MQTT connector.

> **Live-test status**: TLS listener is CLOSED on the reference broker (requires Security config). The example compiles and runs; the connect step will return ECONNREFUSED if the TLS port is not active. Mark live test as N/A on a plain-TCP broker.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server with Security config enabled and TLS listener active on port 8883

## How to Run

```bash
cd examples/javascript
npm install
# With TLS listener active:
export KUBEMQ_MQTT_URL=tls://127.0.0.1:8883
npx tsx connectivity/tls/index.ts

# Or with a plain-TCP broker (for code review without a TLS listener):
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
npx tsx connectivity/tls/index.ts
```

## Expected Output (TLS listener active)

```
[example] connecting to tls://localhost:8883
[subscriber] connected over TLS to tls://localhost:8883
[subscriber] subscribed to events/demo/tls
[publisher] connected over TLS
[publisher] published to events/demo/tls over TLS
[subscriber] topic: events/demo/tls | payload: {"secure":true,"message":"Hello over TLS!"}
[done]
```

## Expected Output (TLS listener closed — live test N/A)

Captured against the reference broker (TLS port 8883 closed). The connect fails with
an `ECONNREFUSED` error (`err.code === 'ECONNREFUSED'`); on Node 26 / mqtt.js 5 the
error `message` string is empty, so the printed line ends after the colon:

```
[example] connecting to tls://localhost:8883
[example] NOTE: TLS listener requires Security config on the broker.
[example] If the TLS port is closed, the connect will fail (expected on a plain-tcp broker).
[subscriber] connection error: 
[subscriber] If the TLS port is closed, this is expected — mark live test N/A.
```

The process exits with code 1. This is the expected N/A outcome on a plain-TCP broker —
the example code is correct; only the broker's TLS listener is unavailable.

## What's Happening

- MQTT.js detects the `tls://` scheme and uses a TLS socket automatically.
- The connector enforces TLS 1.2 minimum; mTLS is supported.
- The example sets `rejectUnauthorized: false` for dev/test convenience. In production, provide a CA bundle.

## MQTT Specifics

| Property | Value |
|----------|-------|
| Protocol version | MQTT 5.0 |
| URL scheme | `tls://` |
| Default URL | `tls://localhost:8883` |
| Topic | `events/demo/tls` |
| QoS | 1 |
| TLS min version | 1.2 (enforced by connector) |
| mTLS | supported (provide `ca`, `cert`, `key` options) |

## Related Examples

- [connectivity/websocket](../websocket/) — WebSocket transport
- [connectivity/auth-jwt](../auth-jwt/) — password-as-JWT authentication
- [events/basic-pubsub](../../events/basic-pubsub/) — same pub/sub pattern over plain TCP
