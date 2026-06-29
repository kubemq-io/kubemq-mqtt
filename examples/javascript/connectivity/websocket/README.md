# JavaScript/TypeScript — Connectivity: WebSocket

Demonstrates Events pub/sub over WebSocket (`ws://host:8083/`) using the KubeMQ MQTT connector with MQTT 5.0 (per decision D7).

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server with WebSocket listener enabled on port 8083

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_MQTT_URL=ws://127.0.0.1:8083/
npx tsx connectivity/websocket/index.ts
```

## Expected Output

```
[example] connecting over WebSocket to ws://localhost:8083/
[subscriber] connected over WebSocket
[subscriber] subscribed to events/demo/ws
[publisher] connected over WebSocket
[publisher] published to events/demo/ws over WebSocket
[subscriber] topic: events/demo/ws | payload: {"transport":"websocket","message":"Hello over WebSocket!"}
[done]
```

## What's Happening

- MQTT.js detects the `ws://` scheme and uses a WebSocket transport automatically.
- The WebSocket path MUST be `/` (`ws://host:8083/`); omitting the trailing slash causes a 404.
- Both MQTT 3.1.1 and 5.0 are supported over WebSocket; this example uses 5.0 (per D7).

## MQTT Specifics

| Property | Value |
|----------|-------|
| Protocol version | MQTT 5.0 |
| URL scheme | `ws://` (path must be `/`) |
| Default URL | `ws://localhost:8083/` |
| Topic | `events/demo/ws` |
| QoS | 1 |

## WebSocket vs TCP

| Aspect | TCP | WebSocket |
|--------|-----|-----------|
| Port | 1883 | 8083 |
| URL scheme | `tcp://` | `ws://` |
| Path | n/a | `/` (required) |
| Browser-compatible | No | Yes |

## Related Examples

- [connectivity/tls](../tls/) — TLS transport (`tls://host:8883`)
- [connectivity/auth-jwt](../auth-jwt/) — authentication over any transport
- [events/basic-pubsub](../../events/basic-pubsub/) — same pub/sub pattern over plain TCP
