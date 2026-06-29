# JavaScript/TypeScript — Connectivity: Auth (password-as-JWT)

Demonstrates password-as-JWT authentication for the KubeMQ MQTT connector.

> **Live-test status**: Auth is DISABLED on the reference broker. The example runs and demonstrates the auth flow; full JWT enforcement requires an auth-enabled broker. Mark live test as partial/N/A on an open broker.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server with MQTT connector enabled on port 1883
- (Optional) A valid KubeMQ JWT token for full auth testing

## How to Run

```bash
cd examples/javascript
npm install
# Without auth (open broker):
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
npx tsx connectivity/auth-jwt/index.ts

# With a JWT token:
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
export KUBEMQ_JWT=<your-kubemq-jwt-token>
npx tsx connectivity/auth-jwt/index.ts
```

## Expected Output (auth-disabled broker)

```
[auth-jwt] KubeMQ MQTT password-as-JWT authentication example
[auth-jwt] Auth model: CONNECT.Password = JWT token (PasswordFlag=true)
[auth-jwt] Username field is display-only and ignored for auth decisions

[test] empty password -> expect CONNACK 0x86 if auth is enabled, 0x00 if disabled...
[empty-password] connected (CONNACK 0x00)
[test] empty password accepted -> auth is DISABLED on this broker (expected on open broker)

[info] KUBEMQ_JWT not set -> skipping authenticated pub/sub demo
```

## What's Happening

- The KubeMQ MQTT connector uses `CONNECT.Password` as a JWT token for authentication.
- `CONNECT.Username` is for display/audit only; it is NOT checked against any user store.
- Auth is checked at connect-time only (no mid-connection recheck).
- When auth is enabled: empty or invalid password → CONNACK 0x86; ACL deny on publish → PUBACK 0x87.

## MQTT Specifics

| Property | Value |
|----------|-------|
| Protocol version | MQTT 5.0 |
| Topic | `events/demo/auth` |
| QoS | 1 |
| `CONNECT.Username` | display-only; ignored for auth |
| `CONNECT.Password` | KubeMQ JWT token (`PasswordFlag=true`) |
| Empty password (auth on) | CONNACK 0x86 (bad auth) |
| ACL deny on publish | PUBACK 0x87 (not authorized) |

## Related Examples

- [connectivity/tls](../tls/) — TLS transport
- [connectivity/websocket](../websocket/) — WebSocket transport
- [events/basic-pubsub](../../events/basic-pubsub/) — same pub/sub pattern on an open broker
