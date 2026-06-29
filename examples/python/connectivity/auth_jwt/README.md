# python — Connectivity: Auth (password-as-JWT)

Demonstrates password-as-JWT authentication for the KubeMQ MQTT connector.
The MQTT CONNECT `Password` field carries a KubeMQ JWT token; the `Username` field
is display-only and is NOT used for authentication.

## Live-Test Status: Partial / N/A

Auth appears **disabled** on the live broker (`tcp://127.0.0.1:1883`).
Connecting with a JWT in the `Password` field succeeds, but JWT enforcement is **not
active** — CONNACK `0x86` (bad auth) is not returned.  Full JWT rejection testing
requires an auth-enabled broker.  Do NOT treat the absence of rejection as a failure.

## Prerequisites

- Python 3.10+
- Install dependencies (from `examples/python/`):
  ```bash
  pip install -r requirements.txt   # paho-mqtt >= 2.1.0
  ```
- KubeMQ server with MQTT connector enabled (auth-enabled for full JWT enforcement)
- A valid KubeMQ JWT token (set `KUBEMQ_MQTT_TOKEN`)

## How to Run

```bash
cd examples/python

# Optional: set your JWT token (required on auth-enabled brokers)
export KUBEMQ_MQTT_TOKEN=<your-kubemq-jwt-token>

# Optional: override the broker URL
export KUBEMQ_MQTT_URL=tcp://localhost:1883

python connectivity/auth_jwt/main.py
```

## Expected Output

**Auth-enabled broker, valid JWT:**

```
Broker : tcp://localhost:1883  (scheme=tcp, host=localhost, port=1883)
MQTT topic  : events/demo/auth
KubeMQ ch   : demo.auth  (/ -> . translation)
JWT token   : eyJhbGciOiJSUzI1...  (Password field, PasswordFlag=true)
[auth] CONNACK 0x00 — connected to localhost:1883
[auth] SUBACK mid=1 reason_code=1
[auth] Published  topic=events/demo/auth  payload='{"auth": "jwt-demo"}'  mid=1
[auth] PUBACK received  mid=1  reason_code=0x00
[auth] Received on events/demo/auth: '{"auth": "jwt-demo"}'
[ok] Auth connect + pub/sub round-trip verified.
Done.
```

**Auth-enabled broker, empty / missing `KUBEMQ_MQTT_TOKEN`:**

```
[auth] KUBEMQ_MQTT_TOKEN is not set — connecting without credentials.
       On an auth-enabled broker this will fail with CONNACK 0x86.
       On this broker (auth disabled) the connection will succeed.
[auth] CONNACK 0x86 — bad username or password (JWT rejected by auth-enabled broker)
Done (connection refused — no publish attempted).
```

**Auth disabled (live broker default — no `KUBEMQ_MQTT_TOKEN` needed):**

Captured from a live run against `tcp://127.0.0.1:1883` (auth disabled — the
connection succeeds and JWT enforcement is not exercised). The PUBACK callback
fires before the publishing print returns, so those two lines can appear in
either order.

```
Broker : tcp://127.0.0.1:1883  (scheme=tcp, host=127.0.0.1, port=1883)
MQTT topic  : events/demo/auth
KubeMQ ch   : demo.auth  (/ -> . translation)
[auth] KUBEMQ_MQTT_TOKEN is not set — connecting without credentials.
       On an auth-enabled broker this will fail with CONNACK 0x86.
       On this broker (auth disabled) the connection will succeed.
[auth] CONNACK 0x00 — connected to 127.0.0.1:1883
[auth] SUBACK mid=1 reason_code=1
[auth] PUBACK received  mid=2  reason_code=0x00
[auth] Published  topic=events/demo/auth  payload='{"auth": "jwt-demo"}'  mid=2
[auth] Received on events/demo/auth: '{"auth": "jwt-demo"}'
[ok] Auth connect + pub/sub round-trip verified.
Done.
```

## What's Happening

1. **Parse the broker URL** from `KUBEMQ_MQTT_URL` (default `tcp://localhost:1883`).
   The scheme selects the transport: `tcp` → plain TCP, `tls` → TLS, `ws` → WebSocket.

2. **Read the JWT token** from `KUBEMQ_MQTT_TOKEN`.  If unset the token is empty —
   this succeeds when auth is disabled but returns `CONNACK 0x86` on an auth-enabled
   broker.

3. **Build a paho-mqtt `MQTTv5` client** and call `username_pw_set()`.
   The `username` argument is informational only; `password` carries the JWT.
   paho encodes this as a standard MQTT CONNECT packet with `PasswordFlag=true`.

4. **Connect** (`connect()` + `loop_start()`).  The broker validates the `Password`
   field immediately.
   - Valid JWT (auth on) → `CONNACK 0x00` (success).
   - Empty / invalid password (auth on) → `CONNACK 0x86` (bad auth); example exits.
   - Auth disabled → `CONNACK 0x00` (open access; any password accepted).

5. **Subscribe** to `events/demo/auth` at QoS 1 on the same connection, then wait
   for `SUBACK`.

6. **Publish** one message to `events/demo/auth` at QoS 1.
   The connector maps the topic prefix `events/` to the KubeMQ Events pattern and
   translates `/` → `.` in the channel name (`demo.auth`).

7. **Wait** for the loopback message delivered to the subscriber and assert round-trip
   success.  Disconnect cleanly.

## MQTT Specifics

| Property | Value |
|----------|-------|
| MQTT version | 5.0 (`MQTTv5`) |
| Topic | `events/demo/auth` |
| KubeMQ channel | `demo.auth` (MQTT `/` maps to KubeMQ `.`) |
| KubeMQ pattern | Events (fire-and-forget pub/sub) |
| QoS | 1 (both publish and subscribe) |
| MQTT CONNECT `Username` | Display-only; NOT used for auth |
| MQTT CONNECT `Password` | KubeMQ JWT token (`PasswordFlag=true`) |
| CONNACK on bad/empty password | `0x86` Bad Username or Password |
| CONNACK on ACL deny (publish) | `0x87` Not Authorized |
| v5 User Properties | None used in this example |

## Security Gotcha

The JWT travels in the MQTT CONNECT `Password` field **in cleartext** at the MQTT
layer.  In production you MUST combine this pattern with the TLS listener (port 8883)
or WebSocket-over-TLS (`wss://`) so the token is encrypted in transit.  Running
password-as-JWT over plain TCP exposes the token to any network observer.

See: [connectivity/tls](../tls/)

## Related Examples

- [connectivity/tls](../tls/) — TLS transport (combine with auth for production)
- [connectivity/websocket](../websocket/) — WebSocket transport
- [events/basic_pubsub](../../events/basic_pubsub/) — auth-agnostic pub/sub
