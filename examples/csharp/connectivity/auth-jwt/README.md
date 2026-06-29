# csharp — Connectivity: Auth (password-as-JWT)

Connect to the KubeMQ MQTT connector with JWT-based authentication. KubeMQ uses the MQTT
CONNECT **Password** field to carry the JWT token (`PasswordFlag=true`); the Username field
is **display-only** and is NOT checked by the broker.

## Prerequisites

- .NET 8 or later
- **MQTTnet** 4.3.7.1207 (declared in `Directory.Packages.props`)
- KubeMQ server with MQTT connector enabled (default: `tcp://localhost:1883`)
- A valid HS256 JWT token if the broker has authentication enabled

## How to Run

```bash
cd examples/csharp/connectivity/auth-jwt

# With a JWT token (auth-enabled broker):
export KUBEMQ_MQTT_TOKEN=<your-jwt-token>
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run

# Without a token (succeeds on auth-disabled brokers):
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run
```

> `KUBEMQ_MQTT_TOKEN` is the preferred env var name; `KUBEMQ_MQTT_JWT` is accepted as an alias.

## Expected Output

**Auth-disabled broker (no JWT set):**

```
=== KubeMQ MQTT — connectivity/auth-jwt ===
[1] Broker URL : tcp://localhost:1883
[1] KUBEMQ_MQTT_TOKEN not set — connecting WITHOUT credentials.
    To test JWT auth: export KUBEMQ_MQTT_TOKEN=<your-token>
[2] Client ID  : csharp-auth-<uuid-prefix>
[3] Credentials: none (auth-disabled broker accepts open connections)
[4] Connecting to 127.0.0.1:1883 via MQTT 5.0 ...
[4] CONNACK 0x00: Connected successfully.
    (Auth disabled on broker — JWT enforcement not exercised)
[5] Subscribing to 'events/demo/auth' at QoS 1 ...
[5] SUBACK: topic=events/demo/auth granted=GrantedQoS1
[6] Publishing to 'events/demo/auth' ...
[6] PUBACK reason: Success
[7] Waiting for echo ...
[recv] topic=events/demo/auth payload={"msg":"auth connectivity test","ts":"..."}
[7] Echo received: {"msg":"auth connectivity test","ts":"..."}
[8] Disconnected cleanly.

Summary:
  Broker   : 127.0.0.1:1883
  ClientID : csharp-auth-<uuid-prefix>
  JWT used : no (auth-disabled broker)
  Publish  : PUBACK=Success
```

**Auth-enabled broker (JWT supplied):**

```
[4] CONNACK 0x00: Connected successfully.
    (JWT accepted — full enforcement requires auth-enabled broker)
```

**Wrong or missing JWT on auth-enabled broker:**

```
[4] CONNACK 0x86: Bad credentials — empty or invalid JWT.
    Auth is ENABLED on this broker. Provide a valid KUBEMQ_MQTT_TOKEN.
```

**ACL denies the client:**

```
[4] CONNACK 0x87: Not authorized — ACL denied this client.
```

## What's Happening

1. **Read configuration** — `KUBEMQ_MQTT_URL` selects the broker (tcp/tls/ws scheme parsed);
   `KUBEMQ_MQTT_TOKEN` carries the JWT token. A unique client ID is generated per run.

2. **Build MQTT 5.0 options** — `MqttClientOptionsBuilder` is configured with MQTT protocol
   version 5.0, a 30-second keep-alive, and clean session. When a JWT is present,
   `WithCredentials(clientId, jwt)` sets `Username=clientId` (display-only) and
   `Password=<JWT>` (`PasswordFlag=true`) in the CONNECT packet.

3. **Connect** — `client.ConnectAsync()` sends the CONNECT packet. The broker's auth hook
   extracts the Password and validates the JWT signature.
   - CONNACK `0x00` — success (or auth disabled).
   - CONNACK `0x86` — bad/missing JWT when auth is enabled.
   - CONNACK `0x87` — JWT valid but ACL denies this client.

4. **Subscribe** to `events/demo/auth` at QoS 1 — demonstrates the session is fully usable
   after authentication. The topic prefix `events/` routes to the KubeMQ Events pattern
   (channel `demo.auth`).

5. **Publish** a JSON test message to `events/demo/auth` at QoS 1. PUBACK `0x00` (Success)
   confirms the broker accepted the message.

6. **Wait for echo** (3 s) — if another subscriber is on the same channel the message
   arrives back; otherwise the publish success alone proves authentication worked.

7. **Disconnect cleanly** with `DisconnectAsync()`.

## MQTT Specifics

| Property         | Value                                             |
|------------------|---------------------------------------------------|
| MQTT version     | 5.0 (`MqttProtocolVersion.V500`)                  |
| Topic (pub/sub)  | `events/demo/auth`                                |
| KubeMQ channel   | `demo.auth` (Events pattern)                      |
| QoS              | 1 (AtLeastOnce)                                   |
| Auth mechanism   | Password field = JWT token (`PasswordFlag=true`)  |
| Username field   | Client ID — display-only, NOT used for auth       |
| Keep-alive       | 30 s                                              |
| v5 properties    | None beyond standard CONNECT fields               |
| CONNACK on fail  | `0x86` bad credentials / `0x87` ACL denied        |
| Retain           | Not used (retain is disabled on KubeMQ MQTT)      |

## Gotchas

- **Password travels in cleartext** at the MQTT layer. In production, always combine JWT auth
  with the TLS listener (`tls://host:8883`) so the token is encrypted in transit.
- **Username is NOT the auth subject** — KubeMQ parses the JWT claims from the Password field.
  The Username value is available in audit logs but has no auth effect.
- **Auth is checked at connect time only** — there is no mid-connection recheck; a revoked
  token does not cause a disconnect until the next CONNECT.
- **Auth disabled → open** — if the broker has no auth configuration, any client (including
  those with no password or an arbitrary string) is accepted. This is the state of the
  reference broker used in CI; the example documents this and marks the JWT enforcement path
  as N/A.

## Related Examples

- [connectivity/tls](../tls/) — TLS transport (production complement for JWT auth)
- [connectivity/websocket](../websocket/) — WebSocket transport
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — v3.1.1 vs v5 differences
- [events/basic-pubsub](../../events/basic-pubsub/) — Events pattern basics
