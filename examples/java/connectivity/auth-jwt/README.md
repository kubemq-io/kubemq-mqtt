# java — Connectivity: Auth (password-as-JWT)

Connect to the KubeMQ MQTT broker using JWT token authentication. The JWT token is
placed in the MQTT CONNECT `Password` field (`PasswordFlag=true`). The `Username`
field is display-only and is **not used for authentication** by the KubeMQ MQTT
connector. After a successful connect, the example performs a pub/sub round-trip on
`events/demo/auth` to confirm the authenticated connection is functional.

**Live-test note:** Auth is **disabled** on the reference test broker
(`tcp://127.0.0.1:1883`). The connection succeeds regardless of the token value.
Full JWT enforcement (CONNACK `0x86` on invalid/missing token) requires an
auth-enabled broker. This example is marked **partial / N/A** for live enforcement.

## Prerequisites

- Java 21+, Maven 3.8+
- KubeMQ server (MQTT connector enabled; auth-enabled broker for full enforcement)
- A valid JWT token issued by KubeMQ's authentication service (optional on an open broker)

## How to Run

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
export KUBEMQ_MQTT_TOKEN=<your-jwt-token>   # omit on open test broker; default "demo-token"
cd examples/java/connectivity/auth-jwt
mvn compile exec:java
```

### Auth-Enabled Broker (full enforcement)

On a broker with auth configured:

```bash
export KUBEMQ_MQTT_URL=tcp://<auth-enabled-host>:1883
export KUBEMQ_MQTT_TOKEN=<valid-kubemq-jwt>
mvn compile exec:java
```

**Security note:** Always use TLS (`tls://`) in production. The JWT token travels
in the MQTT CONNECT `Password` field in **cleartext** at the MQTT layer. Sending a
JWT over unencrypted TCP exposes the credential on the wire.

## Expected Output

```
=== connectivity/auth-jwt (MQTT 5.0) ===
Broker  : tcp://127.0.0.1:1883
Topic   : events/demo/auth  (KubeMQ channel: demo.auth)
Token   : demo-token  [default — set KUBEMQ_MQTT_TOKEN for a real JWT]

NOTE: Auth is DISABLED on the test broker.
      JWT enforcement (CONNACK 0x86 on invalid token) requires
      an auth-enabled broker. This run demonstrates the setup.
      SECURITY: Always use TLS (tls://) in production to protect
      the JWT credential in transit (Password travels in cleartext).

[1] Connecting subscriber with JWT in Password field ...
[2] Subscribing to 'events/demo/auth' at QoS 1 ...
Subscriber connected to tcp://127.0.0.1:1883 (reconnect=false)
    SUBACK granted QoS=1
[3] Connecting publisher with JWT in Password field ...
Publisher  connected to tcp://127.0.0.1:1883 (reconnect=false)
[4] Publishing to 'events/demo/auth' QoS=1, payload='Hello with JWT auth!' ...
    PUBACK reason=0x00 (success)
[5] Waiting for message delivery (up to 10 s) ...
--- Message received ---
  Topic   : events/demo/auth
  QoS     : 1
  Payload : Hello with JWT auth!
  (Authenticated connection confirmed functional.)

Round-trip complete on authenticated connection.
  MQTT topic     : events/demo/auth
  KubeMQ channel : demo.auth
[6] Disconnecting ...
Done.
```

## What's Happening

1. **Broker URL** is read from `KUBEMQ_MQTT_URL` (default `tcp://localhost:1883`).
   The scheme selects the transport (`tcp://`, `tls://`, `ws://`).

2. **JWT token** is read from `KUBEMQ_MQTT_TOKEN` (default `"demo-token"` — harmless
   on an open broker).

3. **CONNECT with JWT** — `MqttConnectionOptions.setPassword(token.getBytes(UTF-8))`
   places the token in the MQTT CONNECT `Password` field. Paho automatically sets
   `PasswordFlag=true` when password is non-null. `setUserName("kubemq-mqtt-java-example")`
   sets a display-only identifier for log/audit visibility.

4. **Subscriber connects first** — the Events pattern is fire-and-forget; a message
   published before any subscriber is present is lost. Subscribe at QoS 1 before
   publishing.

5. **SUBACK** — granted QoS is checked; code `≥ 0x80` signals rejection
   (e.g., `0x87` = ACL deny).

6. **Publisher connects** with the same JWT credential, then publishes to
   `events/demo/auth` at QoS 1.

7. **PUBACK** — reason code `0x00` = success. `0x87` = ACL deny on publish. `0x86`
   would appear at CONNACK time (not PUBACK) for bad credentials.

8. **Message delivery** is awaited with a 10-second timeout using a
   `CountDownLatch`. An authenticated connection failure would surface here
   (or earlier at step 3/6 as a CONNACK `0x86`).

9. Both clients disconnect cleanly.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 (Paho v5 client always negotiates protocol level 5) |
| Auth transport | MQTT CONNECT `Password` field (Paho: `setPassword(bytes)`) |
| `PasswordFlag` | `true` (set automatically by Paho when password != null) |
| `Username` | Display-only sentinel; **not used for auth** by KubeMQ |
| Topic | `events/demo/auth` |
| KubeMQ channel | `demo.auth` (MQTT `/` maps to KubeMQ `.`) |
| QoS (pub + sub) | 1 |
| Retain | Not used (KubeMQ silently drops retained publishes — see Gotchas) |
| CONNACK `0x86` | Bad auth: empty password or invalid JWT (auth-enabled broker) |
| CONNACK `0x87` | Not authorized: valid JWT but ACL denies the client |
| SUBACK/PUBACK `0x87` | ACL deny on subscribe or publish |

## Gotchas

- **Retain silently dropped.** Setting `message.setRetained(true)` results in PUBACK
  `0x00` (success) but the message is **not delivered and not stored**. A
  `publish.error` is recorded in the broker audit log. Never use retain with KubeMQ's
  MQTT connector.

- **JWT in cleartext on unencrypted connections.** The MQTT `Password` field is not
  encrypted at the MQTT protocol layer. Always use `tls://host:8883` in production to
  protect the token in transit.

- **Auth checked at connect-time only.** The KubeMQ MQTT connector verifies the JWT
  at `CONNECT` time. There is no mid-connection recheck; a revoked token is not
  re-evaluated until the client reconnects.

- **Username is NOT the auth identity.** Unlike many MQTT brokers, KubeMQ does not
  use the `Username` field for authentication. The entire auth decision is based on
  the `Password` field (the JWT).

- **Events-Store is always StartNewOnly over MQTT.** Historical replay is not
  available; a subscriber on `store/...` receives only messages published after it
  subscribes.

- **Topic `/` maps to KubeMQ `.`** (lossy). A literal `.` in a topic segment
  conflates with `/`. Avoid dots in MQTT topic segments.

## Related Examples

- [connectivity/tls](../tls/) — TLS transport (protects the JWT in transit)
- [connectivity/websocket](../websocket/) — WebSocket transport
- [events/basic-pubsub](../../events/basic-pubsub/) — unauthenticated connection
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — MQTT version comparison
