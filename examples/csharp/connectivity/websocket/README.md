# csharp — Connectivity: WebSocket

Connect to the KubeMQ MQTT connector over **WebSocket** (`ws://`) and run a
basic Events pub/sub round-trip using **MQTT 5.0** and **MQTTnet v4**.

The KubeMQ WebSocket listener lives at `ws://<host>:8083/` (path `/` is
required).  Both MQTT 3.1.1 and MQTT 5.0 clients are accepted; this example
uses MQTT v5 to demonstrate User Properties (KubeMQ Tags) over WebSocket.

---

## Prerequisites

- .NET 8+
- KubeMQ server with the WebSocket MQTT listener enabled
  (default `ws://localhost:8083/`)

---

## How to Run

```bash
cd examples/csharp/connectivity/websocket
KUBEMQ_MQTT_URL=ws://127.0.0.1:8083/ dotnet run
```

If `KUBEMQ_MQTT_URL` is unset, the example defaults to `ws://localhost:8083/`.

---

## Expected Output

```
Broker (WebSocket): ws://127.0.0.1:8083/
Subscribe topic:    events/demo/ws  (KubeMQ channel demo.ws)
Publish topic:      events/demo/ws  (KubeMQ channel demo.ws)

[1] Connecting subscriber over WebSocket...
    Subscriber connected  (ResultCode=Success)
    Subscribed to 'events/demo/ws'  (SUBACK=GrantedQoS1)

[2] Connecting publisher over WebSocket...
    Publisher connected  (ResultCode=Success)
    Published to 'events/demo/ws' with UserProperty source=csharp-ws-example  (PUBACK=Success)

[3] Waiting for event delivery...
    Received payload:       {"message":"hello via WebSocket","channel":"demo.ws","ts":"..."}
    Received UserProperty:  source=csharp-ws-example
    User-property 'source' round-tripped as a KubeMQ Tag.

[4] Disconnecting...
    Done.
```

---

## What's Happening

1. **Parse WebSocket URL** — `KUBEMQ_MQTT_URL` is validated to start with
   `ws://` or `wss://`; otherwise the example exits with a clear error.

2. **Subscriber connects** — `MqttClientOptionsBuilder.WithWebSocketServer`
   selects the WebSocket transport.  MQTT protocol version is set to **v5**
   (`MqttProtocolVersion.V500`).  The client subscribes to
   `events/demo/ws` at QoS 1 and waits 300 ms for the broker to register the
   subscription before publishing.

3. **Publisher connects** — a second client connects over the same WebSocket
   endpoint and publishes to `events/demo/ws` with a MQTT 5.0 User Property
   (`source=csharp-ws-example`).  KubeMQ forwards User Properties as Tags;
   the subscriber receives them back in `UserProperties`.

4. **Event delivery** — the subscriber receives the payload and logs the
   echoed User Property, proving the full WebSocket pub/sub round-trip.

5. **Clean disconnect** — both clients send a DISCONNECT packet before
   closing the connection.

---

## MQTT Specifics

| Property | Value |
|---|---|
| Transport | WebSocket (`ws://`) |
| MQTT version | 5.0 |
| Subscribe topic | `events/demo/ws` |
| Publish topic | `events/demo/ws` |
| KubeMQ channel | `demo.ws` (topic `/` -> `.`) |
| QoS | 1 (AtLeastOnce) |
| MQTT 5.0 properties used | User Properties (`source=csharp-ws-example`) |
| Retain | NOT used — KubeMQ forces `RetainAvailable=0`; publishing with `Retain=true` is silently stripped (PUBACK `0x00` but message never delivered) |
| Wildcards | Not used in this variant (see `events/wildcard-subscribe`) |

### Reason codes handled

| Code | Meaning | Where |
|---|---|---|
| `0x00` Success | CONNACK / SUBACK GrantedQoS1 / PUBACK OK | All steps |
| `0x80` Broker not ready | PUBACK — publish gated at startup | Publisher |
| `0x87` Not authorized | PUBACK — ACL deny | Publisher |
| `0x90` TopicNameInvalid | PUBACK — empty channel or `DefaultPattern=none` | Publisher |
| `0x97` QuotaExceeded | PUBACK — User Property cap (32 props / 4096 bytes) | Publisher |
| `0x8F` TopicFilterInvalid | SUBACK — empty topic segment | Subscriber |
| `0xA2` WildcardsNotSupported | SUBACK — wildcard on non-Events topics | Subscriber |

---

## Gotchas

- **Retain silently dropped**: Publishing with `Retain=true` returns PUBACK
  `0x00` (success) but the message is **never delivered** and an error is
  audited server-side.  `RetainAvailable=0` is advertised in CONNACK.
- **Literal `.` in topic segments**: `events/demo.ws` and `events/demo/ws`
  both map to KubeMQ channel `demo.ws`.  Avoid dots in topic path segments to
  prevent unintended channel aliasing.
- **WS path must be `/`**: The KubeMQ WebSocket listener is registered at
  path `/`; using any other path will fail the WebSocket handshake.

---

## Related Examples

- [connectivity/tls](../tls/) — TLS transport (`tls://host:8883`)
- [connectivity/auth-jwt](../auth-jwt/) — JWT authentication
- [events/basic-pubsub](../../events/basic-pubsub/) — Events pub/sub over TCP with wildcards and User Properties
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — MQTT 3.1.1 vs 5.0 feature comparison
