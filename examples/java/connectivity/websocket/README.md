# java — Connectivity: WebSocket

Connect to the KubeMQ MQTT broker over WebSocket (`ws://` scheme) using **MQTT 5.0**.
The Paho v5 client transparently frames MQTT bytes inside WebSocket, so the MQTT
protocol is identical to plain TCP — the only difference is the transport layer.

## Prerequisites

- Java 21+, Maven 3.8+
- KubeMQ server running with the WebSocket MQTT listener on port 8083  
  (the default `ws://127.0.0.1:8083/` path is `/`)

## How to Run

```bash
export KUBEMQ_MQTT_URL=ws://127.0.0.1:8083/
cd examples/java/connectivity/websocket
mvn compile exec:java
```

If `KUBEMQ_MQTT_URL` is not set the example defaults to `ws://localhost:8083/`.

## Expected Output

```
=== connectivity/websocket ===
Broker   : ws://127.0.0.1:8083/
ClientID : kubemq-mqtt-java-ws-<8hex>
Topic    : events/demo/ws  (KubeMQ Events channel: demo.ws)

[sub] Connecting ...
[sub] Connected via WebSocket (MQTT 5.0).
[sub] Subscribing to 'events/demo/ws' at QoS 1 ...
[sub] Connected to: ws://127.0.0.1:8083
[sub] Subscribed.

[pub] Connecting ...
[pub] Connected via WebSocket (MQTT 5.0).
[pub] Publishing to 'events/demo/ws' (QoS 1): Hello via WebSocket from Java MQTT 5.0!
[pub] Connected to: ws://127.0.0.1:8083
[sub] Received on 'events/demo/ws' (QoS 1): Hello via WebSocket from Java MQTT 5.0!
[pub] PUBACK received (reason: 0x00(Success)).

Round-trip complete over WebSocket transport.
[pub] Disconnected.
[sub] Disconnected.

=== Done ===
```

> Note: the `[…] Connected to: …` line is printed by Paho's async `connectComplete`
> callback, so it interleaves with the synchronous step logging and reports the
> server URI without the trailing `/` (Paho normalizes it). The exact interleaving
> of the connect-callback, PUBACK, and delivery lines varies run-to-run.

## What's Happening

1. **Broker URL parsed** — `KUBEMQ_MQTT_URL` is read; the `ws://` scheme tells the
   Paho `MqttAsyncClient` to upgrade the connection to WebSocket before the MQTT
   handshake.  No extra configuration is needed; scheme detection is built into Paho.

2. **Subscriber connects** — a subscriber client connects with `cleanStart=true` and
   keepalive 30 s using **MQTT 5.0** (Paho v5 client always negotiates protocol level 5).

3. **Subscribe** — the subscriber subscribes to `events/demo/ws` at QoS 1.  The KubeMQ
   connector maps this topic to the **Events** pattern on channel `demo.ws`
   (`/` → `.` substitution).  A short pause lets the subscription register in the bridge.

4. **Publisher connects** — a separate publisher client connects over the same WebSocket
   endpoint.

5. **Publish** — one message is sent to `events/demo/ws` at QoS 1 with `retain=false`.
   The broker returns PUBACK 0x00 (success) immediately.

6. **Message received** — the subscriber callback fires; the round-trip is confirmed.

7. **Clean disconnect** — both clients send DISCONNECT and close gracefully.

## MQTT Specifics

| Property | Value |
|---|---|
| Broker scheme | `ws://` (WebSocket; no TLS) |
| Default URL | `ws://localhost:8083/` |
| WS path | `/` (fixed by the KubeMQ listener) |
| MQTT version | 5.0 (protocol level 5) |
| Topic | `events/demo/ws` |
| KubeMQ pattern | Events (fire-and-forget pub/sub) |
| KubeMQ channel | `demo.ws` (`/` → `.`) |
| QoS | 1 (at-least-once) |
| retain | `false` — **must not be set** (see gotcha below) |
| MQTT 5.0 properties | none required for basic pub/sub |

## Gotcha: Retain Is Silently Dropped

The KubeMQ MQTT connector forces `RetainAvailable = 0`.  If a PUBLISH is sent with
`retain = true` at runtime, the broker returns **PUBACK 0x00 (success)** but the
message is **silently discarded** — it is never delivered and never stored.
A `publish.error` entry is added to the broker audit log but no error is surfaced to
the client.  Always set `retain = false`.

Note: `retain = true` in a CONNECT Will message results in **CONNACK 0x9A**
(retain not supported) — that is the one case where a reject IS sent.

## Related Examples

- [connectivity/tls](../tls/) — MQTT over TLS (`tls://` scheme, port 8883)
- [connectivity/auth-jwt](../auth-jwt/) — password-as-JWT authentication
- [events/basic-pubsub](../../events/basic-pubsub/) — basic Events pub/sub over plain TCP
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — comparing MQTT 3.1.1 and 5.0
