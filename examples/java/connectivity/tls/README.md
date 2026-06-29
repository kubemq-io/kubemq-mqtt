# java — Connectivity: TLS

Connect to the KubeMQ MQTT broker over TLS using the `tls://` scheme and MQTT 5.0
(Eclipse Paho MQTTv5 client). After the encrypted handshake the example performs a
basic Events publish/subscribe round-trip to confirm the connection is functional.

> **Live-test note:** The TLS listener (`tls://127.0.0.1:8883`) is **closed** on the
> reference test broker. The code compiles and demonstrates the correct setup, but
> end-to-end verification requires a TLS-enabled KubeMQ broker. The live test is
> therefore marked **N/A** on the reference broker.

---

## Prerequisites

| Requirement | Detail |
|-------------|--------|
| Java | 21+ |
| Maven | 3.8+ |
| KubeMQ server | TLS MQTT listener enabled (port 8883) |
| TLS cert | Server CA cert for trust — or use InsecureSkipVerify for dev/test |

---

## How to Run

```bash
# Point at a TLS-enabled KubeMQ broker
export KUBEMQ_MQTT_URL=tls://127.0.0.1:8883

cd examples/java/connectivity/tls
mvn compile exec:java
```

---

## Expected Output

### Against a TLS-enabled broker (port 8883 open)

On a broker with the TLS MQTT listener enabled, the round-trip completes:

```
[connectivity/tls] Connecting to: tls://127.0.0.1:8883  (Paho scheme: ssl://127.0.0.1:8883)
[connectivity/tls] ClientID: kubemq-mqtt-tls-java-a1b2c3d4
[connectivity/tls] Topic: events/demo/tls  QoS: 1
[connectivity/tls] SSL: InsecureSkipVerify=true  (production: supply a trusted CA cert)
[connectivity/tls] Connected to: ssl://127.0.0.1:8883
[connectivity/tls] CONNACK: reasonCode=0x00 (0x00=success)
[connectivity/tls] Subscribing to: events/demo/tls at QoS 1
[connectivity/tls] SUBACK: reasonCode=0x01  (0x01=QoS1 granted; 0x80=failure; 0xA2=wildcard not supported)
[connectivity/tls] Publishing: topic=events/demo/tls  payload=Hello via TLS from Java MQTT 5.0! timestamp=1718198400000  QoS=1
[connectivity/tls] PUBACK received: reasonCode=0x00 (0x00=success; 0x80=broker not ready; 0x97=quota exceeded)
[connectivity/tls] Waiting for message delivery (max 5 s)...
[connectivity/tls] Received on topic=events/demo/tls  payload=Hello via TLS from Java MQTT 5.0! timestamp=1718198400000  QoS=1
[connectivity/tls] Round-trip complete over TLS.
[connectivity/tls] Disconnecting...
[connectivity/tls] Disconnected cleanly.
```

### On the reference test broker (port 8883 CLOSED) — live test N/A

The TLS listener is **closed** on the reference broker, so the SSL setup runs
correctly but the MQTT `CONNECT` cannot complete; the example prints its setup
lines and then fails at connect time (test status **N/A**, not a code defect):

```
[connectivity/tls] Connecting to: tls://127.0.0.1:8883  (Paho scheme: ssl://127.0.0.1:8883)
[connectivity/tls] ClientID: kubemq-mqtt-tls-java-<8hex>
[connectivity/tls] Topic: events/demo/tls  QoS: 1
[connectivity/tls] SSL: InsecureSkipVerify=true  (production: supply a trusted CA cert)
... client.connect() to the closed 8883 port fails (MqttException, no CONNACK) ...
```

The setup (SSL context, `tls://` → `ssl://` scheme mapping, socket factory, topic
`events/demo/tls` → channel `demo.tls`) is exercised and correct; only the network
connect to the closed port cannot succeed.

---

## What's Happening

1. **Read broker URL** from `KUBEMQ_MQTT_URL` (default `tls://localhost:8883`) and
   normalise the scheme for Paho. Paho v5 only registers the `tcp://`, `ssl://`,
   `ws://`, and `wss://` schemes, so the example maps `tls://` → `ssl://` before
   constructing the client (passing `tls://` verbatim throws *"no NetworkModule
   installed for scheme tls"*). The original `tls://` URL is still shown in the logs.

2. **Build SSLContext** — the example uses an all-trusting `TrustManager`
   (`InsecureSkipVerify`) so no certificate provisioning is needed for development.
   In production replace this with a proper `TrustManagerFactory` loaded from a
   KeyStore containing the server CA certificate (see the comment in `Main.java`).
   KubeMQ MQTT enforces TLS ≥ 1.2; mTLS is also supported when configured.

3. **Set `socketFactory`** on `MqttConnectionOptions` — Paho uses this factory to
   create the underlying `SSLSocket` before the MQTT CONNECT handshake.

4. **Connect** (`MQTT 5.0 CONNECT`) — `clean_start=true`, `keepalive=30 s`.
   On success the broker sends `CONNACK reasonCode=0x00`.

5. **Subscribe** to `events/demo/tls` at QoS 1. KubeMQ maps this to the Events
   pattern on channel `demo.tls` (the `/` in the suffix becomes `.`).
   The broker replies with `SUBACK reasonCode=0x01` (QoS 1 granted).

6. **Publish** to `events/demo/tls` at QoS 1. KubeMQ fans the event out to all
   matching subscribers. The broker acknowledges with `PUBACK reasonCode=0x00`.
   `retained=false` is explicit — KubeMQ MQTT forces `RetainAvailable=0`; a retained
   publish would be silently dropped (PUBACK still succeeds, message is discarded).

7. **Receive** — the subscriber callback fires; the payload is logged.

8. **Disconnect** — clean MQTT DISCONNECT is sent before closing the socket.

---

## MQTT Specifics

| Property | Value |
|----------|-------|
| Scheme | `tls://` (TCP with TLS wrapper) |
| Default port | `8883` |
| MQTT version | 5.0 |
| Topic | `events/demo/tls` |
| KubeMQ channel | `demo.tls` (topic `/` → channel `.`) |
| KubeMQ pattern | Events (fire-and-forget) |
| QoS | 1 (at-least-once) |
| Retained | `false` — RetainAvailable=0 on KubeMQ MQTT; retained publish is silently dropped |
| SSL | `javax.net.ssl.SSLContext` / `SSLSocketFactory` via `MqttConnectionOptions.setSocketFactory()` |
| TLS minimum | 1.2 (enforced by KubeMQ server) |

---

## Production TLS Setup

Replace the `insecureSslContext()` helper with a proper context:

```java
KeyStore ts = KeyStore.getInstance("JKS");
ts.load(new FileInputStream("truststore.jks"), "changeit".toCharArray());

TrustManagerFactory tmf = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm());
tmf.init(ts);

SSLContext ctx = SSLContext.getInstance("TLSv1.3");
ctx.init(null, tmf.getTrustManagers(), null);
opts.setSocketFactory(ctx.getSocketFactory());
```

For mutual TLS (mTLS), also initialise a `KeyManagerFactory` from the client
keystore and pass it as the first argument to `ctx.init(...)`.

---

## Relevant Reason Codes

| Code | Meaning in this example |
|------|------------------------|
| `0x00` | CONNACK / PUBACK / SUBACK success |
| `0x01` | SUBACK — QoS 1 granted |
| `0x80` | CONNACK broker not ready, or SUBACK/PUBACK general failure |
| `0x8B` | DISCONNECT server shutting down |
| `0x8F` | SUBACK topic-filter-invalid (empty segment in topic) |
| `0x90` | SUBACK topic-name-invalid (empty channel / DefaultPattern none) |
| `0x97` | PUBACK quota exceeded (user-property cap or RpcMaxPending) |
| `0x9A` | CONNACK retain-not-supported (Will-retain at CONNECT) |

---

## Related Examples

- [connectivity/websocket](../websocket/) — WebSocket transport (`ws://`)
- [connectivity/auth-jwt](../auth-jwt/) — JWT authentication over plain TCP
  (see also: combine with TLS for secure auth in production)
- [events/basic-pubsub](../../events/basic-pubsub/) — Events pub/sub over plain TCP
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — MQTT 3.1.1 vs 5.0 comparison
