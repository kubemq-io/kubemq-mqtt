# python — Connectivity: TLS

Demonstrates connecting to the KubeMQ MQTT connector over TLS (port 8883) and
performing a basic events pub/sub round-trip.

## Live-Test Status: N/A (TLS listener disabled on default broker)

The TLS listener (`tls://localhost:8883`) is **closed** on this deployment.
This example compiles and is logically correct, but the live connection requires
a broker started with `CONNECTORSMQTT_TLS_PORT=8883` and a Security configuration
that provides the server certificate and key.

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` (from `examples/python/`)
- KubeMQ server with TLS listener enabled (see [Configuration](#server-configuration) below)
- Optional: CA certificate path for proper server certificate verification

## How to Run

```bash
cd examples/python

# Point at the TLS listener (required)
export KUBEMQ_MQTT_URL=tls://localhost:8883

# Optional: provide a CA cert for full server certificate verification.
# Without this, InsecureSkipVerify=True is used (development / self-signed certs only).
# export KUBEMQ_MQTT_TLS_CA=/path/to/ca.crt

python connectivity/tls/main.py
```

## Expected Output

### TLS-enabled broker (InsecureSkipVerify, no CA set)

```
[tls] InsecureSkipVerify=True — server certificate not validated.
  NOTE: In production, set KUBEMQ_MQTT_TLS_CA=/path/to/ca.crt
        to enable full server certificate verification.
[tls] Connecting to localhost:8883 over TLS (MQTT 5.0) ...
[tls] Connected over TLS to localhost:8883
[tls] Subscribed to events/demo/tls (QoS 1)
[tls] SUBACK received — granted QoS: [1]
[tls] Published to events/demo/tls (QoS 1)
[tls] Message received on events/demo/tls: {"transport": "tls", "channel": "demo.tls"}
[tls] Round-trip complete.
Done.
```

### TLS listener disabled (default live broker)

Captured from a live run against `tls://127.0.0.1:8883`. Port 8883 is CLOSED on
the default KubeMQ deployment, so the connection is refused — this is the
expected outcome (the code is correct; live TLS testing is N/A until the listener
is enabled).

```
[tls] InsecureSkipVerify=True — server certificate not validated.
  NOTE: In production, set KUBEMQ_MQTT_TLS_CA=/path/to/ca.crt
        to enable full server certificate verification.
[tls] Connecting to 127.0.0.1:8883 over TLS (MQTT 5.0) ...
[tls] Could not connect to 127.0.0.1:8883: [Errno 61] Connection refused

  NOTE: The TLS listener is disabled on the default KubeMQ deployment.
  To enable it, start the KubeMQ server with:
    CONNECTORSMQTT_TLS_PORT=8883
  plus a Security configuration providing the server certificate and key.
```

## What's Happening

1. **Parse the URL** — `KUBEMQ_MQTT_URL` is read and split into `(scheme, host, port)`.
   Scheme `tls://` selects the TLS listener on port 8883.

2. **Build the TLS context** — if `KUBEMQ_MQTT_TLS_CA` is set, the server certificate
   is verified against the supplied CA bundle.  Otherwise `InsecureSkipVerify=True`
   is applied via `ssl.CERT_NONE` — acceptable for development and self-signed
   certificates but **must not** be used in production.

3. **Create the client** — `paho.mqtt.Client` with `protocol=MQTTv5` and
   `transport="tcp"`.  TLS is handled by paho-mqtt internally via
   `client.tls_set_context(ctx)` over the underlying TCP socket.

4. **Connect** — `client.connect(host, port, keepalive=30)` initiates the
   TLS handshake and MQTT CONNECT packet.  If the port is closed, an
   `OSError` is raised immediately and the example prints a descriptive error.

5. **Subscribe** — inside `on_connect`, subscribe to `events/demo/tls` at QoS 1.
   The MQTT topic prefix `events/` routes the message through the KubeMQ Events
   bridge; the rest of the topic (`demo/tls`) becomes the KubeMQ channel name
   `demo.tls` (MQTT `/` maps to KubeMQ `.`).

6. **Publish** — send a payload to `events/demo/tls` at QoS 1.  KubeMQ fans the
   event out to all active subscribers (the same client in this example).

7. **Receive** — `on_message` fires with the loopback event; the threading.Event
   is set.

8. **Disconnect** — `client.loop_stop()` + `client.disconnect()` for a clean exit.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 (`MQTTv5`) |
| Transport | TCP with TLS (`tls_set_context`) |
| Port | 8883 |
| Topic (subscribe) | `events/demo/tls` |
| Topic (publish) | `events/demo/tls` |
| QoS | 1 |
| KubeMQ channel | `demo.tls` (slash → dot) |
| KubeMQ pattern | Events |
| MQTT 5.0 properties | None (plain pub/sub) |

## Server Configuration

To enable the TLS listener on the KubeMQ server:

```bash
# Port to listen on (default 8883, disabled when Security config is absent)
CONNECTORSMQTT_TLS_PORT=8883

# KubeMQ Security config must also supply:
#   - Server certificate (PEM)
#   - Server private key (PEM)
#   - Optional: client CA for mTLS
```

TLS minimum version: 1.2. Mutual TLS (mTLS) is supported.

## Gotchas

- **InsecureSkipVerify in production** — skipping certificate verification exposes
  you to man-in-the-middle attacks.  Always supply a CA bundle (`KUBEMQ_MQTT_TLS_CA`)
  in any non-development environment.

- **Retain not supported** — publishing with the MQTT retain flag set will succeed
  at the PUBACK level (reason code `0x00`) but the message is **silently dropped**
  by the broker.  Do not rely on retained messages with KubeMQ MQTT.

- **Events-Store is StartNewOnly** — if you switch the topic prefix to `store/`,
  there is no historical replay over MQTT.  Only messages published after the
  subscription is established are delivered.

- **JWT in cleartext without TLS** — the MQTT CONNECT `Password` field carries
  the JWT token in cleartext over a plain TCP connection.  Always use TLS when
  authentication is enabled.

## Related Examples

- [connectivity/websocket](../websocket/) — WebSocket transport (`ws://host:8083/`)
- [connectivity/auth_jwt](../auth_jwt/) — JWT authentication (combine with TLS in production)
- [events/basic_pubsub](../../events/basic_pubsub/) — same pub/sub pattern over plain TCP
