# python — Connectivity: WebSocket

Demonstrates connecting to the KubeMQ MQTT connector over WebSocket
(`ws://host:8083/`) using MQTT 5.0 (paho-mqtt 2.x).

The KubeMQ MQTT connector exposes three transport listeners:

| Listener | Default URL | Notes |
|----------|-------------|-------|
| TCP | `tcp://host:1883` | Plain TCP, default |
| TLS | `tls://host:8883` | TLS over TCP (must be configured) |
| WebSocket | `ws://host:8083/` | MQTT over WebSocket, path `/` |

This example exercises the WebSocket listener.  All KubeMQ MQTT patterns
(Events, Events-Store, Queues, RPC) work over WebSocket exactly as they do
over TCP — the transport is transparent to the broker.

## Prerequisites

- Python 3.10+
- Install dependencies (from `examples/python/`):
  ```bash
  pip install -r requirements.txt   # paho-mqtt>=2.1.0
  ```
- KubeMQ server running with the WebSocket listener enabled (default port 8083).
  Set `CONNECTORSMQTT_WS_PORT=8083` to enable (it is enabled by default).

## How to Run

```bash
cd examples/python
export KUBEMQ_MQTT_URL=ws://localhost:8083/
python connectivity/websocket/main.py
```

The example also respects any other `ws://` URL (e.g. a remote broker):

```bash
export KUBEMQ_MQTT_URL=ws://192.168.1.10:8083/
python connectivity/websocket/main.py
```

## Expected Output

Captured from a live run against `ws://127.0.0.1:8083/`.

```
Broker     : ws://127.0.0.1:8083/  (scheme=ws, host=127.0.0.1, port=8083)
Transport  : websockets
MQTT topic : events/demo/ws
KubeMQ ch  : demo.ws  (/ -> . translation)
[subscriber] Connected over WebSocket to ws://127.0.0.1:8083/
[subscriber] SUBACK mid=1 reason_code=0x01
[publisher]  Published to events/demo/ws  payload='{"via": "websocket"}'  user-prop transport=websocket  mid=1
[subscriber] Received on events/demo/ws
             payload    : '{"via": "websocket"}'
             user-props : [('transport', 'websocket')]  (forwarded as KubeMQ Tags)
[ok] payload and user-property (KubeMQ tag) round-trip verified over WebSocket
Done.
```

## What's Happening

1. **URL parsing** — `KUBEMQ_MQTT_URL` is read; the `ws://` scheme selects
   `transport="websockets"` in paho-mqtt.  A `tcp://` or `tls://` URL falls
   back to TCP transport (the example is not WS-only).
2. **Subscriber connects** — a separate MQTT 5.0 client is created with
   `transport="websockets"` and connects to `ws://host:8083/` with
   `keepalive=30` and `clean_start=True`.
3. **Subscribe** — `events/demo/ws` at QoS 1.  The topic prefix `events/`
   selects the KubeMQ Events pattern; `/` in the topic translates to `.` in
   the KubeMQ channel name (`demo.ws`).
4. **Publisher connects** — a second MQTT 5.0 client connects over the same
   WebSocket transport with a different `client_id`.
5. **Publish with User Properties** — the publisher attaches
   `transport=websocket` as an MQTT 5.0 User Property.  The connector
   forwards it as a KubeMQ Tag on the `demo.ws` channel.
6. **Receipt verified** — the subscriber prints the payload and user
   properties; the example asserts both match and exits cleanly.

## MQTT Specifics

| Attribute | Value |
|-----------|-------|
| Topic | `events/demo/ws` |
| KubeMQ channel | `demo.ws` (`/` → `.` translation) |
| Pattern | Events (fire-and-forget pub/sub) |
| QoS | 1 |
| MQTT version | 5.0 (`mqtt.MQTTv5`) |
| paho transport | `"websockets"` |
| WebSocket path | `/` (root; required by KubeMQ MQTT WS listener) |
| User Properties | `transport=websocket` forwarded as KubeMQ Tags |
| `clean_start` | `True` |
| `keepalive` | 30 s |

## Relevant Reason Codes

| Code | Meaning |
|------|---------|
| `0x00` | Success |
| `0x80` | Broker not ready (KubeMQ still starting) |
| `0x86` | Bad username or password (auth-enabled brokers) |
| `0x87` | Not authorized (ACL denied) |
| `0x8F` | Topic filter invalid (empty segment in topic) |
| `0x90` | Topic name invalid (empty channel / no DefaultPattern) |
| `0x97` | Quota exceeded (>32 user props or >4096 bytes total) |
| `0xA2` | Wildcard subscriptions not supported on non-events patterns |

## Notes

- **RETAIN is stripped silently** — the KubeMQ MQTT connector forces
  `RetainAvailable=0`.  A `PUBLISH` with the retain flag set will receive a
  successful PUBACK (0x00), but the message is **dropped** and not delivered.
  Avoid setting `retain=True` on any publish.
- **Events-Store is always StartNewOnly** — subscribing to `store/<ch>` only
  delivers messages published _after_ the subscription; there is no historical
  replay over MQTT.
- **WebSocket path must be `/`** — the KubeMQ WS listener listens at the root
  path.  paho-mqtt uses `/` by default when `transport="websockets"`, so no
  extra configuration is needed.
- **Both MQTT 3.1.1 and 5.0 work over WebSocket** — MQTT 5.0 is used here
  (per project convention D7) to demonstrate User Properties / KubeMQ Tags.

## Related Examples

- [connectivity/tls](../tls/) — TLS transport (port 8883)
- [connectivity/auth_jwt](../auth_jwt/) — JWT password authentication
- [events/basic_pubsub](../../events/basic_pubsub/) — same Events pub/sub pattern over TCP with full user-property walkthrough
- [protocol/mqtt_v311_vs_v5](../../protocol/mqtt_v311_vs_v5/) — side-by-side MQTT 3.1.1 vs 5.0 feature comparison
