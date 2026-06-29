# KubeMQ MQTT — Python Examples

All Python examples use [paho-mqtt](https://github.com/eclipse/paho.mqtt.python) 2.x (MQTT 5.0 via `MQTTv5`).

## Prerequisites

- Python 3.10+
- KubeMQ server running with MQTT connector enabled (default port 1883)

## Setup

```bash
cd examples/python
pip install -r requirements.txt
# or with uv:
uv pip install -r requirements.txt
```

## Broker URL

All examples read the `KUBEMQ_MQTT_URL` environment variable (default `tcp://localhost:1883`).
The scheme selects the transport:

| Scheme | Transport | Default address |
|--------|-----------|-----------------|
| `tcp`  | Plain TCP | `tcp://localhost:1883` |
| `tls`  | TLS/SSL   | `tls://localhost:8883` |
| `ws`   | WebSocket | `ws://localhost:8083/` |

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883   # default
```

## Run Any Example

```bash
export KUBEMQ_MQTT_URL=tcp://localhost:1883
python events/basic_pubsub/main.py
```

## Examples

| Pattern | Variant | Directory |
|---------|---------|-----------|
| Events | Basic Pub/Sub | [events/basic_pubsub/](events/basic_pubsub/) |
| Events | Wildcard Subscribe | [events/wildcard_subscribe/](events/wildcard_subscribe/) |
| Events-Store | Start-New-Only | [events_store/start_new_only/](events_store/start_new_only/) |
| Queues | Shared Consume | [queues/shared_consume/](queues/shared_consume/) |
| Queues | Ack and Redelivery | [queues/ack_and_redelivery/](queues/ack_and_redelivery/) |
| RPC | Query Round-Trip | [rpc/query_round_trip/](rpc/query_round_trip/) |
| RPC | Command Round-Trip | [rpc/command_round_trip/](rpc/command_round_trip/) |
| Connectivity | TLS | [connectivity/tls/](connectivity/tls/) |
| Connectivity | WebSocket | [connectivity/websocket/](connectivity/websocket/) |
| Connectivity | Auth JWT | [connectivity/auth_jwt/](connectivity/auth_jwt/) |
| Protocol | MQTT v3.1.1 vs v5 | [protocol/mqtt_v311_vs_v5/](protocol/mqtt_v311_vs_v5/) |

## Key Constraints (read before writing examples)

1. **Retain is silently dropped** — PUBACK 0x00 succeeds but the message is never delivered.
2. **Events-Store over MQTT is always `StartNewOnly`** — no historical replay.
3. **`$share` group name is audit-only** — all groups compete in one shared KubeMQ queue pool.
4. **RPC requires MQTT 5.0** — a v3.1.1 publish to `commands/` or `queries/` is silently dropped. PUBACK is sent immediately (not after the response); implement your own response-wait timeout.
5. **Literal `.` in a topic segment conflates with `/`** — `events/a.b/c` and `events/a/b/c` both map to channel `a.b.c`.
6. **Overlapping wildcard event filters each deliver one copy** — N overlapping matching entries → N copies delivered (no cross-entry dedup).
