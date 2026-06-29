# python — RPC: Query Round-Trip

Demonstrates a KubeMQ Queries RPC round-trip via MQTT 5.0.

The MQTT client acts as the **caller** only. It publishes a query to
`queries/demo/rpc`, waits for a response on its private reply inbox, and logs
the response payload, echoed CorrelationData, and user-properties (KubeMQ tags).

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` (from `examples/python/`) — installs `paho-mqtt >= 2.0`
- KubeMQ server with MQTT connector enabled
- **MQTT 5.0 required** — v3.1.1 publishes to `queries/` are silently dropped
- **A gRPC-side responder must be running** on channel `demo.rpc` (type `query`)

## gRPC Responder

MQTT clients **cannot** act as RPC responders. Subscribing to `queries/` from
an MQTT client returns SUBACK `0x83`. Responders are always on the KubeMQ gRPC
side.

The shared test-responder for this example is at:

```
kubemq-mqtt/.work/test-harness/responder/
```

Start it before running the example:

```bash
cd kubemq-mqtt/.work/test-harness/responder
go run . -channel demo.rpc -type query
```

## How to Run

```bash
# Terminal 1 — gRPC responder (keep running)
cd kubemq-mqtt/.work/test-harness/responder
go run . -channel demo.rpc -type query

# Terminal 2 — run the example
cd examples/python
export KUBEMQ_MQTT_URL=tcp://localhost:1883   # default; adjust as needed
python rpc/query_round_trip/main.py
```

## Expected Output

Captured from a live run against `tcp://127.0.0.1:1883` with the gRPC responder
running on channel `demo.rpc` (type `query`). `CorrelationData` is per-run unique
random bytes.

```
[client] connecting to tcp://127.0.0.1:1883 as client-id='python-query-rpc-client'
[client] connected (rc=0x00)
[client] subscribing to reply inbox '$reply/python-query-rpc-client/inbox' (QoS 1)
[client] SUBACK 0x01 — subscribed to reply inbox OK
[client] publishing query to 'queries/demo/rpc'
  payload:          {"query": "hello from MQTT"}
  ResponseTopic:    $reply/python-query-rpc-client/inbox
  CorrelationData:  aef80701482640988eccaf6b455fdfd6
[client] PUBACK 0x00 — broker accepted the query (PUBACK is immediate; response arrives separately)
[client] waiting up to 30s for query response on '$reply/python-query-rpc-client/inbox'...
[client] query response received:
  inbox topic:      $reply/python-query-rpc-client/inbox
  payload:          pong:{"query": "hello from MQTT"}
  CorrelationData:  aef80701482640988eccaf6b455fdfd6 (matched)
  user_props:
    kubemq-metadata: responder/query
    rpc-type: query
    responder: mqtt-test
[client] disconnected
```

## What's Happening

1. **Connect** — MQTT 5.0 client connects to the broker.
2. **Subscribe to reply inbox** (`$reply/python-query-rpc-client/inbox`, QoS 1)
   *before* publishing. The `$reply/<clientID>/...` namespace is mochi-local: it
   is never routed to the KubeMQ array and is only reachable by the owning
   client.
3. **Publish query** to `queries/demo/rpc` with two MQTT 5.0 properties:
   - `ResponseTopic` — the broker routes the gRPC response back here.
   - `CorrelationData` — opaque bytes echoed verbatim on the response so the
     caller can match responses to requests.
4. **PUBACK arrives immediately** — the broker acknowledges receipt, NOT
   completion. The client must implement its own response-wait timeout
   (`KUBEMQ_RPC_TIMEOUT`, default 30 s).
5. **gRPC responder** on channel `demo.rpc` receives the query, runs
   `SendQueryResponse`, and the broker delivers the response to the reply inbox.
6. **Receive response** on `$reply/python-query-rpc-client/inbox` — payload
   contains the responder's reply body; `UserProperty` carries
   `kubemq-metadata` and any tags the responder attached.
7. **Disconnect** cleanly.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 (required) |
| Query topic | `queries/demo/rpc` |
| KubeMQ channel | `demo.rpc` (topic `/` → channel `.`) |
| Reply inbox | `$reply/python-query-rpc-client/inbox` |
| Publish QoS | 1 |
| Subscribe QoS | 1 |
| MQTT 5.0 property — publish | `ResponseTopic` = `$reply/python-query-rpc-client/inbox` |
| MQTT 5.0 property — publish | `CorrelationData` = random UUID bytes |
| MQTT 5.0 property — publish | `UserProperty` = request tags (become KubeMQ tags) |
| MQTT 5.0 property — response | `CorrelationData` = echoed from request |
| MQTT 5.0 property — response | `UserProperty` = `kubemq-metadata` + responder tags |

## Gotchas

- **PUBACK is immediate**: it signals that the broker received the PUBLISH, NOT
  that the gRPC responder has replied. Always wait on the reply inbox with a
  timeout.
- **Own `$reply` namespace only**: `ResponseTopic` must begin with
  `$reply/<own-clientID>/`. Using another client's namespace returns PUBACK
  `0x83`.
- **v3.1.1 silent drop**: a v3.1.1 client that publishes to `queries/` will
  receive PUBACK `0x00` but the message is silently discarded by the connector.
  MQTT 5.0 is mandatory for RPC.
- **`RpcMaxPending` quota**: if too many unanswered RPCs are in flight,
  PUBACK `0x97` is returned.
- **No retain**: a PUBLISH with the Retain flag set is silently dropped at
  runtime (PUBACK `0x00` still arrives; the message is not delivered or stored).

## Related Examples

- [rpc/command_round_trip](../command_round_trip/) — Commands RPC (no response body; uses `kubemq-executed` user-prop)
- [queues/shared_consume](../../queues/shared_consume/) — guaranteed delivery alternative via Queues
- [events/basic_pubsub](../../events/basic_pubsub/) — fire-and-forget Events (no reply inbox needed)
