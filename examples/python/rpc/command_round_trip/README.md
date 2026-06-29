# python — RPC: Command Round-Trip

Demonstrates a KubeMQ Commands RPC round-trip via MQTT 5.0.

The client publishes a command to `commands/demo/cmd`, then waits for the
response on its own reply inbox `$reply/python-command-client/inbox`.
A gRPC-side responder must be running on KubeMQ channel `demo.cmd`
(MQTT clients **cannot** be RPC responders — subscribing to `commands/`
returns SUBACK `0x83`).

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` (from `examples/python/`, installs `paho-mqtt>=2.0`)
- KubeMQ server with MQTT connector enabled on `tcp://localhost:1883`
- **MQTT 5.0 required** — v3.1.1 publishes to `commands/` are silently
  dropped (PUBACK still `0x00`, no error; see gotcha below)
- **A gRPC command responder must be running** on channel `demo.cmd`
  before the example is run

## How to Run

```bash
# Terminal 1 — start the gRPC command responder
cd kubemq-mqtt/.work/test-harness/responder
go run . -channel demo.cmd -type command

# Terminal 2 — run the example
cd examples/python
export KUBEMQ_MQTT_URL=tcp://localhost:1883   # default; override as needed
python rpc/command_round_trip/main.py
```

Override the RPC response timeout (default 30 s):

```bash
KUBEMQ_RPC_TIMEOUT=10 python rpc/command_round_trip/main.py
```

## Expected Output

Captured from a live run against `tcp://127.0.0.1:1883` with the gRPC responder
running on channel `demo.cmd` (type `command`). The correlation prefix is per-run
unique random bytes.

```
[client] connecting to tcp://127.0.0.1:1883
[client] connected (MQTT 5.0)
[client] subscribed to reply topic '$reply/python-command-client/inbox' (QoS 1)
[client] command published to 'commands/demo/cmd' (PUBACK=immediate, correlation=340321e5..., awaiting response...)
[client] command response received on '$reply/python-command-client/inbox'
  kubemq-executed:      true
  payload (empty):      b''
  correlation echoed:   True (340321e5...)
  user_props:           {'kubemq-executed': 'true'}
[client] command executed successfully
[client] disconnected
```

If no gRPC responder is running the connector times out after
`RpcTimeoutSeconds` (default 30 s) and no reply arrives:

```
[client] ERROR: no response within 32s.  Ensure a gRPC command responder ...
```

## What's Happening

| Step | Action |
|------|--------|
| 1 | Client connects as `python-command-client` with MQTT 5.0 |
| 2 | Client **subscribes** to `$reply/python-command-client/inbox` at QoS 1 — **must happen before publishing** |
| 3 | Client **publishes** to `commands/demo/cmd` with `ResponseTopic` + `CorrelationData` |
| 4 | Broker sends **PUBACK immediately** (not after execution) |
| 5 | KubeMQ connector bridges the command to gRPC channel `demo.cmd` |
| 6 | gRPC responder executes the command and sends back a `CommandReply` |
| 7 | Broker delivers the response to `$reply/python-command-client/inbox` |
| 8 | Client reads `kubemq-executed` user-prop; confirms payload is empty and `CorrelationData` is echoed |

## MQTT Specifics

| Property | Value |
|----------|-------|
| Publish topic | `commands/demo/cmd` |
| Subscribe topic (reply inbox) | `$reply/python-command-client/inbox` |
| QoS | 1 (both subscribe and publish) |
| MQTT version | **5.0 only** (v3.1.1 → silently dropped) |
| `ResponseTopic` | `$reply/python-command-client/inbox` (own `$reply/<clientID>/` namespace) |
| `CorrelationData` | random UUID bytes, echoed back in the response |
| Request user-props | `action=do`, `target=demo-server` (become KubeMQ tags) |
| Response user-props | `kubemq-executed=true/false` + optional `kubemq-error` |
| Response body | **empty** (commands carry no response body) |

## Gotchas

- **MQTT 5.0 only**: publishing to `commands/` over MQTT v3.1.1 succeeds at
  the wire level (PUBACK `0x00`) but the message is **silently dropped** and
  never reaches the gRPC side. The client receives no indication of the drop.
- **PUBACK is immediate**: the PUBACK confirms broker receipt, not command
  execution. Always implement your own response-wait timeout.
- **Own `$reply` namespace**: `ResponseTopic` must start with
  `$reply/<own-clientID>/`. Using another client's prefix returns PUBACK `0x83`.
- **MQTT clients cannot be responders**: subscribing to `commands/` as a
  potential responder returns SUBACK `0x83`. Only gRPC-side clients can respond.
- **`RpcMaxPending` exceeded** → PUBACK `0x97` (quota exceeded); back off and retry.
- **No body in response**: unlike Queries (which carry a body + `kubemq-metadata`),
  Commands return an **empty payload**; success/failure is signalled by
  `kubemq-executed` user-prop alone.

## Related Examples

- [rpc/query_round_trip](../query_round_trip/) — Queries (response carries body + metadata)
- [events/basic_pubsub](../../events/basic_pubsub/) — fire-and-forget Events (no round-trip)
- [protocol/mqtt_v311_vs_v5](../../protocol/mqtt_v311_vs_v5/) — v3.1.1 RPC silent-drop demo
