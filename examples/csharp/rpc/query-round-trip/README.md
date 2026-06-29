# csharp — RPC: Query Round-Trip

Full request/response cycle for KubeMQ Queries over MQTT 5.0.

## Prerequisites

- .NET 8+
- KubeMQ server with MQTT connector enabled (default `tcp://localhost:1883`)
- A running gRPC query responder on channel `demo.rpc` (see below)

## How to Run

Start the gRPC test responder first (required — MQTT clients cannot be RPC responders):

```bash
cd .work/test-harness/responder
go run . -channel demo.rpc -type query
```

Then in a separate terminal:

```bash
cd examples/csharp/rpc/query-round-trip
dotnet run
```

Override the broker URL with the environment variable:

```bash
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run
```

## Expected Output

```
Connecting to tcp://127.0.0.1:1883 as 'csharp-mqtt-rpc-q-...'...
Connected
Subscribed to reply inbox '$reply/csharp-mqtt-rpc-q-.../inbox' (QoS GrantedQoS1)
Publishing query to 'queries/demo/rpc'...
PUBACK received (immediate, before response) — waiting for query response...
Query response received:
  correlation-id   : <requestId>
  body             : pong:{"query":"hello","requestId":"..."}
  kubemq-metadata  : responder/query
  tags             : responder=mqtt-test, rpc-type=query
Disconnected. Done.
```

## What's Happening

1. **Connect** — client connects with MQTT 5.0 (`WithProtocolVersion(V500)`). MQTT 5.0 is required; publishing to `queries/` with v3.1.1 is silently dropped by the connector.
2. **Subscribe reply inbox** — the client subscribes to `$reply/<clientID>/inbox` at QoS 1 **before** publishing. Using a `$reply/<clientID>/` prefix is mandatory; a foreign namespace returns SUBACK `0x83`.
3. **Publish query** — the client publishes to `queries/demo/rpc` with two MQTT 5.0 properties:
   - `ResponseTopic` = `$reply/<clientID>/inbox`
   - `CorrelationData` = the request ID bytes
4. **Immediate PUBACK** — the broker sends PUBACK back immediately on receipt, **not** after the gRPC responder answers. The client therefore implements its own 15-second response-wait timeout.
5. **gRPC responder** — a `kubemq-go/v2` process listens on KubeMQ channel `demo.rpc` (type query). MQTT clients cannot be RPC responders (an RPC subscribe over MQTT returns SUBACK `0x83`).
6. **Receive response** — the connector delivers the responder's answer to `$reply/<clientID>/inbox`. The response carries:
   - **body** — the query reply body (e.g. `pong:<echo of request>`)
   - **user-property `kubemq-metadata`** — set by the responder
   - **additional user-properties** — mapped from KubeMQ tags (e.g. `responder`, `rpc-type`)
   - **CorrelationData** — echoed from the original request for matching

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 (required) |
| Subscribe topic | `$reply/<clientID>/inbox` |
| Publish topic | `queries/demo/rpc` |
| KubeMQ channel | `demo.rpc` (topic `/` becomes `.`) |
| QoS | 1 (AtLeastOnce) for both subscribe and publish |
| `ResponseTopic` | `$reply/<clientID>/inbox` (own namespace) |
| `CorrelationData` | request UUID bytes, echoed in response |
| Response body | query reply body |
| Response user-props | `kubemq-metadata` + tags from responder |

## Gotchas

- **PUBACK is immediate, not on-response.** The client must manage its own timeout (see `CancellationTokenSource`). If the responder is not running, the publish succeeds but no response ever arrives.
- **ResponseTopic must be in your own `$reply/<clientID>/` namespace.** Using another client's `$reply/` prefix returns PUBACK `0x83` (implementation specific error).
- **MQTT 3.1.1 silently drops RPC publishes.** There is no error; the message is simply not processed.
- **RpcMaxPending exceeded** returns PUBACK `0x97` (quota exceeded). Reduce in-flight requests or increase `RpcMaxPending` on the server.

## Related Examples

- [rpc/command-round-trip](../command-round-trip/) — command (fire-and-confirm) RPC; response has no body, only `kubemq-executed` user-prop
