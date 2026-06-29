# JavaScript/TypeScript — RPC: Query Round-Trip

Demonstrates RPC Queries over MQTT 5.0 using `ResponseTopic` and `CorrelationData`. The query response includes a **body** and user-properties (`kubemq-metadata` + tags). **Requires a gRPC-side responder** — MQTT clients cannot serve as RPC responders.

## Prerequisites

- Node.js 18+, `npm install` run in `examples/javascript/`
- KubeMQ server with MQTT connector enabled (`tcp://127.0.0.1:1883`)
- A gRPC query responder running for channel `demo.rpc`:
  ```bash
  cd .work/test-harness/responder
  go run . -channel demo.rpc -type query
  ```

## How to Run

```bash
cd examples/javascript
npm install

# In a separate terminal, start the gRPC query responder:
# cd .work/test-harness/responder && go run . -channel demo.rpc -type query

export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
npx tsx rpc/query-round-trip/index.ts
```

## Expected Output

```
[client] connected (MQTT 5.0)
[client] subscribed to reply topic: $reply/js-mqtt-query-client/inbox
[client] query published to queries/demo/rpc (PUBACK received immediately — waiting for response)
[client] query response received on $reply/js-mqtt-query-client/inbox
[client]   CorrelationData echoed: query-corr-1
[client]   body: pong:{"request":"hello"}
[client]   user-props: {"kubemq-metadata":"responder/query","responder":"mqtt-test","rpc-type":"query"}
[done]
```

> The three response user-property keys (`kubemq-metadata`, `responder`, `rpc-type`) are
> always present, but their **serialization order varies run-to-run** — mqtt.js builds the
> user-properties map in wire-arrival order, so the JSON above may show the same keys in a
> different order on each run.

## What's Happening

1. **Connect with MQTT 5.0** (`protocolVersion: 5`). RPC is MQTT 5.0-only — a v3.1.1 publish to `queries/<ch>` is silently dropped (PUBACK still succeeds, but no response arrives).
2. **Subscribe to own reply inbox first** (`$reply/js-mqtt-query-client/inbox`, QoS 1). The reply topic must be in the client's own `$reply/<clientID>/...` namespace — using a foreign namespace returns PUBACK `0x83`.
3. **Arm the response listener before publishing** to eliminate the race window between PUBACK and the response arriving.
4. **Publish to `queries/demo/rpc`** with `Properties.ResponseTopic` and `Properties.CorrelationData`. Optional `UserProperties` become KubeMQ request tags.
5. **PUBACK arrives immediately** on receipt — the broker does NOT wait for the gRPC responder before acknowledging. The client must implement its own response-wait timeout (here: 30 s, matching the connector's `RpcTimeoutSeconds` default).
6. **Query response arrives on the reply topic** with `CorrelationData` echoed, a non-empty body, and `kubemq-metadata` user-property.

## MQTT Specifics

| Property | Value |
|---|---|
| Protocol version | MQTT 5.0 (REQUIRED) |
| Subscribe (reply inbox) | `$reply/js-mqtt-query-client/inbox` |
| Publish (query) | `queries/demo/rpc` |
| KubeMQ channel | `demo.rpc` |
| QoS | 1 (both subscribe and publish) |
| `Properties.ResponseTopic` | `$reply/js-mqtt-query-client/inbox` (own namespace) |
| `Properties.CorrelationData` | `query-corr-1` (echoed on response) |
| `Properties.UserProperties` | `source=js-example` (optional; become KubeMQ request tags) |
| Response body | Present — e.g. `pong:{"request":"hello"}` |
| Response user-props | `kubemq-metadata` + tags from the gRPC responder |

## Gotchas

- **Immediate PUBACK**: the broker PUBACKs the query publish instantly (before the gRPC responder replies). The client must implement its own timeout; do not rely on PUBACK as a signal that a response is coming.
- **MQTT 5.0 only**: a v3.1.1 publish to `queries/<ch>` is silently dropped — PUBACK succeeds, but no response is delivered and a `publish.error` is audited server-side.
- **Own `$reply` namespace**: the `ResponseTopic` must be `$reply/<own-clientID>/<suffix>`. A foreign namespace (e.g. another client's `clientID`) returns PUBACK `0x83`.
- **MQTT clients cannot be responders**: subscribing to `queries/<ch>` as a responder returns SUBACK `0x83`. Responders must be gRPC-side (e.g. `kubemq-go`).
- **`RpcMaxPending` quota**: if the broker has too many in-flight RPC requests, publish returns PUBACK `0x97`. Handle it with a backoff retry.

## Related Examples

- [rpc/command-round-trip](../command-round-trip/) — same RPC flow for Commands (response has NO body; outcome is in `kubemq-executed` user-prop)
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — demonstrates v3.1.1 RPC silent drop
