# Queries Pattern

## Overview

Queries implement **RPC with data response**. An MQTT client sends a query request to a
KubeMQ channel; a gRPC responder processes it and returns a payload (body + metadata).
The MQTT client receives the response on its private reply topic.

Queries and Commands share the same MQTT flow; the key difference is the response shape:
commands return an execution status with no body; queries return an actual data payload.

| Property | Value |
|----------|-------|
| KubeMQ pattern | Queries |
| MQTT topic prefix (send) | `queries/<channel-path>` |
| MQTT reply topic (receive) | `$reply/<own-clientID>/<suffix>` |
| MQTT version required | **5.0 only** — v3.1.1 publishes are silently dropped |
| Minimum QoS | 1 (recommended) |
| Responder | gRPC-side only — MQTT clients cannot be responders |
| Response carries body | Yes — arbitrary bytes in payload |

> **Gotcha #4 — Queries require MQTT 5.0. A v3.1.1 publish to `queries/` is silently dropped.**
> The connector checks the MQTT protocol version before processing. If the connection is
> MQTT 3.1.1, the publish is discarded and `publish.error` is audited. The PUBACK still
> returns `0x00`. Verified by integration test #12 `RPC311Dropped`.

> **Gotcha #4 (continued) — PUBACK is immediate; the response arrives later.**
> The server returns PUBACK as soon as the PUBLISH is received, before the RPC round-trip
> completes. The client MUST implement its own response-wait timeout.

## Flow Diagram

```
MQTT client (v5)                      KubeMQ connector           gRPC responder
      |                                      |                          |
      | SUBSCRIBE $reply/<clientID>/inbox    |                          |
      |------------------------------------->|                          |
      | SUBACK 0x01                          |                          |
      |<-------------------------------------|                          |
      |                                      |                          |
      | PUBLISH queries/<ch>                 |                          |
      |   Properties.ResponseTopic = $reply/<clientID>/inbox           |
      |   Properties.CorrelationData = <bytes>                         |
      |------------------------------------->|                          |
      | PUBACK 0x00  ← immediate             |                          |
      |<-------------------------------------|                          |
      |                                      | SendQueryRequest         |
      |                                      |------------------------->|
      |                                      |          QueryResponse   |
      |                                      |<-------------------------|
      |                                      |                          |
      | PUBLISH $reply/<clientID>/inbox      |                          |
      |   payload = <response body bytes>    |                          |
      |   Properties.CorrelationData = <same bytes>                    |
      |   Properties.User:                   |                          |
      |     kubemq-metadata = "<string>"  (if responder set metadata)  |
      |     <additional tags from responder>                           |
      |<-------------------------------------|                          |
```

## Difference from Commands

| Aspect | Commands | Queries |
|--------|----------|---------|
| MQTT prefix | `commands/` | `queries/` |
| Response body | Always empty | Present — responder's data |
| Response user-props | `kubemq-executed`, `kubemq-error` | `kubemq-metadata` + responder tags |
| Use case | "Execute this action; did it work?" | "Give me data" |

## Prerequisites

1. A **gRPC responder** must be running, subscribed to the same KubeMQ Queries channel.
   MQTT clients cannot be responders (subscribe to `queries/` returns SUBACK `0x83`).
   See [.work/test-harness/responder/](../../.work/test-harness/responder/) for the
   reference Go responder.
2. MQTT connection must be **MQTT 5.0** (`protocolVersion=5`).

## Step 1 — Subscribe to your reply topic

```
SUBSCRIBE  filter=$reply/<clientID>/inbox  QoS=1
SUBACK     reason=0x01  (granted QoS 1)
```

Subscribe before sending any queries. The `$reply/` namespace is mochi-local and never
routed to the KubeMQ array. Use only your own `$reply/<own-clientID>/` namespace —
a foreign namespace returns SUBACK `0x83`.

## Step 2 — Send the query

```
PUBLISH
  topic    = queries/<channel-path>
  QoS      = 1
  payload  = <request bytes>
  Properties:
    ResponseTopic   = $reply/<own-clientID>/inbox
    CorrelationData = <opaque bytes — echoed on response>
```

Topic example: `queries/inventory/check` → KubeMQ channel `inventory.check`.

**PUBACK is returned immediately.** Start your response timeout immediately after receiving
the PUBACK — the response has NOT yet arrived.

**Send failure codes:**

| PUBACK reason | Cause |
|---------------|-------|
| `0x83` | Missing `ResponseTopic` or `ResponseTopic` not in own `$reply/<clientID>/` namespace |
| `0x97` | `RpcMaxPending` quota reached (default 1024) |
| `0x00` (but dropped) | v3.1.1 connection — silently discarded |

## Step 3 — Receive the response

```
← PUBLISH
    topic    = $reply/<clientID>/inbox
    QoS      = 1
    payload  = <response body — arbitrary bytes from responder>
    Properties:
      CorrelationData = <same bytes you sent>
      User:
        kubemq-metadata = "<string>"   (if the responder set metadata; may be absent)
        <key> = <value>                (any additional tags the responder attached)
```

**Integration test #10 (`RPCQueryRoundTrip`) verifies the canonical flow:**
- Subscribe `$reply/<id>/inbox`
- Publish `queries/it/rpc` with `ResponseTopic=$reply/<id>/inbox`, `CorrelationData=corr-123`
- gRPC responder replies with body `pong:ping`
- MQTT client receives `pong:ping` on `$reply/<id>/inbox` with `CorrelationData=corr-123`

## Parsing the Response

| Field | Type | Meaning |
|-------|------|---------|
| Payload | bytes | The response body returned by the gRPC responder |
| `kubemq-metadata` user-prop | string (optional) | Metadata string set by the responder |
| Other user-props | string | Any KubeMQ `Tags` the responder attached |
| `CorrelationData` | bytes | Echoed from the request — use to match response to request |

The query response is mapped as follows:

```go
if reqType == pb.Request_Query {
    body = resp.Body
    if resp.Metadata != "" {
        tags["kubemq-metadata"] = resp.Metadata
    }
    for k, v := range resp.Tags {
        tags[k] = v
    }
}
```

## MQTT Clients Cannot Be Responders

```
SUBSCRIBE  filter=queries/inventory/check  QoS=1
SUBACK     reason=0x83   (MQTT clients cannot subscribe as RPC responders)
```

Responders must use the KubeMQ gRPC API (e.g. kubemq-go `SubscribeToQueries` +
`SendQueryResponse`).

## Timeout and Quota

| Setting | Env var | Default |
|---------|---------|---------|
| RPC response timeout | `CONNECTORSMQTT_RPC_TIMEOUT_SECONDS` | 30 s |
| Max concurrent pending RPCs | `CONNECTORSMQTT_RPC_MAX_PENDING` | 1024 |

No response within `RpcTimeoutSeconds`: no PUBLISH on the reply topic; `rpc.timeout` audited.
Your client-side timeout must handle this case. Verified by integration test #11 `RPCTimeout`.

When `RpcMaxPending` is reached, PUBACK `0x97` is returned.

## Topic Grammar

```
queries/inventory/check   →   channel  inventory.check
queries/svc/status        →   channel  svc.status
```

> **Gotcha #5 — literal `.` conflates with `/`.**
> `queries/a.b` and `queries/a/b` both map to channel `a.b`.

## Multiple Concurrent Queries

Always set a unique `CorrelationData` per request (e.g., a UUID). The connector echoes the
exact bytes on the response, enabling the client to match responses to requests when multiple
queries are in flight simultaneously.

## Examples

| Language | query-round-trip |
|----------|-----------------|
| Go | [examples/go/rpc/query-round-trip/](../../examples/go/rpc/query-round-trip/) |
| Python | [examples/python/rpc/query_round_trip/](../../examples/python/rpc/query_round_trip/) |
| JavaScript | [examples/javascript/rpc/query-round-trip/](../../examples/javascript/rpc/query-round-trip/) |
| Java | [examples/java/rpc/query-round-trip/](../../examples/java/rpc/query-round-trip/) |
| C# | [examples/csharp/rpc/query-round-trip/](../../examples/csharp/rpc/query-round-trip/) |
| Ruby | N/A (v3.1.1 only — RPC requires MQTT 5.0) |
| Rust | [examples/rust/rpc/query-round-trip/](../../examples/rust/rpc/query-round-trip/) |

Ruby note: the `mqtt` gem is MQTT 3.1.1 only. RPC requires MQTT 5.0 `ResponseTopic` and
`CorrelationData` properties. Even if a v3.1.1 client publishes to `queries/`, the connector
silently drops the message (gotcha #4).

## Related Docs

- [Commands pattern](commands.md) — RPC with execution acknowledgement (no body)
- [Topic Grammar reference](../reference/topic-grammar.md)
- [Reason Codes reference](../reference/reason-codes.md)
- [Capabilities reference](../reference/capabilities.md) — `RpcMaxPending`, `RpcTimeoutSeconds`
- [Guide: Protocol Versions](../guides/protocol-versions.md)
