# Commands Pattern

## Overview

Commands implement **RPC with execution acknowledgement**. An MQTT client sends a command
to a KubeMQ channel; a gRPC responder processes it and returns a pass/fail acknowledgement.
The MQTT client receives the result on its private reply topic.

| Property | Value |
|----------|-------|
| KubeMQ pattern | Commands |
| MQTT topic prefix (send) | `commands/<channel-path>` |
| MQTT reply topic (receive) | `$reply/<own-clientID>/<suffix>` |
| MQTT version required | **5.0 only** — v3.1.1 publishes are silently dropped |
| Minimum QoS | 1 (recommended) |
| Responder | gRPC-side only — MQTT clients cannot be responders |
| Response carries body | No — execution status only |

> **Gotcha #4 — Commands require MQTT 5.0. A v3.1.1 publish to `commands/` is silently dropped.**
> The connector checks the MQTT protocol version before processing a commands/ publish. If the
> connection is MQTT 3.1.1 (protocol level 4), the publish is discarded and `publish.error` is
> audited server-side. The wire-level PUBACK still returns `0x00` (success) so the v3.1.1
> client does not know the message was dropped.
>
> Verified by integration test #12 `RPC311Dropped`.

> **Gotcha #4 (continued) — PUBACK is sent immediately, before the response arrives.**
> The server returns PUBACK as soon as it receives the PUBLISH, before the RPC round-trip
> completes. The client MUST implement its own response-wait timeout on the `$reply` channel.

## Flow Diagram

```
MQTT client (v5)                      KubeMQ connector           gRPC responder
      |                                      |                          |
      | SUBSCRIBE $reply/<clientID>/inbox    |                          |
      |------------------------------------->|                          |
      | SUBACK 0x01                          |                          |
      |<-------------------------------------|                          |
      |                                      |                          |
      | PUBLISH commands/<ch>                |                          |
      |   Properties.ResponseTopic = $reply/<clientID>/inbox           |
      |   Properties.CorrelationData = <bytes>                         |
      |------------------------------------->|                          |
      | PUBACK 0x00  ← immediate (NOT after response)                  |
      |<-------------------------------------|                          |
      |                                      | SendCommandRequest       |
      |                                      |------------------------->|
      |                                      |        CommandResponse   |
      |                                      |<-------------------------|
      |                                      |                          |
      | PUBLISH $reply/<clientID>/inbox      |                          |
      |   Properties.CorrelationData = <same bytes>                    |
      |   Properties.User:                   |                          |
      |     kubemq-executed = "true"/"false" |                          |
      |     kubemq-error    = "<msg>" (on failure only)                 |
      |<-------------------------------------|                          |
```

## Prerequisites

1. A **gRPC responder** must be running, subscribed to the same KubeMQ Commands channel.
   MQTT clients cannot be responders (subscribe to `commands/` returns SUBACK `0x83`).
   See [.work/test-harness/responder/](../../.work/test-harness/responder/) for the
   reference Go responder used in the examples.
2. MQTT connection must be **MQTT 5.0** (`protocolVersion=5`).

## Step 1 — Subscribe to your reply topic

The reply topic must be in your own `$reply/<own-clientID>/` namespace. Subscribing to another
client's reply namespace returns SUBACK `0x83`.

```
SUBSCRIBE  filter=$reply/<clientID>/inbox  QoS=1
SUBACK     reason=0x01  (granted QoS 1)
```

The `$reply/` namespace is mochi-local: it is never routed to the KubeMQ array and does not
appear in KubeMQ channel lists.

## Step 2 — Send the command

```
PUBLISH
  topic    = commands/<channel-path>
  QoS      = 1
  payload  = <request bytes>
  Properties:
    ResponseTopic   = $reply/<own-clientID>/inbox
    CorrelationData = <opaque bytes — echoed on response>
```

Topic example: `commands/device/reboot` → KubeMQ channel `device.reboot`

**PUBACK is returned immediately** (before the response is available). Start your response
timeout immediately after PUBACK.

**Send failure codes:**

| PUBACK reason | Cause |
|---------------|-------|
| `0x83` | Missing or foreign `ResponseTopic` (not in own `$reply/<clientID>/` namespace) |
| `0x97` | `RpcMaxPending` quota reached (default 1024 concurrent pending RPCs) |
| `0x00` (but dropped) | v3.1.1 publish — silently discarded, `publish.error` audited |

## Step 3 — Receive the response

The response arrives as an MQTT PUBLISH on your reply topic:

```
← PUBLISH
    topic    = $reply/<clientID>/inbox
    QoS      = 1
    payload  = (empty — commands carry no body)
    Properties:
      CorrelationData = <same bytes you sent>
      User:
        kubemq-executed = "true"   (or "false" on failure)
        kubemq-error    = "<message>"  (only present when executed=false)
```

**Always correlate responses to requests via `CorrelationData`** if you have more than one
pending command. The connector echoes the exact bytes you sent; use a UUID or sequence number.

**No response within `RpcTimeoutSeconds` (default 30 s):** no PUBLISH is delivered on the
reply topic; the server audits `rpc.timeout`. Your client-side timeout must fire.
Verified by integration test #11 `RPCTimeout`.

## Parsing the Response

| User property | Type | Meaning |
|---------------|------|---------|
| `kubemq-executed` | `"true"` or `"false"` | Whether the responder processed the command successfully |
| `kubemq-error` | string (optional) | Error message when `kubemq-executed=false` |

The response payload (body) is always empty for commands. Do not rely on payload content.

The response tags are populated as follows:

```go
tags["kubemq-executed"] = strconv.FormatBool(resp.Executed)
if resp.Error != "" {
    tags["kubemq-error"] = resp.Error
}
```

## MQTT Clients Cannot Be Responders

Subscribing to `commands/` as a responder is not supported:

```
SUBSCRIBE  filter=commands/device/reboot  QoS=1
SUBACK     reason=0x83   (impl-specific error — MQTT clients cannot be RPC responders)
```

Responders must use the KubeMQ gRPC API (e.g. kubemq-go `SubscribeToCommands` +
`SendCommandResponse`). See the [test responder](../../.work/test-harness/responder/) for a
working example.

## Topic Grammar

```
commands/device/reboot   →   channel  device.reboot
commands/svc/restart     →   channel  svc.restart
```

> **Gotcha #5 — literal `.` conflates with `/`.**
> `commands/a.b` and `commands/a/b` both map to channel `a.b`.

## Timeout and Quota

| Setting | Env var | Default |
|---------|---------|---------|
| RPC response timeout | `CONNECTORSMQTT_RPC_TIMEOUT_SECONDS` | 30 s |
| Max concurrent pending RPCs | `CONNECTORSMQTT_RPC_MAX_PENDING` | 1024 |

When `RpcMaxPending` is reached, the next PUBLISH to `commands/` returns PUBACK `0x97`.

## Examples

| Language | command-round-trip |
|----------|--------------------|
| Go | [examples/go/rpc/command-round-trip/](../../examples/go/rpc/command-round-trip/) |
| Python | [examples/python/rpc/command_round_trip/](../../examples/python/rpc/command_round_trip/) |
| JavaScript | [examples/javascript/rpc/command-round-trip/](../../examples/javascript/rpc/command-round-trip/) |
| Java | [examples/java/rpc/command-round-trip/](../../examples/java/rpc/command-round-trip/) |
| C# | [examples/csharp/rpc/command-round-trip/](../../examples/csharp/rpc/command-round-trip/) |
| Ruby | N/A (v3.1.1 only — RPC requires MQTT 5.0) |
| Rust | [examples/rust/rpc/command-round-trip/](../../examples/rust/rpc/command-round-trip/) |

Ruby note: the `mqtt` gem is MQTT 3.1.1 only. RPC (Commands and Queries) requires MQTT 5.0
`ResponseTopic` and `CorrelationData` properties, which v3.1.1 does not support. Even if a
v3.1.1 client publishes to `commands/`, the connector silently drops the message (gotcha #4).

## Related Docs

- [Queries pattern](queries.md) — RPC with data response (body returned)
- [Topic Grammar reference](../reference/topic-grammar.md)
- [Reason Codes reference](../reference/reason-codes.md)
- [Capabilities reference](../reference/capabilities.md) — `RpcMaxPending`, `RpcTimeoutSeconds`
- [Guide: Protocol Versions](../guides/protocol-versions.md)
