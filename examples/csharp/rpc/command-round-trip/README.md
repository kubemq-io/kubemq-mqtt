# csharp — RPC: Command Round-Trip

Full request/confirm cycle for KubeMQ Commands over MQTT 5.0. The client publishes a command to `commands/demo/cmd`, waits on its own `$reply/<clientID>/inbox` topic, and checks the `kubemq-executed` user-property in the response. A gRPC-side responder on channel `demo.cmd` is required — MQTT clients cannot act as RPC responders.

## Prerequisites

- .NET 8+
- KubeMQ server with MQTT connector enabled (default `tcp://localhost:1883`)
- A running gRPC command responder on channel `demo.cmd`:
  ```bash
  # from .work/test-harness/responder/
  go run . -channel demo.cmd -type command
  ```

## How to Run

```bash
cd examples/csharp/rpc/command-round-trip
dotnet run
# or with a custom broker URL
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run
```

## Expected Output

```
[1] Connecting to 127.0.0.1:1883 as MQTT 5.0...
    Connected as 'csharp-cmd-<suffix>'
[2] Subscribing to reply topic '$reply/csharp-cmd-<suffix>/inbox'...
    Subscribed (result=GrantedQoS1)
[3] Publishing command to 'commands/demo/cmd'...
    PUBACK received immediately (before execution) — waiting for command response...
[4] Command response received:
    correlation-id   : <requestId>
    kubemq-executed  : True
    Result: SUCCESS
[5] Disconnecting.
    Done.
```

## What's Happening

1. **Connect with MQTT 5.0.** RPC is not available on MQTT 3.1.1 — a v3.1.1 publish to `commands/` is silently dropped by the connector (PUBACK still succeeds but the message is discarded).

2. **Subscribe to the reply topic first.** The client subscribes to `$reply/<clientID>/inbox` at QoS 1 _before_ publishing the command. This is a mochi-local topic: it is not routed to the KubeMQ array and does not require a KubeMQ channel. The `$reply/<clientID>/` namespace is enforced — subscribing to or publishing with a foreign `$reply/` namespace returns PUBACK/SUBACK `0x83`.

3. **Publish the command with `ResponseTopic` and `CorrelationData`.** The publish carries:
   - `Topic`: `commands/demo/cmd` → KubeMQ channel `demo.cmd` (topic `/` becomes `.`)
   - `Properties.ResponseTopic`: `$reply/<clientID>/inbox`
   - `Properties.CorrelationData`: a unique request ID (echoed on the response)

4. **PUBACK is returned immediately** upon receipt — _before_ the gRPC responder executes. This is intentional connector behaviour (not an error). The client must implement its own response-wait timeout; the example uses a 15-second `CancellationTokenSource`.

5. **The gRPC responder executes the command** and sends a `CommandReply` back to KubeMQ. The connector injects the reply onto the `ResponseTopic`.

6. **The command response has NO body.** The outcome is conveyed entirely via MQTT 5.0 User Properties:
   - `kubemq-executed`: `"true"` if the command succeeded, `"false"` otherwise.
   - `kubemq-error`: present and non-empty only on failure.
   - `CorrelationData`: the exact bytes from the original request, echoed back for request matching.

7. **Disconnect** cleanly after processing the response.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 (required — v3.1.1 publishes to `commands/` are silently dropped) |
| Command topic | `commands/demo/cmd` |
| Reply topic | `$reply/<clientID>/inbox` |
| QoS | 1 (AtLeastOnce) |
| `Properties.ResponseTopic` | `$reply/<clientID>/inbox` (own namespace required) |
| `Properties.CorrelationData` | unique request ID bytes (echoed on response) |
| Response body | **none** |
| Response user-props | `kubemq-executed` = `"true"` / `"false"` ; `kubemq-error` (on failure) |
| KubeMQ channel | `demo.cmd` (topic `/` → `.` in channel name) |
| PUBACK timing | **Immediate on receipt**, not after execution |

## Gotchas

**MQTT 5.0 is required for RPC.**
Publishing to `commands/demo/cmd` over a v3.1.1 connection is silently dropped. The wire PUBACK still returns `0x00` success — there is no error — but the command is never delivered. Always connect with `MqttProtocolVersion.V500`.

**PUBACK is NOT the command response.**
The connector sends PUBACK immediately when it receives the publish, before the gRPC responder has executed anything. Waiting for PUBACK is not sufficient; the client must wait on the `$reply/<clientID>/...` topic with its own timeout.

**Subscribe the reply topic before publishing.**
If the reply topic subscription arrives after the response is published, the response is lost. The 200 ms delay after subscribing gives the broker time to register the subscription before the command is dispatched.

**ResponseTopic must be in own `$reply/<clientID>/` namespace.**
Using any other `ResponseTopic` (including another client's `$reply/` namespace or a plain topic) returns PUBACK reason code `0x83` (implementation-specific error).

**MQTT clients cannot be RPC responders.**
Subscribing to `commands/<ch>` or `queries/<ch>` as a consumer over MQTT returns SUBACK `0x83`. All RPC responders must be gRPC-side (e.g. using `kubemq-go/v2`).

**`RpcMaxPending` quota.**
If more than `RpcMaxPending` (default 1024) requests are in-flight simultaneously, new publishes receive PUBACK `0x97` (quota exceeded).

## Related Examples

- [rpc/query-round-trip](../query-round-trip/) — query (request/response with body) RPC
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — demonstrates v3.1.1 RPC silent-drop
- [connectivity/websocket](../../connectivity/websocket/) — same pattern over WebSocket transport
