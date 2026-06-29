# java — RPC: Command Round-Trip

Send a Command RPC request over MQTT 5.0 and receive the execution result.

**Requires MQTT 5.0.** Publishing to `commands/` with a v3.1.1 client is
silently dropped — no error is returned, no response arrives.

**Requires a gRPC-side command responder.**
MQTT clients cannot be RPC responders (subscribing to `commands/` yields
SUBACK `0x83`).  The connector bridges the MQTT publish to a KubeMQ Commands
channel; a gRPC-side process must subscribe to that channel and send back a
reply.  The test stage starts the shared test-harness responder automatically.
For manual runs, start it as described below.

---

## Prerequisites

- Java 21+, Maven 3.8+
- KubeMQ server running with the MQTT connector enabled
- gRPC-side command responder running on `127.0.0.1:50000`

---

## How to Run

### 1 — Start the gRPC responder (separate terminal)

```bash
cd /path/to/kubemq-mqtt/.work/test-harness/responder
go run . -channel demo.cmd -type command -grpc 127.0.0.1:50000
```

The responder subscribes to KubeMQ channel `demo.cmd` over gRPC and replies
`kubemq-executed=true` to every command it receives.

### 2 — Run the example

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
cd examples/java/rpc/command-round-trip
mvn compile exec:java
```

---

## Expected Output

```
=== rpc/command-round-trip (MQTT 5.0) ===
Broker:        tcp://127.0.0.1:1883
ClientID:      java-mqtt-cmd-a1b2c3d4
Command topic: commands/demo/cmd
Reply topic:   $reply/java-mqtt-cmd-a1b2c3d4/inbox

Connecting to broker ...
Connected.

Subscribing to reply topic: $reply/java-mqtt-cmd-a1b2c3d4/inbox
Subscribed (QoS 1).

[callback] Connect complete (reconnect=false)
Publishing command to: commands/demo/cmd
  Payload:         execute-action
  ResponseTopic:   $reply/java-mqtt-cmd-a1b2c3d4/inbox
  CorrelationData: cmd-<uuid>

[callback] PUBACK received (immediate — before response)
Waiting up to 10s for command response ...

[callback] Response arrived on: $reply/java-mqtt-cmd-a1b2c3d4/inbox
[callback] Body: (empty — correct for command response)
[callback] User property: kubemq-executed = true
[callback] CorrelationData echoed: cmd-<uuid>

Command result:
  kubemq-executed: true
  CorrelationData match: true (sent=cmd-<uuid>, echoed=cmd-<uuid>)

SUCCESS: command executed; CorrelationData echoed correctly.

Disconnecting ...
Done.
```

---

## What's Happening (step-by-step)

1. **Create client** — `MqttAsyncClient` connected to the broker with
   `MqttConnectionOptions` (clean start, MQTT 5.0).

2. **Subscribe to reply inbox BEFORE publishing** — topic
   `$reply/<clientID>/inbox` at QoS 1.
   This must happen first: if the response arrives before the subscription
   is established the message is lost and the example times out.

3. **Publish command** to `commands/demo/cmd` with MQTT 5.0 publish properties:
   - `ResponseTopic` = `$reply/<clientID>/inbox`
   - `CorrelationData` = unique bytes for this request
   - Optional user properties (mapped to KubeMQ Tags)

4. **Receive immediate PUBACK** — the broker ACKs the publish as soon as it
   is received, not after the gRPC responder replies.  The `deliveryComplete`
   callback fires here.

5. **Wait for response** — the `messageArrived` callback receives the response
   delivered to `$reply/<clientID>/inbox`.
   - **No body** — command responses carry no payload.
   - **`kubemq-executed`** = `"true"` (success) or `"false"` (failure).
   - **`kubemq-error`** = error string, present only when `kubemq-executed=false`.
   - **`CorrelationData`** echoed verbatim.

6. **Self-imposed timeout** — if no response arrives within 10 seconds the
   example logs an error and exits non-zero.  There is no server-side
   notification for a missing responder; the client must time out itself.

7. **Disconnect** cleanly.

---

## MQTT Specifics

| Item | Value |
|------|-------|
| MQTT version | **5.0 (required)** — v3.1.1 publishes to `commands/` are silently dropped |
| Publish topic | `commands/demo/cmd` |
| KubeMQ channel | `demo.cmd` (`/` → `.` mapping) |
| Subscribe topic | `$reply/<clientID>/inbox` |
| QoS (publish) | 1 |
| QoS (subscribe) | 1 |
| `ResponseTopic` | `$reply/<clientID>/inbox` (must be own namespace) |
| `CorrelationData` | unique bytes per request; echoed on response |
| Response body | **empty** — commands carry no response body |
| `kubemq-executed` | `"true"` / `"false"` |
| `kubemq-error` | error string (only when `kubemq-executed=false`) |
| PUBACK timing | **immediate on receipt**, before the responder replies |

---

## Gotchas

- **Immediate PUBACK.**  The broker ACKs the publish before the gRPC responder
  has processed the command.  A missing `waitForCompletion` after `publish()`
  only confirms delivery to the broker, not execution.  Always implement a
  separate response-wait timeout.

- **Subscribe before publish.**  The reply inbox subscription must be in place
  before the command is published.  On a fast broker the response can arrive
  before the subscription if you reverse the order.

- **ResponseTopic namespace.**  The `ResponseTopic` must be in your own
  `$reply/<clientID>/...` namespace.  Using another client's namespace or a
  plain topic yields PUBACK `0x83`.

- **MQTT clients cannot be RPC responders.**  Subscribing to `commands/` or
  `queries/` from an MQTT client yields SUBACK `0x83`.  Responders must use
  the KubeMQ gRPC API (kubemq-go/v2 or another gRPC SDK).

- **v3.1.1 silent drop.**  Publishing to `commands/` with an MQTT 3.1.1 client
  succeeds at the PUBACK level but the message is silently dropped by the
  connector.  No response arrives.  Always use MQTT 5.0 for RPC.

- **Quota: `0x97`.**  If the broker's `RpcMaxPending` limit is reached the
  PUBACK carries reason code `0x97`.  Back off and retry.

---

## Related Examples

- [rpc/query-round-trip](../query-round-trip/) — query RPC; response includes a body and `kubemq-metadata`
- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget events, no reply
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — contrast v3.1.1 and v5.0 behaviour, including the v3.1.1 RPC drop
