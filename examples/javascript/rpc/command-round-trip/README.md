# JavaScript/TypeScript — RPC: Command Round-Trip

Demonstrates MQTT 5.0 Command (fire-with-ack RPC) round-trip via KubeMQ. Unlike Queries, a Command response carries **no body** — the outcome is in user-properties `kubemq-executed` (`"true"`/`"false"`) and optional `kubemq-error`. **Requires a gRPC-side command responder.**

## Prerequisites

- Node.js 18+, `npm install` run from `examples/javascript/`
- KubeMQ server with MQTT connector enabled on `tcp://localhost:1883`
- A gRPC command responder running for channel `demo.cmd`:
  ```bash
  go run .work/test-harness/responder/ -channel demo.cmd -type command
  ```
  The responder must be started **before** running the example. MQTT clients cannot act as RPC responders — a SUBACK 0x83 is returned if they try.

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
npx tsx rpc/command-round-trip/index.ts
```

## Expected Output

```
[client] connecting to tcp://127.0.0.1:1883 as js-mqtt-cmd-<hex> (MQTT 5.0)
[client] connected (MQTT 5.0)
[client] Step 1: subscribing to reply topic $reply/js-mqtt-cmd-<hex>/inbox
[client]   subscribed (granted QoS 1)
[client] Step 3: publishing command to commands/demo/cmd
[client]   payload: {"action":"restart","target":"service-a","requestedBy":"mqtt-client"}
[client]   correlationData: <uuid>
[client]   PUBACK received immediately (execution outcome pending on reply topic)
[client] Step 3 done: waiting for command response...
[client] Step 4: command response received on $reply/js-mqtt-cmd-<hex>/inbox
[client]   payload (should be empty): (empty — expected for command responses)
[client]   kubemq-executed:           true
[client]   correlationData echoed:    <uuid> (match=true)
[client]   all user-properties:
[client]     kubemq-executed = true
[client] Command executed successfully.
[done] Disconnected cleanly.
```

## What's Happening

1. **Connect** — client connects with `protocolVersion: 5`. RPC is MQTT 5.0 only; a v3.1.1 publish to `commands/` is silently dropped (PUBACK 0x00 returned, message never delivered).
2. **Subscribe reply inbox first** — the client subscribes to its own `$reply/<clientID>/inbox` **before** publishing. Missing this step risks losing the response if it arrives before the subscription is established.
3. **Publish command** — publishes to `commands/demo/cmd` with `Properties.ResponseTopic` pointing at its reply inbox and a unique `Properties.CorrelationData` for request/response matching.
4. **PUBACK is immediate** — the broker ACKs the publish straight away, before the gRPC responder executes the command. The client must implement its own timeout (here: 30 s, matching the server default `CONNECTORSMQTT_RPC_TIMEOUT_SECONDS`).
5. **Command response** — the broker delivers a PUBLISH to the reply topic once the gRPC responder replies:
   - **Payload**: empty (commands carry no body — this is a key difference from queries).
   - **`kubemq-executed`**: `"true"` if the command succeeded; `"false"` otherwise.
   - **`kubemq-error`**: optional error string, present only when `executed=false`.
   - **`CorrelationData`**: echoed verbatim so concurrent in-flight requests can be matched.
6. **Clean disconnect** — `client.endAsync()` sends a DISCONNECT and closes the TCP connection.

## MQTT Specifics

| Property | Value |
|---|---|
| Protocol version | MQTT 5.0 (REQUIRED — v3.1.1 drops silently) |
| Subscribe (reply inbox) | `$reply/<clientID>/inbox` |
| Publish (command) | `commands/demo/cmd` |
| KubeMQ channel routed to | `demo.cmd` (topic `/` becomes `.`) |
| QoS | 1 (both subscribe and publish) |
| `Properties.ResponseTopic` | `$reply/<clientID>/inbox` (own namespace only) |
| `Properties.CorrelationData` | UUID bytes (echoed on response) |
| Response payload | empty (commands have NO body) |
| Response user-props | `kubemq-executed` (`"true"`/`"false"`) + optional `kubemq-error` |

## Gotchas

- **RPC is MQTT 5.0 only** (gotcha #4). A v3.1.1 client publishing to `commands/` receives PUBACK 0x00 but the message is silently dropped. No error is reported at the protocol level.
- **PUBACK is immediate** — it does NOT indicate the command was executed. The client must implement its own response-wait timeout. A 30 s default matches `CONNECTORSMQTT_RPC_TIMEOUT_SECONDS`.
- **Reply topic must be in own namespace** — `ResponseTopic` must start with `$reply/<own-clientID>/`. A foreign namespace causes PUBACK 0x83.
- **MQTT clients cannot be RPC responders** — subscribing to `commands/<ch>` or `queries/<ch>` from an MQTT client returns SUBACK 0x83. Responders must use the gRPC API (e.g. via kubemq-go).
- **Command vs. Query response** — command responses have no body; query responses include a body and `kubemq-metadata` user-property. Do not expect a body in the command response.

## Related Examples

- [rpc/query-round-trip](../query-round-trip/) — same RPC flow for Queries (response includes body + `kubemq-metadata`)
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — demonstrates v3.1.1 RPC silent-drop (gotcha #4)
