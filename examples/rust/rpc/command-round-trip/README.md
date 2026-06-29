# rust — RPC: Command Round-Trip

Demonstrates a complete MQTT 5.0 Command (fire-with-ack RPC) round-trip via the KubeMQ MQTT connector
using [`rumqttc`](https://crates.io/crates/rumqttc) (v5 API).
A command is published to `commands/demo/cmd`, which the broker routes to KubeMQ channel `demo.cmd`.
A gRPC-side responder receives and acknowledges the command; the broker delivers the acknowledgement
back to the MQTT client on its private `$reply/...` inbox.

**MQTT 5.0 only.** A v3.1.1 client receives a successful PUBACK (0x00) but the message is silently
dropped and never reaches any responder. See [gotcha #4](#gotcha-4--rpc-is-mqtt-50-only-immediate-puback-client-side-timeout-required) below.

---

## Prerequisites

- Rust 1.75+ (stable)
- KubeMQ broker running with the MQTT connector enabled.
  Default: `tcp://localhost:1883`; override with `KUBEMQ_MQTT_URL`.

- **gRPC RPC test-responder** running on channel `demo.cmd`.
  MQTT clients cannot act as RPC responders (SUBACK 0x83); the responder must connect via the gRPC API.
  Start it from the root of this repo:

  ```bash
  go run .work/test-harness/responder/ \
    -channel demo.cmd \
    -type command \
    -grpc 127.0.0.1:50000
  ```

  Override the gRPC address with `-grpc <host:port>` if your broker listens elsewhere.

---

## How to Run

```bash
# (optional) point at a non-default broker
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883

# start the gRPC responder first (separate terminal)
go run .work/test-harness/responder/ -channel demo.cmd -type command

# run the example from the rust/ root
cargo run -p command-round-trip
```

---

## Expected Output

> Regenerated from a live run in the LiveTest stage. Topic strings below are canonical.

```
Step 1: Connecting to broker 127.0.0.1:1883 as mqtt-rust-cmd-a1b2c3d4
Step 2: Subscribing to reply topic "$reply/mqtt-rust-cmd-a1b2c3d4/inbox"
Step 3: Publishing command to "commands/demo/cmd"
  payload: {"action":"restart","target":"service-a","requestedBy":"mqtt-rust-client"}
  correlationData: <uuid>
  PUBACK received immediately (execution outcome pending on $reply/...)
Step 4: Waiting for command response on "$reply/mqtt-rust-cmd-a1b2c3d4/inbox" ...
  all user-properties:
    kubemq-executed = true
Step 5: Command response received:
  payload (should be empty): ""
  kubemq-executed:           true
  correlationData echoed:    <uuid> (match=true)
Command executed successfully.
COMMAND_RESPONSE_RECEIVED
```

If the responder is not running and the `RpcTimeoutSeconds` elapses the server audits an
`rpc.timeout` event and no response is delivered — the example exits with a timeout error after 30 s.

---

## What's Happening

1. **Connect** — the client opens a TCP connection to the MQTT broker and sends CONNECT with a
   unique `clientID`. A fresh session is used each run (no persistent session state).

2. **Subscribe to reply inbox** — the client subscribes to `$reply/<clientID>/inbox` at QoS 1
   **before** publishing. This is the mochi-local reply namespace: the broker enforces that a client
   may only subscribe to `$reply/<own-clientID>/...`. Missing this step or subscribing after the
   publish risks losing the response.

3. **Publish the command** — the client publishes to `commands/demo/cmd` (QoS 1, MQTT 5.0) with:
   - `ResponseTopic` = the reply topic from step 2
   - `CorrelationData` = a UUID (opaque bytes the broker echoes back)
   - `Payload` = the command body (arbitrary bytes; KubeMQ forwards it to the responder)

4. **Immediate PUBACK** — the broker acknowledges the publish (PUBACK 0x00) as soon as it receives
   the message, **before** the gRPC responder has processed it. The PUBACK only confirms delivery to
   the broker, not execution of the command. The client must wait separately for the response.

5. **gRPC responder executes the command** — the broker fans the message out to the KubeMQ Commands
   channel `demo.cmd`. The gRPC-side responder (kubemq-go) receives a `CommandReceive`, processes it,
   and calls `SendCommandResponse`. The reply carries `executed=true` and no error.

6. **Response delivered to $reply inbox** — the broker delivers the response as a PUBLISH to the
   client's `$reply/<clientID>/inbox` topic. The response has:
   - **No payload** (empty body — this is the defining difference from a Query response)
   - `kubemq-executed` user-property: `"true"` if the command succeeded, `"false"` otherwise
   - `kubemq-error` user-property: present only when `kubemq-executed` is `"false"`
   - `CorrelationData`: the same bytes that were sent in step 3

7. **Client-side timeout** — a `tokio::time::timeout(30s)` provides the self-imposed deadline.
   The client uses a `oneshot` channel populated by the rumqttc event loop task.

8. **Graceful shutdown** — the event loop task exits after the response is delivered.

---

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | **5.0 only** (level 5 in CONNECT) |
| Publish topic | `commands/demo/cmd` → KubeMQ channel `demo.cmd` |
| Subscribe topic | `$reply/<clientID>/inbox` (mochi-local, never routed to KubeMQ) |
| QoS | 1 (both publish and subscribe) |
| `ResponseTopic` | `$reply/<clientID>/inbox` — must be own namespace (else PUBACK 0x83) |
| `CorrelationData` | UUID string bytes — echoed verbatim on the response |
| Response payload | **empty** (command responses carry NO body) |
| Response user-properties | `kubemq-executed` = `"true"` / `"false"`, `kubemq-error` (on failure) |
| PUBACK timing | Immediate on receipt — **not** after command execution |
| Retain | Not used; retain is forced off (`RetainAvailable=0`) — a retained publish is silently dropped |

### Gotcha #4 — RPC is MQTT 5.0 only (immediate PUBACK, client-side timeout required)

Publishing to `commands/` or `queries/` from a v3.1.1 client yields a successful PUBACK (0x00) but
the message is **silently dropped** and `publish.error` is audited server-side. There is no wire
indication of the drop. Always use MQTT 5.0 for RPC patterns.

Additionally, the broker sends PUBACK **before** the responder executes the command. Do not treat
PUBACK as confirmation of execution. The client is responsible for implementing a response-wait
timeout (this example uses `tokio::time::timeout`).

### Gotcha — MQTT clients cannot be RPC responders

A subscribe to `commands/<channel>` or `queries/<channel>` from an MQTT client returns **SUBACK 0x83**
(implementation-specific error). RPC responders must connect via the KubeMQ gRPC API (e.g. kubemq-go).

### Gotcha — Command response has no body

Unlike a Query response (which carries a payload), a Command response has an **empty payload**.
The execution outcome is conveyed entirely through the `kubemq-executed` and `kubemq-error`
User Properties. Code that reads the payload for a command result will always see an empty slice.

### Reason codes to handle

| Code | Meaning |
|---|---|
| `0x00` | Success |
| `0x83` | PUBACK: `ResponseTopic` not in own `$reply/<clientID>/...` namespace |
| `0x83` | SUBACK: subscribe to `commands/` or `queries/` (MQTT cannot be responder) |
| `0x97` | PUBACK: `RpcMaxPending` quota exceeded (too many concurrent in-flight RPCs) |
| `0x80` | PUBACK: broker not ready (startup gate) |

---

## Related Examples

- [`rpc/query-round-trip`](../query-round-trip/) — same RPC flow but for Queries; response carries a payload body and `kubemq-metadata` + tags as user-properties
- [`protocol/mqtt-v311-vs-v5`](../../protocol/mqtt-v311-vs-v5/) — demonstrates the v3.1.1 silent-drop behavior for RPC publishes
- [`connectivity/auth-jwt`](../../connectivity/auth-jwt/) — how to authenticate with a JWT in the MQTT Password field
