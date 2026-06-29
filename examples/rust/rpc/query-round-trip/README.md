# rust — RPC: Query Round-Trip

Demonstrates an MQTT 5.0 query RPC round-trip using `rumqttc`. The client sends a
query and receives a response body plus metadata via KubeMQ's Queries pattern.

## Prerequisites

- Rust 1.75+ (stable)
- KubeMQ server with MQTT connector enabled on port 1883 (TCP) — MQTT **5.0 required**
- A gRPC query responder listening on KubeMQ channel `demo.rpc`; the test harness
  provides this at `.work/test-harness/responder/`:

  ```bash
  go run .work/test-harness/responder/main.go -channel demo.rpc -type query
  ```

## How to Run

```bash
# From the workspace root (examples/rust/)
cd examples/rust
KUBEMQ_MQTT_URL=tcp://localhost:1883 cargo run -p query-round-trip
```

The `KUBEMQ_MQTT_URL` variable accepts `tcp://`, `tls://`, or `ws://` URLs;
it defaults to `tcp://localhost:1883`.

## Expected Output

> Regenerated from a live run in the LiveTest stage. Topic strings below are canonical.

```
=== MQTT 5.0 Query Round-Trip ===
[1] Broker URL  : tcp://localhost:1883
[1] Client ID   : mqtt-rust-query-<random>
[1] Query topic : queries/demo/rpc -> KubeMQ channel demo.rpc
[1] Reply topic : $reply/mqtt-rust-query-<random>/inbox
[1] Correlation : <uuid>
[1] Connected to localhost:1883 (MQTT 5.0)
[2] Subscribing to reply topic "$reply/mqtt-rust-query-<random>/inbox" (QoS 1) ...
    Subscribe sent; waiting for SUBACK ...
    SUBACK[0] = 0x01 (granted QoS AtLeastOnce)
[3] Publishing query to "queries/demo/rpc" ...
    payload         : {"action":"echo","data":"hello from rust-mqtt","ts":...}
    ResponseTopic   : $reply/mqtt-rust-query-<random>/inbox
    CorrelationData : <uuid>
    Publish sent (awaiting PUBACK; PUBACK is IMMEDIATE — not a sign the response is ready)
    Waiting for gRPC responder on channel demo.rpc to reply ...
    PUBACK = 0x00 (success — PUBACK is IMMEDIATE, response arrives separately)

[4] Response received on "$reply/mqtt-rust-query-<random>/inbox"
    body            : pong:{"action":"echo","data":"hello from rust-mqtt","ts":...}
    correlation     : <uuid>
    correlation match : true
    user-properties (kubemq-metadata / tags):
      kubemq-metadata = responder/query
      responder = mqtt-test
      rpc-type = query

QUERY_RESPONSE_RECEIVED
[5] Disconnecting ...
[5] Done.
=== Query round-trip complete ===
```

## What's Happening (Step-by-Step)

1. **Connect** — a unique client ID is generated; `$reply/<clientID>/inbox` is
   the private reply namespace for this client.
2. **Subscribe** to `$reply/<clientID>/inbox` at QoS 1 **before** publishing.
   This ensures the reply inbox is registered on the broker before the query is sent.
   A SUBACK with reason `ImplementationSpecific (0x83)` means the topic was not in
   the own namespace; `WildcardSubscriptionsNotSupported (0xA2)` means a wildcard
   was used on a non-events topic.
3. **Publish** to `queries/demo/rpc` with MQTT 5.0 properties:
   - `ResponseTopic = $reply/<clientID>/inbox`
   - `CorrelationData = <uuid bytes>`
   **PUBACK arrives immediately** when the broker receives the publish — not after
   the responder replies. A PUBACK reason of `ImplementationSpecificError (0x83)`
   means the `ResponseTopic` is not in the own namespace; `QuotaExceeded (0x97)`
   means `RpcMaxPending` was exceeded.
4. **Receive** the response on the reply inbox. The response includes:
   - payload body (e.g. `pong:<original-payload>`)
   - MQTT 5.0 User-Properties carrying `kubemq-metadata` and any tags set by the
     responder (round-tripped as KubeMQ Tags)
   - `CorrelationData` echoed verbatim from the publish
5. **Disconnect** cleanly.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | **5.0 required** — v3.1.1 publishes to `queries/` are silently dropped |
| Query topic | `queries/demo/rpc` |
| KubeMQ channel | `demo.rpc` (`queries/` prefix consumed; `/` becomes `.`) |
| Reply topic | `$reply/<clientID>/inbox` |
| QoS | 1 (AtLeastOnce) on both subscribe and publish |
| v5 `ResponseTopic` | `$reply/<clientID>/inbox` (must be own namespace) |
| v5 `CorrelationData` | request UUID bytes, echoed on the response |
| Response body | present (unlike Commands, Queries carry a reply body) |
| Response user-props | `kubemq-metadata` + tags; `CorrelationData` echoed |
| KubeMQ pattern | Queries |
| Retain | not applicable — broker has RetainAvailable=0 |

## Gotchas

- **MQTT 5.0 only** — v3.1.1 `PUBLISH` to `queries/` is silently dropped by the
  connector with no error returned to the v3 client.
- **PUBACK is immediate** — the PUBACK for the publish arrives before the gRPC
  responder replies. Do NOT interpret PUBACK as a query response; implement a
  separate response-wait timeout (this example uses 30 s).
- **Own namespace only** — the `ResponseTopic` and the `$reply/...` subscription
  topic must both start with `$reply/<own-clientID>/`. Using a different client's
  prefix returns SUBACK/PUBACK `0x83`.
- **MQTT clients cannot be query responders** — the connector rejects an RPC
  subscription from an MQTT client with SUBACK `0x83`. A gRPC-side responder via
  `kubemq-go` is always required.
- **`RpcMaxPending` quota** — if too many in-flight RPC requests are outstanding,
  the broker returns PUBACK `0x97`. Back off and retry.

## Related Examples

- [rpc/command-round-trip](../command-round-trip/) — same flow for Commands (no body in response; `kubemq-executed` user-prop)
- [events/basic-pubsub](../../events/basic-pubsub/) — non-RPC fire-and-forget events with user-properties as KubeMQ tags
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — contrast MQTT v3.1.1 and v5; shows silent RPC drop on v3.1.1
