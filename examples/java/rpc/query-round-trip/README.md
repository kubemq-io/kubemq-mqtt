# java — RPC: Query Round-Trip

Send a Query RPC request over MQTT 5.0 and receive the response with body, metadata, and tags.

**Requires MQTT 5.0.** Publishing to `queries/` with a MQTT 3.1.1 client is silently dropped — the PUBACK is acknowledged at the wire level but no request reaches the responder because v3.1.1 has no `ResponseTopic` property.

A gRPC-side responder must be running before you start this example. MQTT clients cannot act as RPC responders (a subscribe to `queries/` or `commands/` from an MQTT client is rejected with SUBACK `0x83`); only gRPC-side subscribers can respond.

## Prerequisites

- Java 21+, Maven 3.8+
- KubeMQ server with the MQTT connector enabled (default `tcp://127.0.0.1:1883`)
- gRPC responder running on `127.0.0.1:50000` for channel `demo.rpc` type `query`:

```bash
cd .work/test-harness/responder
go run . -channel demo.rpc -type query
```

## How to Run

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
cd examples/java/rpc/query-round-trip
mvn compile exec:java
```

## Expected Output

```
[rpc/query-round-trip] connecting to tcp://127.0.0.1:1883 as java-mqtt-query-client-<id>
[step 1] connected (MQTT 5.0)
[step 2] subscribing to reply inbox: $reply/java-mqtt-query-client-<id>/inbox
[connection] connected to tcp://127.0.0.1:1883
[step 2] subscribed to reply inbox (grantedQoS=1)
[step 3] publishing query to: queries/demo/rpc
         payload:         ping
         ResponseTopic:   $reply/java-mqtt-query-client-<id>/inbox
         CorrelationData: corr-123
[step 4] PUBACK received (immediate — response is NOT yet here; waiting up to 10 s)
[response] arrived on topic: $reply/java-mqtt-query-client-<id>/inbox
[step 5] response received:
         body:            pong:ping
         CorrelationData: corr-123 (echoed=true)
         user-properties:
           kubemq-metadata = responder/query
           responder = mqtt-test
           rpc-type = query
[step 6] disconnecting
[done] query round-trip completed successfully
```

## What's Happening

1. **Subscribe to own reply inbox first.** The client subscribes to `$reply/<clientID>/inbox` at QoS 1 before sending anything. The `$reply/<clientID>/` namespace is mochi-local — it is never bridged to other nodes or persisted in KubeMQ Events-Store.

2. **Publish the query.** The client publishes to `queries/demo/rpc` at QoS 1 with two mandatory MQTT 5.0 properties:
   - `ResponseTopic` set to the reply inbox `$reply/<own-clientID>/inbox`
   - `CorrelationData` set to a correlation identifier (echoed on the response)

3. **PUBACK arrives immediately.** The broker acknowledges the publish before the gRPC responder has processed anything. This is by design — PUBACK is NOT a signal that a response has arrived. The client implements its own 10-second response-wait timeout.

4. **Responder processes the query.** The gRPC-side responder receives the forwarded request on KubeMQ channel `demo.rpc`, prepends `pong:` to the body, and sends a `QueryReply` with metadata and tags.

5. **Response delivered to inbox.** The broker delivers the response to `$reply/<clientID>/inbox`. The response carries:
   - Body — the query reply payload (`pong:ping` in the test responder)
   - `CorrelationData` — echoed from the original request
   - User properties — `kubemq-metadata` plus any tags the responder set

6. **Clean disconnect.** The client disconnects and releases the connection.

## MQTT Specifics

| Property | Value |
|---|---|
| Request topic | `queries/demo/rpc` |
| KubeMQ channel | `demo.rpc` (topic `/` maps to channel `.`) |
| Reply topic | `$reply/<clientID>/inbox` (must be own `$reply/<clientID>/` namespace) |
| QoS | 1 (both subscribe and publish) |
| MQTT version | 5.0 (mandatory — v3.1.1 publish is silently dropped) |
| `ResponseTopic` property | `$reply/<own-clientID>/inbox` |
| `CorrelationData` property | Echoed unchanged on the response |
| PUBACK timing | Immediate on receipt, before the response |
| Response body | Query reply body (non-empty, unlike commands) |
| Response user properties | `kubemq-metadata` + responder tags |
| Retain | Forced `RetainAvailable=0`; setting retain at runtime → PUBACK succeeds but message is silently dropped |

## Gotchas

**Immediate PUBACK does not mean response.** The broker sends PUBACK as soon as it accepts the publish. The actual query response arrives later, after the gRPC responder processes it. Always implement a client-side timeout for the response wait (this example uses 10 seconds).

**Foreign `$reply` namespace → PUBACK `0x83`.** The `ResponseTopic` in the publish properties MUST start with `$reply/<own-clientID>/`. Using another client's reply namespace causes the broker to reject the publish with PUBACK reason code `0x83`.

**RpcMaxPending quota.** If too many in-flight RPC requests accumulate without responses, the broker returns PUBACK `0x97` (quota exceeded). Requests time out server-side after `RpcTimeoutSeconds` (default 30 s) if no responder replies.

**No array subscription for `$reply`.** The reply inbox is mochi-broker-local. It is never registered as an array subscription in the KubeMQ array.

**Responder must be gRPC-side.** An MQTT subscriber to `queries/` receives SUBACK `0x83` and cannot act as a responder. Use `kubemq-go/v2` (or another gRPC SDK) to subscribe and respond.

## Related Examples

- [rpc/command-round-trip](../command-round-trip/) — command RPC (no response body; only `kubemq-executed` user property)
- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget, no reply inbox
- [queues/shared-consume](../../queues/shared-consume/) — competing consumers via `$share` subscriptions
