# csharp — Protocol: MQTT 3.1.1 vs 5.0

Demonstrates the behavioral differences between MQTT 3.1.1 and MQTT 5.0 when using the KubeMQ MQTT connector. The example shows what each protocol version can and cannot do, making the upgrade case for MQTT 5.0 concrete and testable.

## Prerequisites

- .NET 8+
- KubeMQ server with MQTT connector enabled (default `tcp://localhost:1883`)
- Optional: a running gRPC responder on channel `demo.cmd` (type command) for the RPC round-trip in Part 2c (see `.work/test-harness/responder/`): `go run . -channel demo.cmd -type command`; without it the RPC wait times out gracefully

## How to Run

```bash
cd examples/csharp/protocol/mqtt-v311-vs-v5
dotnet run

# or with a custom broker URL:
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run
```

## Expected Output

```
Broker: tcp://127.0.0.1:1883

═══════════════════════════════════════════════════════════
  PART 1 — MQTT 3.1.1
═══════════════════════════════════════════════════════════
[1a] MQTT 3.1.1 — Events pub/sub
     Topic: events/demo/v3  (KubeMQ channel: demo.v3)
     Subscriber connected  (CONNACK=Success)
     Subscribed  (SUBACK=GrantedQoS1)
     Publisher connected  (CONNACK=Success)
     Published to 'events/demo/v3'  (no user properties — v3.1.1 cannot carry KubeMQ Tags)
     Received: {"hello":"from v3.1.1","ts":"..."}
     v3.1.1 basic events pub/sub: OK

[1b] MQTT 3.1.1 — RPC attempt on 'commands/demo/x' (silently dropped)
     A v3.1.1 publish to commands/ or queries/ returns PUBACK 0x00 (success)
     but the command is NEVER executed — silently dropped by the connector.
     PUBACK returned: Success  (0x00=Success — wire ack OK, but command was NOT executed)
     v3.1.1 RPC silently dropped: confirmed.

[1c] MQTT 3.1.1 — Shared subscription ($share) is NOT available.
     Queue consume requires MQTT 5.0 shared subscriptions.
     v3.1.1 cannot subscribe to $share/... (no such protocol feature).

     v3.1.1 clients disconnected.

═══════════════════════════════════════════════════════════
  PART 2 — MQTT 5.0
═══════════════════════════════════════════════════════════
[2a] MQTT 5.0 — Events with User Properties (KubeMQ Tags)
     Topic: events/demo/v3  (KubeMQ channel: demo.v3)
     Subscriber connected  (CONNACK=Success)
     Subscribed  (SUBACK=GrantedQoS1)
     Publisher connected  (CONNACK=Success)
     Published with UserProperty 'x-source=csharp-v5-example'  (PUBACK=Success)
     Received: {"hello":"from v5.0","ts":"..."}
     Received UserProperty 'x-source': csharp-v5-example
     User-property 'x-source' round-tripped as KubeMQ Tag: OK

[2b] MQTT 5.0 — Shared subscription ($share) for queue consume
     v5.0 supports $share/<group>/queues/<ch> at QoS>=1.
     Subscribed to '$share/demo-group/queues/demo/v5-q'  (SUBACK=GrantedQoS1) — OK
     Plain subscribe to 'queues/demo/v5-q' (no $share) -> SUBACK=ImplementationSpecificError (0x83 = rejected — must use $share/... syntax)

[2c] MQTT 5.0 — RPC publish to 'commands/demo/cmd' with ResponseTopic
     ...
     RPC client connected as 'csharp-v5-rpc-...'  (CONNACK=Success)
     Subscribed to reply topic '$reply/csharp-v5-rpc-.../inbox'  (SUBACK=GrantedQoS1)
     Published command to 'commands/demo/cmd'  (PUBACK=Success — IMMEDIATE, not after response)
     Command response received:
       correlation-id  : <uuid>
       kubemq-executed : True
     v5.0 RPC round-trip: OK

     v5.0 clients disconnected.

═══════════════════════════════════════════════════════════
  Summary: v3.1.1 vs v5.0
═══════════════════════════════════════════════════════════
  Feature                    │ MQTT 3.1.1       │ MQTT 5.0
  ─────────────────────────────────────────────────────────
  Events pub/sub             │ OK               │ OK
  User Properties (Tags)     │ NOT AVAILABLE    │ Supported (32/4096B)
  Queue shared-sub ($share)  │ NOT AVAILABLE    │ Supported (QoS>=1)
  RPC (commands/queries)     │ SILENTLY DROPPED │ Supported ($reply/...)
  Retain                     │ Silently dropped │ Silently dropped
  Reason codes               │ Binary only      │ Detailed hex codes
```

## What's Happening

### Part 1 — MQTT 3.1.1

1. **[1a] Events pub/sub** — Two v3.1.1 clients connect with `MqttProtocolVersion.V311`. The subscriber connects and subscribes to `events/demo/v3` before the publisher connects. A JSON payload is published and received. No User Properties are set or received — v3.1.1 has no such field.

2. **[1b] RPC silently dropped** — The v3.1.1 publisher publishes to `commands/demo/x`. The MQTT wire layer acknowledges with PUBACK `0x00` (success). However, the KubeMQ connector silently drops the message — it is never routed as an RPC command. No error is returned to the client. This is the v3.1.1 RPC drop behavior (spec test #12 `RPC311Dropped`).

3. **[1c] Shared subscription unavailable** — v3.1.1 has no `$share/...` shared subscription syntax. Queue consumption is not possible from a v3.1.1 client.

### Part 2 — MQTT 5.0

4. **[2a] Events with User Properties** — Two v5.0 clients connect with `MqttProtocolVersion.V500`. The publisher sets a User Property `x-source=csharp-v5-example`. The KubeMQ connector maps this to a KubeMQ message Tag. On delivery to the v5.0 subscriber, the Tag is mapped back to a User Property — demonstrating the bidirectional Tags ↔ User Properties bridge (connector spec §2.9).

5. **[2b] Shared subscription for queues** — The v5.0 client subscribes to `$share/demo-group/queues/demo/v5-q` at QoS 1 (success). The same client then attempts a plain `queues/demo/v5-q` subscribe (without `$share/`) and receives SUBACK `0x83` — confirming the `$share` syntax is mandatory. QoS 0 on a shared subscription also returns SUBACK `0x83`.

6. **[2c] RPC round-trip** — A dedicated v5.0 RPC client subscribes to its own `$reply/<clientID>/inbox` topic **before** publishing. It then publishes to `commands/demo/cmd` (KubeMQ channel `demo.cmd`) with `ResponseTopic=$reply/<clientID>/inbox` and `CorrelationData`. The PUBACK is returned **immediately** by the broker — before the command is processed. The client waits separately for the response on the reply topic. The response carries no body; the result is in User Property `kubemq-executed=true/false` with optional `kubemq-error`. Requires a gRPC responder on channel `demo.cmd` (type command).

## MQTT Specifics

| Property | MQTT 3.1.1 | MQTT 5.0 |
|---|---|---|
| Protocol version | `MqttProtocolVersion.V311` | `MqttProtocolVersion.V500` |
| Events topic (pub + sub) | `events/demo/v3` | `events/demo/v3` |
| KubeMQ channel | `demo.v3` | `demo.v3` |
| QoS | 1 (AtLeastOnce) | 1 (AtLeastOnce) |
| User Properties | Not available | `x-source=csharp-v5-example` (KubeMQ Tag) |
| User Property cap | N/A | 32 properties / 4096 bytes total; exceeding → PUBACK `0x97` |
| Queue consume topic | Not available | `$share/demo-group/queues/demo/v5-q` |
| Queue consume QoS | N/A | 1 (QoS 0 → SUBACK `0x83`) |
| RPC topic | `commands/demo/x` (publish silently dropped) | `commands/demo/cmd` (KubeMQ channel `demo.cmd`) |
| RPC ResponseTopic | Not available | `$reply/<clientID>/inbox` |
| RPC CorrelationData | Not available | UUID bytes |
| PUBACK for RPC | `0x00` — wire ack, command NOT executed | `0x00` — IMMEDIATE (before response) |
| RPC response body | N/A | Empty (commands have no body) |
| RPC response user-props | N/A | `kubemq-executed=true/false`, optional `kubemq-error` |

## Gotchas

**RPC is MQTT 5.0 ONLY — and the PUBACK is immediate.**
A v3.1.1 publish to `commands/` or `queries/` returns PUBACK `0x00` (Success) at the wire level. The command is silently dropped — it is never executed, and no error is returned. There is no way to detect this failure from a v3.1.1 client. Always use MQTT 5.0 with `ResponseTopic` + `CorrelationData` for RPC. Even on v5.0, the PUBACK is returned immediately on receipt — not after the RPC executes. Implement a client-side timeout to wait for the reply-topic response.

**User Properties (KubeMQ Tags) require MQTT 5.0.**
A v3.1.1 publish carries no user-property field. KubeMQ Tags set by a gRPC producer are not echoed to v3.1.1 subscribers. If you need KubeMQ message tags, you must use MQTT 5.0.

**Queue consume requires `$share/` prefix.**
Subscribing to `queues/<ch>` directly (without `$share/<group>/`) returns SUBACK `0x83` (ImplementationSpecificError). Use `$share/<group>/queues/<ch>` at QoS 1 or 2. QoS 0 on a shared subscription also returns `0x83`.

**`$share` group name is audit/metrics-only — not per-group copies.**
Unlike standard MQTT brokers that deliver one copy per group, the KubeMQ connector maps ALL `$share` groups to a single shared queue pool. Consumers from different group names all compete for the same messages. The group name is for observability only.

**Retain is silently dropped on BOTH protocol versions.**
A PUBLISH with `Retain=true` receives PUBACK `0x00` (success) but the message is never stored or delivered. The v5.0 CONNACK carries `RetainAvailable=0` as an informational flag, but runtime retain publishes on either version are silently dropped without disconnecting the client. Only a Will-retain at CONNECT time triggers an active CONNACK `0x9A` rejection.

**Literal `.` in a topic segment is the same as `/`.**
`events/demo.v3` and `events/demo/v3` both map to KubeMQ channel `demo.v3`. Avoid dots in topic path segments to prevent unintended channel aliasing.

## Related Examples

- [events/basic-pubsub](../../events/basic-pubsub/) — MQTT 5.0 events pub/sub with User Properties
- [events/wildcard-subscribe](../../events/wildcard-subscribe/) — wildcard subscriptions
- [queues/shared-consume](../../queues/shared-consume/) — full shared-subscription queue consume
- [rpc/command-round-trip](../../rpc/command-round-trip/) — full MQTT 5.0 command RPC
- [rpc/query-round-trip](../../rpc/query-round-trip/) — full MQTT 5.0 query RPC
