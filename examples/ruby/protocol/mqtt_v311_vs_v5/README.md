# Ruby — Protocol: MQTT v3.1.1 vs v5

Documents the Ruby `mqtt` gem's MQTT 3.1.1 capabilities and its limitations compared to MQTT 5.0 when used with the KubeMQ MQTT connector.

> **Important:** The Ruby `mqtt` gem (njh/ruby-mqtt) is **MQTT 3.1.1 only**. It does not support MQTT 5.0. The script demonstrates events pub/sub (which works fine over v3.1.1) and documents the v5-only features that are not available from Ruby with this gem. For a true side-by-side v3.1.1 vs v5 comparison, see the Go, Python, JavaScript, Java, C#, or Rust examples.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with MQTT connector enabled (port 1883)
- `mqtt` gem ~> 0.7

## How to Run

```bash
cd examples/ruby
bundle install
export KUBEMQ_MQTT_URL=tcp://localhost:1883
ruby protocol/mqtt_v311_vs_v5/main.rb
```

## MQTT Specifics

These describe the pub/sub that the script actually executes (the events round-trip over MQTT 3.1.1). The v3.1.1-vs-v5 feature matrix below is reference material, not what the script runs.

| Property | Value |
|----------|-------|
| Topic | `events/demo/v3` |
| KubeMQ channel | `demo.v3` (`/` → `.` mapping) |
| QoS | 1 |
| MQTT version | 3.1.1 (`mqtt` gem — njh/ruby-mqtt; MQTT 5.0 not supported) |
| KubeMQ pattern | Events (`events/` prefix) |
| User Properties | N/A — MQTT 3.1.1 has no User Properties |

## Expected Output

Captured from a live run against `tcp://127.0.0.1:1883`.

```
=== Ruby mqtt gem: MQTT 3.1.1 only ===

What works (v3.1.1):
  - events pub/sub
  - events-store pub/sub (StartNewOnly)
  - queues PRODUCE (plain publish to queues/<ch>)
  - connectivity/tls
  - connectivity/auth_jwt

What requires MQTT 5.0 (NOT supported by this gem):
  - User Properties <-> KubeMQ Tags mapping
  - Shared-subscription queue consume ($share/<g>/queues/<ch>)
  - RPC: commands and queries patterns (v3.1.1 publish silently dropped)
  - WebSocket transport (gem has no WebSocket)

Running events pub/sub over MQTT 3.1.1...
Published to events/demo/v3 (MQTT 3.1.1)
Received: topic=events/demo/v3 payload=hello from MQTT 3.1.1

Note: v5-only features (User Properties, shared-queue consume, RPC)
      are not available with the Ruby mqtt gem. Use a v5-capable MQTT
      client library for those patterns.
```

## MQTT 3.1.1 vs 5.0 Feature Matrix (Ruby gem perspective)

| Feature | MQTT 3.1.1 (this gem) | MQTT 5.0 (not supported by this gem) |
|---------|-----------------------|---------------------------------------|
| Events pub/sub | available | available |
| Events-Store pub/sub | available (StartNewOnly) | available (StartNewOnly) |
| Queues PRODUCE | available | available |
| Queues CONSUME (`$share`) | **N/A** (needs v5) | available (QoS >= 1) |
| RPC Commands | **silently dropped** (gotcha #4) | available |
| RPC Queries | **silently dropped** (gotcha #4) | available |
| User Properties <-> KubeMQ Tags | **N/A** (no user-props in v3) | available (32 props / 4096 B cap) |
| WebSocket transport | **N/A** (gem: TCP + TLS only) | available (ws://host:8083/) |
| Retain | silently dropped (gotcha #1) | silently dropped (gotcha #1) |

## Gotcha #4 — RPC v5-Only + Immediate PUBACK

A v3.1.1 client that publishes to `commands/` or `queries/` receives a PUBACK `0x00` (success at the wire level), but the message is **silently dropped** by the connector and a `publish.error` is audited server-side. The RPC request is never executed. This is documented in integration test #12 (`RPC311Dropped`).

## Related Examples

- [events/basic_pubsub](../../events/basic_pubsub/) — baseline pub/sub
- [rpc/command_round_trip](../../rpc/command_round_trip/) — N/A for Ruby (v5-only)
- [rpc/query_round_trip](../../rpc/query_round_trip/) — N/A for Ruby (v5-only)
