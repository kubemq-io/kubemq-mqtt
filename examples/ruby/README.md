# KubeMQ MQTT — Ruby Examples

All Ruby examples use the [`mqtt` gem](https://github.com/njh/ruby-mqtt) (njh/ruby-mqtt), which implements **MQTT 3.1.1 only** (no MQTT 5.0, no WebSocket transport — TCP and TLS only).

> **Important Ruby subset note:** v5-only patterns (RPC, shared-subscription queue consume, user-properties) require a v5-capable client; the Ruby `mqtt` gem is MQTT 3.1.1 only. WebSocket transport is also unavailable: the njh/ruby-mqtt `mqtt` gem speaks only TCP and TLS (no WebSocket transport), so the `connectivity/websocket` example is N/A for Ruby. See spec decision D6.

## Prerequisites

- Ruby 3.1+
- Bundler (`gem install bundler`)
- KubeMQ server running with the MQTT connector enabled (port 1883 by default):
  ```bash
  docker run -d -p 1883:1883 -p 8883:8883 europe-docker.pkg.dev/kubemq/images/kubemq-next:latest
  ```
- MQTT library: `mqtt` gem >= 0.6.0 (pinned to `~> 0.7` in this Gemfile)

## Setup

```bash
cd examples/ruby
bundle install
```

## Broker URL

All examples read the single env var `KUBEMQ_MQTT_URL` (default `tcp://localhost:1883`).

```bash
export KUBEMQ_MQTT_URL=tcp://localhost:1883
```

For TLS examples (port 8883, requires a TLS-enabled KubeMQ server):

```bash
export KUBEMQ_MQTT_URL=tls://localhost:8883
```

## Run Any Example

```bash
ruby events/basic_pubsub/main.rb
ruby events/wildcard_subscribe/main.rb
ruby events_store/start_new_only/main.rb
ruby connectivity/tls/main.rb
ruby connectivity/auth_jwt/main.rb
ruby protocol/mqtt_v311_vs_v5/main.rb
```

## Example Index

| # | Directory | Pattern | Description | Ruby |
|---|-----------|---------|-------------|------|
| 1 | [events/basic_pubsub](events/basic_pubsub/) | Events | Fire-and-forget pub/sub | available |
| 2 | [events/wildcard_subscribe](events/wildcard_subscribe/) | Events | Wildcard subscribers + overlap behavior | available |
| 3 | [events_store/start_new_only](events_store/start_new_only/) | Events-Store | Persistent pub/sub — StartNewOnly, no replay | available |
| 4 | [queues/shared_consume](queues/shared_consume/) | Queues | Shared-subscription consumer | N/A — needs MQTT 5.0 |
| 5 | [queues/ack_and_redelivery](queues/ack_and_redelivery/) | Queues | Ack / redelivery / timeout | N/A — needs MQTT 5.0 |
| 6 | [rpc/query_round_trip](rpc/query_round_trip/) | Queries (RPC) | MQTT 5.0 query round-trip | N/A — needs MQTT 5.0 |
| 7 | [rpc/command_round_trip](rpc/command_round_trip/) | Commands (RPC) | MQTT 5.0 command round-trip | N/A — needs MQTT 5.0 |
| 8 | [connectivity/tls](connectivity/tls/) | Events over TLS | TLS client with `ssl: true` | available |
| 9 | [connectivity/websocket](connectivity/websocket/) | Events over WebSocket | WebSocket transport | N/A — gem has no WebSocket |
| 10 | [connectivity/auth_jwt](connectivity/auth_jwt/) | Events + Auth | Password-as-JWT authentication | available |
| 11 | [protocol/mqtt_v311_vs_v5](protocol/mqtt_v311_vs_v5/) | Events (v3.1.1) | Protocol contrast note — gem is v3.1.1 only | note-only |

Ruby ships 6 of the 11 variants as runnable scripts; the remaining 5 (`queues/shared_consume`, `queues/ack_and_redelivery`, `rpc/query_round_trip`, `rpc/command_round_trip`, `connectivity/websocket`) are documented N/A — each directory's README explains why (all require MQTT 5.0, or — for WebSocket — a transport the `mqtt` gem does not provide).

## Topic Grammar Quick Reference

| MQTT topic prefix | KubeMQ pattern |
|-------------------|----------------|
| `events/<ch>` | Events |
| `store/<ch>` | Events-Store |
| `queues/<ch>` | Queues (produce only from Ruby) |
| `commands/<ch>` | Commands RPC (v5-only, N/A in Ruby) |
| `queries/<ch>` | Queries RPC (v5-only, N/A in Ruby) |
| `$share/<g>/queues/<ch>` | Queues consume (v5-only, N/A in Ruby) |

Topic `/` becomes `.` in the KubeMQ channel. A literal `.` in a topic segment also becomes `.` in the channel name — it conflates with `/` (lossy; **gotcha #5**).

## KubeMQ MQTT Connector Gotchas (Ruby-relevant)

1. **Retain silently dropped** — the broker forces `RetainAvailable=0`. A retained PUBLISH returns PUBACK success (`0x00`) but the message is **silently discarded** (not delivered, not stored). Do not set the retain flag.
2. **Events-Store never replays** — `store/` subscriptions always start `StartNewOnly`. Messages published before a subscription was created are never delivered.
5. **Literal `.` in a topic segment conflates with `/`** — `events/a.b/c` and `events/a/b/c` both map to KubeMQ channel `a.b.c`. Avoid dots in topic segments.

(Gotchas #3/#4/#6 require MQTT 5.0 or v5-specific patterns and do not apply to the Ruby `mqtt` gem subset.)
