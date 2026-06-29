# KubeMQ MQTT — Documentation

Documentation for the KubeMQ MQTT connector.

The MQTT connector is an embedded [mochi-mqtt](https://github.com/mochi-mqtt/server) broker hosted inside the KubeMQ server. It bridges the MQTT protocol onto KubeMQ's five native messaging patterns — Events, Events-Store, Queues, Commands, and Queries — using standard MQTT client libraries from any language or tool.

## Contents

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Protocol stack diagram, MQTT topic → KubeMQ channel/pattern mapping, User-Properties ↔ Tags mapping |
| [getting-started.md](getting-started.md) | Docker quick-start, first event in minutes, per-language run table |
| [configuration.md](configuration.md) | `MqttConfig` fields, 17 `CONNECTORSMQTT_*` env vars, defaults, enable/disable |
| **Patterns** | |
| [patterns/events.md](patterns/events.md) | Fire-and-forget pub/sub, wildcard subscribe |
| [patterns/events-store.md](patterns/events-store.md) | Persistent pub/sub — StartNewOnly, NO historical replay |
| [patterns/queues.md](patterns/queues.md) | Produce + shared-subscription consume, ack/redelivery |
| [patterns/commands.md](patterns/commands.md) | RPC commands — MQTT 5.0 only, gRPC responder required |
| [patterns/queries.md](patterns/queries.md) | RPC queries — MQTT 5.0 only, gRPC responder required |
| **Guides** | |
| [guides/topic-mapping.md](guides/topic-mapping.md) | Topic grammar, wildcards, `$share`, `$reply`, dot-conflation, User-Properties ↔ Tags |
| [guides/authentication.md](guides/authentication.md) | Password-as-JWT authentication, ACL authorization |
| [guides/qos-and-sessions.md](guides/qos-and-sessions.md) | QoS levels, `clean_session`, session persistence |
| [guides/tls-and-websocket.md](guides/tls-and-websocket.md) | TLS on port 8883, WebSocket on port 8083 |
| [guides/protocol-versions.md](guides/protocol-versions.md) | MQTT 3.1.1 vs MQTT 5.0 feature matrix |
| [guides/migration.md](guides/migration.md) | Versioning & migration — v1.0.0 initial release, forward compatibility policy |
| **Reference** | |
| [reference/topic-grammar.md](reference/topic-grammar.md) | Formal grammar and prefix→pattern table |
| [reference/capabilities.md](reference/capabilities.md) | Forced capabilities, limits, retain correction |
| [reference/connections-endpoint.md](reference/connections-endpoint.md) | `GET /api/mqtt/connections` shape, Prometheus metrics |
| [reference/reason-codes.md](reference/reason-codes.md) | All MQTT reason codes the connector produces |

## Examples

Working code examples in 7 languages are in [../examples/](../examples/) — see [../examples/README.md](../examples/README.md) for the full index.

## Prerequisites

All examples and documentation assume:

- KubeMQ server running — the MQTT connector is **enabled by default** on port `1883`
- No extra configuration is needed to use MQTT (`CONNECTORSMQTT_ENABLE=true` is the default)
- To disable the connector: set `CONNECTORSMQTT_ENABLE=false`
- Default ports: TCP `1883`, TLS `8883` (requires Security config), WebSocket `8083`
- Single broker environment variable: `KUBEMQ_MQTT_URL` (default `tcp://localhost:1883`)

## Six Gotchas

Six non-obvious behaviors to read before writing code:

| # | Gotcha | Where documented |
|---|--------|-----------------|
| 1 | **Retain silently dropped** — a runtime PUBLISH with retain=1 gets PUBACK `0x00` (success) but the message is dropped and never delivered | [reference/capabilities.md](reference/capabilities.md) |
| 2 | **Events-Store never replays** — MQTT subscriptions to `store/<ch>` are always `StartNewOnly`; no historical messages are delivered | [patterns/events-store.md](patterns/events-store.md) |
| 3 | **`$share` group is one competing pool** — all group names compete in a single KubeMQ queue pool; the group name is audit/metrics-only, not a per-group copy | [patterns/queues.md](patterns/queues.md) |
| 4 | **RPC is MQTT 5.0 only + immediate PUBACK** — a v3.1.1 publish to `commands/` or `queries/` is silently dropped; v5 PUBACK arrives before the response, so clients must implement their own timeout | [patterns/commands.md](patterns/commands.md), [patterns/queries.md](patterns/queries.md) |
| 5 | **Literal `.` in a topic segment conflates with `/`** — `events/a.b/c` and `events/a/b/c` both map to channel `a.b.c` (lossy round-trip) | [guides/topic-mapping.md](guides/topic-mapping.md) |
| 6 | **Overlapping wildcard filters multiply delivery** — each matching bridge registry entry delivers one copy; N overlapping subscriptions matching a single publish → N copies received | [patterns/events.md](patterns/events.md) |
