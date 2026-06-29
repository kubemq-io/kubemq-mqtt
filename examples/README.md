# KubeMQ MQTT — Examples

Working code examples for the [KubeMQ MQTT connector](../docs/README.md) in 7 languages.

The connector bridges MQTT clients to KubeMQ patterns by topic prefix: `events/` → Events,
`store/` → Events-Store, `queues/` → Queues, `commands/` → Commands (RPC), `queries/` → Queries (RPC).
`/` in an MQTT topic maps to `.` in the KubeMQ channel name.

## Quick Start

1. Start KubeMQ with the MQTT connector enabled (it is enabled by default on port 1883):
   ```bash
   docker run -d -p 1883:1883 -p 8883:8883 -p 8083:8083 kubemq/kubemq
   ```

2. Set the broker URL (optional; defaults to `tcp://localhost:1883`). The scheme selects the
   transport — `tcp://` plain TCP, `tls://` TLS (8883), `ws://` WebSocket (8083, path `/`):
   ```bash
   export KUBEMQ_MQTT_URL=tcp://localhost:1883
   ```

3. Pick your language and follow its root README for run instructions.

## Languages

| Language | Folder | MQTT library (pinned) | Runtime / prerequisites |
|----------|--------|-----------------------|-------------------------|
| Go | [go/](go/) | `eclipse/paho.golang` v0.23.0 (v5) + `paho.mqtt.golang` v1.5.1 (v3.1.1, comparison only) | Go 1.24+ |
| Python | [python/](python/) | `paho-mqtt` 2.x (MQTT 5.0) | Python 3.10+, pip |
| JavaScript/TypeScript | [javascript/](javascript/) | `mqtt` (MQTT.js) v5.15.1+ (MQTT 5.0) | Node.js 18+, npm |
| Java | [java/](java/) | `org.eclipse.paho.mqttv5.client` 1.2.5 (+ Paho v3 for comparison) | Java 21+, Maven 3.8+ |
| C# | [csharp/](csharp/) | `MQTTnet` >= 4.3.7 (MQTT 5.0) | .NET 8+ |
| Ruby | [ruby/](ruby/) | `mqtt` gem (njh/ruby-mqtt) — **MQTT 3.1.1 only** | Ruby 3.1+, Bundler |
| Rust | [rust/](rust/) | `rumqttc` 0.25 (MQTT 5.0) | Rust 1.75+ (stable), Cargo |

> **Ruby subset note:** the Ruby `mqtt` gem speaks MQTT 3.1.1 only (no v5, no WebSocket).
> The v5-only variants — RPC (queries/commands), shared-subscription queue consume, and
> user-properties — and the WebSocket connectivity variant are **N/A for Ruby**. See spec decision D6.

## Pattern Matrix (11 variants)

Every language ships the same 11-variant set (Ruby excepted for the v5-only and WebSocket rows, per D6).
Each variant directory lives under `<language>/<pattern>/<variant>/`.

| # | Variant | Directory | KubeMQ pattern | Go | Python | JS/TS | Java | C# | Ruby | Rust |
|---|---------|-----------|----------------|----|--------|-------|------|----|------|------|
| 1 | Events: Basic Pub/Sub | `events/basic-pubsub` | Events | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 2 | Events: Wildcard Subscribe | `events/wildcard-subscribe` | Events (`+`→`*`, `#`→`>`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3 | Events-Store: Start New Only | `events-store/start-new-only` | Events-Store (no replay over MQTT) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 4 | Queues: Shared Consume | `queues/shared-consume` | Queues (`$share/<group>/queues/<ch>`, QoS ≥ 1) | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ (v5) | ✅ |
| 5 | Queues: Ack and Redelivery | `queues/ack-and-redelivery` | Queues (ack-on-PUBACK; redeliver on disconnect) | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ (v5) | ✅ |
| 6 | RPC: Query Round-Trip | `rpc/query-round-trip` | Queries (MQTT 5.0 only; gRPC responder) | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ (v5) | ✅ |
| 7 | RPC: Command Round-Trip | `rpc/command-round-trip` | Commands (MQTT 5.0 only; gRPC responder) | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ (v5) | ✅ |
| 8 | Connectivity: TLS | `connectivity/tls` | Events over TLS (8883) — see TLS note | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 9 | Connectivity: WebSocket | `connectivity/websocket` | Events over WebSocket (8083, path `/`) | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ (no WS) | ✅ |
| 10 | Connectivity: Auth JWT | `connectivity/auth-jwt` | Events with password-as-JWT | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 11 | Protocol: MQTT v3.1.1 vs v5 | `protocol/mqtt-v311-vs-v5` | Events (v3.1.1 RPC drop) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

✅ = shipped · ❌ = N/A (reason in parentheses)

> **TLS note:** the TLS variants ship and are structurally correct for every language (TLS is transport-level and works over MQTT 3.1.1 too, so Ruby is included). The connector's TLS listener (`8883`) is started **only when a KubeMQ Security configuration is present** — on a default deployment the port is open but the listener is not active, so a live TLS run requires a TLS-enabled broker. See [../docs/guides/tls-and-websocket.md](../docs/guides/tls-and-websocket.md).

## Gotchas (surfaced across docs and example READMEs)

1. **Retain is silently dropped.** Retain is forced off (`RetainAvailable=0`); a retained publish gets PUBACK `0x00` then is dropped (no disconnect, emits `publish.error`).
2. **Events-Store has no replay over MQTT.** `store/<ch>` subscriptions are always `StartNewOnly` — no historical replay.
3. **`$share` group is one competing pool**, not per-group copies — queue messages are load-balanced across the single pool.
4. **RPC is MQTT 5.0 only with immediate PUBACK.** v3.1.1 publishes to `commands/`/`queries/` are silently dropped; the client self-times-out. MQTT clients cannot be responders (a gRPC responder is required).
5. **`.` conflates with `/`.** `events/demo.temp` and `events/demo/temp` both map to channel `demo.temp`.
6. **Overlapping events wildcards deliver one copy per matching bridge registry entry** — N overlapping filters matching the same publish → N copies.

## Documentation

Full documentation is in [../docs/](../docs/README.md).
