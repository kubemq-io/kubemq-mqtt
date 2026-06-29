# KubeMQ MQTT — Java Examples

All Java examples use the [Eclipse Paho MQTTv5 client](https://github.com/eclipse/paho.mqtt.java) (`org.eclipse.paho:org.eclipse.paho.mqttv5.client:1.2.5`) and Maven.
The MQTT v3.1.1-vs-v5 comparison example additionally uses the Paho MQTTv3 client.

## Prerequisites

- Java 21+
- Maven 3.8+
- KubeMQ server with the MQTT connector enabled and listening on `tcp://localhost:1883`

## Broker URL Environment Variable

Every example reads `KUBEMQ_MQTT_URL` (default `tcp://localhost:1883`).
Set it to match your broker before running:

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883   # plain TCP (default)
export KUBEMQ_MQTT_URL=ws://127.0.0.1:8083/   # WebSocket
export KUBEMQ_MQTT_URL=tls://127.0.0.1:8883   # TLS (requires cert setup)
```

The scheme prefix (`tcp`, `ws`, `tls`) selects the transport automatically.

## Run Any Example

```bash
cd examples/java/<pattern>/<variant>
mvn compile exec:java
```

For example:

```bash
cd examples/java/events/basic-pubsub
mvn compile exec:java
```

Or build and verify all examples compile from the language root:

```bash
cd examples/java
mvn compile
```

## Examples

| Pattern | Variant | Directory |
|---------|---------|-----------|
| Events | [Basic pub/sub](events/basic-pubsub/) | `events/basic-pubsub` |
| Events | [Wildcard subscribe](events/wildcard-subscribe/) | `events/wildcard-subscribe` |
| Events-Store | [Start-new-only](events-store/start-new-only/) | `events-store/start-new-only` |
| Queues | [Shared consume](queues/shared-consume/) | `queues/shared-consume` |
| Queues | [Ack and redelivery](queues/ack-and-redelivery/) | `queues/ack-and-redelivery` |
| RPC | [Query round-trip](rpc/query-round-trip/) | `rpc/query-round-trip` |
| RPC | [Command round-trip](rpc/command-round-trip/) | `rpc/command-round-trip` |
| Connectivity | [TLS](connectivity/tls/) | `connectivity/tls` |
| Connectivity | [WebSocket](connectivity/websocket/) | `connectivity/websocket` |
| Connectivity | [Auth JWT](connectivity/auth-jwt/) | `connectivity/auth-jwt` |
| Protocol | [MQTT v3.1.1 vs v5](protocol/mqtt-v311-vs-v5/) | `protocol/mqtt-v311-vs-v5` |

## MQTT Topic-to-KubeMQ Channel Mapping

| MQTT Topic Prefix | KubeMQ Pattern |
|-------------------|----------------|
| `events/<ch>` | Events (fire-and-forget) |
| `store/<ch>` | Events-Store (persistent) |
| `queues/<ch>` | Queues (produce) |
| `commands/<ch>` | Commands (RPC) |
| `queries/<ch>` | Queries (RPC) |

Topic `/` becomes `.` in the KubeMQ channel name (e.g., `events/site1/temp` -> channel `site1.temp`).

## Key Constraints

- MQTT 5.0 is required for RPC (commands/queries). Publishing to `commands/` or `queries/` topics with a v3.1.1 client is silently dropped.
- Wildcards (`+` maps to `*`, `#` maps to `>`) are allowed only on Events subscriptions.
- Queue consumption requires MQTT 5.0 shared subscriptions: `$share/<group>/queues/<ch>` at QoS >= 1.
- Retain is disabled: `CONNACK RetainAvailable=0`. Runtime retained publishes are silently dropped.
- Events-Store subscriptions always use `StartNewOnly` — no historical replay over MQTT.
