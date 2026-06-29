# Configuration

## Overview

The MQTT connector is configured through the `MqttConfig` and `MqttCapabilitiesConfig` structs in KubeMQ's config layer. Every field maps to a `CONNECTORSMQTT_*` environment variable.

**Important naming convention**: the env var prefix is `CONNECTORSMQTT_` — no separator between `CONNECTORS` and `MQTT`, and **no** `KUBEMQ_` prefix. This is different from some other KubeMQ env vars.

The connector is **enabled by default**. You do not need to set any env var to use MQTT on a stock KubeMQ deployment.

## MqttConfig Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Enable` | bool | `true` | Enable the MQTT connector. Set to `false` to disable entirely. |
| `Port` | string | `"1883"` | TCP listener port. Set to `""` to disable the TCP listener. |
| `TlsPort` | string | `"8883"` | TLS listener port. Only active when a Security config (cert + key) is present. Set to `""` to disable. |
| `WsPort` | string | `"8083"` | WebSocket listener port (path is always `/`). Set to `""` to disable. |
| `DefaultPattern` | string | `"events"` | Routing for prefixless topics. One of: `events`, `store`, `none`. When `none`, a prefixless publish returns PUBACK `0x90`; a prefixless subscribe returns SUBACK `0x8F`. |
| `SubBuffSize` | int | `100` | Internal buffer size for event/events-store subscription delivery channels. Range: 1–10000. |
| `QueueAckTimeoutSeconds` | int | `30` | How long the connector waits for a PUBACK before treating a delivered queue message as not-acknowledged and redelivering it. Must be > 0. |
| `RpcTimeoutSeconds` | int | `30` | How long the connector waits for a gRPC-side RPC responder to reply before timing out. Must be > 0. |
| `RpcMaxPending` | int | `1024` | Maximum number of in-flight RPC requests across all clients. Exceeding this returns PUBACK `0x97`. Must be > 0. |
| `Capabilities` | struct | (see below) | mochi-mqtt server capability limits. |

## MqttCapabilitiesConfig Fields

These fields configure the protocol-level limits advertised in the MQTT `CONNACK` packet.

**Three capabilities are always forced regardless of config**:

| Capability | Forced value | Notes |
|------------|-------------|-------|
| `RetainAvailable` | `0` | Retain is not supported. A runtime publish with retain=1 is silently stripped — PUBACK succeeds but the message is dropped. See [gotcha #1](#gotcha-1-retain-silently-dropped). Will-retain at CONNECT yields CONNACK `0x9A`. |
| `SharedSubAvailable` | `1` | Required for `$share/` queue consume subscriptions. |
| `WildcardSubAvailable` | `1` | Required for events wildcard subscriptions. |

Configurable capabilities:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `MaxClients` | int64 | `0` (unlimited) | Maximum simultaneous MQTT connections. `0` = unlimited. |
| `MaxPacketSizeBytes` | uint32 | `4194304` (4 MB) | Maximum MQTT packet size in bytes. |
| `ReceiveMaximum` | uint16 | `1024` | Maximum in-flight QoS 1/2 publishes per connection (MQTT 5.0 flow control). |
| `MaxInflight` | uint16 | `8192` | Maximum total in-flight messages across all connections. |
| `MaxSessionExpirySeconds` | uint32 | `3600` | Maximum session expiry interval (seconds). Sessions are in-memory and node-local. |
| `MaxMessageExpirySeconds` | int64 | `86400` | Maximum message expiry interval (seconds). |
| `MaxQos` | byte | `2` | Maximum QoS level the broker accepts. Values: `0`, `1`, or `2`. |
| `MinProtocolVersion` | byte | `4` | Minimum MQTT protocol version. `4` = MQTT 3.1.1 (and above); `5` = MQTT 5.0 only. MQTT 3.1 (level 3) is always rejected. |

## Environment Variables

All 17 `CONNECTORSMQTT_*` variables:

| Environment variable | Default | Maps to |
|---------------------|---------|---------|
| `CONNECTORSMQTT_ENABLE` | `true` | `MqttConfig.Enable` |
| `CONNECTORSMQTT_PORT` | `1883` | `MqttConfig.Port` — set to `""` to disable TCP |
| `CONNECTORSMQTT_TLS_PORT` | `8883` | `MqttConfig.TlsPort` — set to `""` to disable TLS |
| `CONNECTORSMQTT_WS_PORT` | `8083` | `MqttConfig.WsPort` — set to `""` to disable WebSocket |
| `CONNECTORSMQTT_DEFAULT_PATTERN` | `events` | `MqttConfig.DefaultPattern` — `events` \| `store` \| `none` |
| `CONNECTORSMQTT_SUB_BUFF_SIZE` | `100` | `MqttConfig.SubBuffSize` — 1–10000 |
| `CONNECTORSMQTT_QUEUE_ACK_TIMEOUT_SECONDS` | `30` | `MqttConfig.QueueAckTimeoutSeconds` — must be > 0 |
| `CONNECTORSMQTT_RPC_TIMEOUT_SECONDS` | `30` | `MqttConfig.RpcTimeoutSeconds` — must be > 0 |
| `CONNECTORSMQTT_RPC_MAX_PENDING` | `1024` | `MqttConfig.RpcMaxPending` — must be > 0 |
| `CONNECTORSMQTT_CAPABILITIES_MAX_CLIENTS` | `0` | `MqttCapabilitiesConfig.MaxClients` — 0 = unlimited |
| `CONNECTORSMQTT_CAPABILITIES_MAX_PACKET_SIZE_BYTES` | `4194304` | `MqttCapabilitiesConfig.MaxPacketSizeBytes` |
| `CONNECTORSMQTT_CAPABILITIES_RECEIVE_MAXIMUM` | `1024` | `MqttCapabilitiesConfig.ReceiveMaximum` |
| `CONNECTORSMQTT_CAPABILITIES_MAX_INFLIGHT` | `8192` | `MqttCapabilitiesConfig.MaxInflight` |
| `CONNECTORSMQTT_CAPABILITIES_MAX_SESSION_EXPIRY_SECONDS` | `3600` | `MqttCapabilitiesConfig.MaxSessionExpirySeconds` |
| `CONNECTORSMQTT_CAPABILITIES_MAX_MESSAGE_EXPIRY_SECONDS` | `86400` | `MqttCapabilitiesConfig.MaxMessageExpirySeconds` |
| `CONNECTORSMQTT_CAPABILITIES_MAX_QOS` | `2` | `MqttCapabilitiesConfig.MaxQos` — 0, 1, or 2 |
| `CONNECTORSMQTT_CAPABILITIES_MIN_PROTOCOL_VERSION` | `4` | `MqttCapabilitiesConfig.MinProtocolVersion` — 4 or 5 |

## Validation Rules

These rules are enforced at startup (`MqttConfig.Validate()`):

- When `Enable=true`: at least one of `Port`, `TlsPort`, `WsPort` must be set (non-empty).
- All set ports must be distinct from each other.
- `DefaultPattern` must be one of `events`, `store`, `none`.
- `SubBuffSize` must be in the range 1–10000.
- `QueueAckTimeoutSeconds`, `RpcTimeoutSeconds`, `RpcMaxPending` must all be > 0.
- `TlsPort` is silently ignored (with a warning log) if no Security config (cert + key) is provided.

## Docker Quick-Reference

```bash
# Default — MQTT on 1883, WebSocket on 8083, TLS listener present but inactive
docker run -d -p 1883:1883 -p 8083:8083 kubemq/kubemq

# Disable MQTT entirely
docker run -d kubemq/kubemq -e CONNECTORSMQTT_ENABLE=false

# Disable only the WebSocket listener
docker run -d -p 1883:1883 kubemq/kubemq -e CONNECTORSMQTT_WS_PORT=""

# Change DefaultPattern so prefixless topics route to Events-Store
docker run -d -p 1883:1883 kubemq/kubemq -e CONNECTORSMQTT_DEFAULT_PATTERN=store

# Reject prefixless topics entirely (PUBACK 0x90 / SUBACK 0x8F)
docker run -d -p 1883:1883 kubemq/kubemq -e CONNECTORSMQTT_DEFAULT_PATTERN=none

# Restrict to MQTT 5.0 only (reject 3.1.1 clients)
docker run -d -p 1883:1883 kubemq/kubemq -e CONNECTORSMQTT_CAPABILITIES_MIN_PROTOCOL_VERSION=5

# Increase RPC pending limit
docker run -d -p 1883:1883 kubemq/kubemq -e CONNECTORSMQTT_RPC_MAX_PENDING=4096

# Lower queue ack timeout to 10 seconds
docker run -d -p 1883:1883 kubemq/kubemq -e CONNECTORSMQTT_QUEUE_ACK_TIMEOUT_SECONDS=10
```

## TOML Configuration (alternative to env vars)

```toml
[Connectors.MQTT]
  Enable = true
  Port = "1883"
  TlsPort = "8883"
  WsPort = "8083"
  DefaultPattern = "events"
  SubBuffSize = 100
  QueueAckTimeoutSeconds = 30
  RpcTimeoutSeconds = 30
  RpcMaxPending = 1024

  [Connectors.MQTT.Capabilities]
    MaxClients = 0
    MaxPacketSizeBytes = 4194304
    ReceiveMaximum = 1024
    MaxInflight = 8192
    MaxSessionExpirySeconds = 3600
    MaxMessageExpirySeconds = 86400
    MaxQos = 2
    MinProtocolVersion = 4
```

## Gotcha #1 — Retain Silently Dropped {#gotcha-1-retain-silently-dropped}

> **WARNING**: The broker advertises `RetainAvailable=0` in `CONNACK`. Any MQTT client library that respects this will refuse to send a retained publish at the library level. If you use a library that does not check, or if you set the retain flag manually on a raw publish, the broker's mochi layer silently strips the retain flag, issues a success PUBACK (`0x00`), and **drops the message** — it is never delivered and never stored. The `publish.error` metric is incremented. This behavior is **not** a DISCONNECT (`0x9A`); that code is returned only for Will-retain set at CONNECT time.

See [reference/capabilities.md](reference/capabilities.md) for the full retain correction.

## Listener Defaults at a Glance

| Listener | Default port | Env var | Disable by setting |
|----------|-------------|---------|-------------------|
| TCP | `1883` | `CONNECTORSMQTT_PORT` | `""` (empty string) |
| TLS | `8883` | `CONNECTORSMQTT_TLS_PORT` | `""` or omit Security config |
| WebSocket | `8083` | `CONNECTORSMQTT_WS_PORT` | `""` (empty string) |

The WebSocket listener serves MQTT-over-WebSocket at path `/`. Both MQTT 3.1.1 and 5.0 clients can connect over WebSocket. When a Security config is present, the WebSocket listener is upgraded to `wss://` (MQTT-over-TLS-WebSocket).
