# Capabilities

This page lists every capability the KubeMQ MQTT connector advertises to clients —
including the values that are **forced** regardless of configuration — and every
configurable limit.

---

## Forced (Non-Configurable) Capabilities

These flags are hardcoded and cannot be changed at runtime:

| Capability | Wire value | Effect |
|---|---|---|
| `RetainAvailable` | `0` | Retain is disabled. See [Gotcha: Retain Silently Dropped](#gotcha-retain). |
| `SharedSubAvailable` | `1` | Required for `$share/<group>/queues/<ch>` queue consumption. |
| `WildcardSubAvailable` | `1` | Required for Events wildcard subscriptions (`+`, `#`). |
| `NoInheritedPropertiesOnAck` | `true` | PUBACK never echoes PUBLISH user properties back to the sender. Every successful QoS 1 PUBACK is property-free. |

---

## Configurable Limits and Defaults

The corresponding
environment variable for each is listed in the [Configuration guide](../configuration.md).

| Capability | Default | Min | Max | Notes |
|---|---|---|---|---|
| `MaximumPacketSize` | `4 194 304` (4 MB) | 1 | — | Maximum PUBLISH payload + header size in bytes. Clients that send larger packets are disconnected. |
| `ReceiveMaximum` | `1024` | 1 | 65 535 | Maximum number of concurrent in-flight QoS 1/2 publishes per client (flow control). |
| `MaximumInflight` | `8192` | 1 | — | Broker-wide in-flight QoS message cap. |
| `MaximumSessionExpiryInterval` | `3600` s | 0 | — | Maximum allowed value clients may request for `SessionExpiryInterval`. |
| `MaximumMessageExpiryInterval` | `86400` s | 0 | — | Maximum allowed message expiry. **RPC publishes only**: if `MessageExpiryInterval` is set on a `commands/` or `queries/` PUBLISH and is shorter than `RpcTimeoutSeconds`, the effective RPC timeout becomes `min(RpcTimeoutSeconds, MessageExpiryInterval)`. This does not apply to queue or events publishes. |
| `MaximumQos` | `2` | 0 | 2 | Highest QoS the broker grants. |
| `MinimumProtocolVersion` | `4` (MQTT 3.1.1) | 4 | 5 | MQTT 3.1 (level 3) is rejected at CONNECT. |
| `MaximumClients` | `0` (unlimited) | 0 | — | `0` = unlimited. |

---

## Protocol Version Support

| Protocol | Level byte | Accepted | Notes |
|---|---|---|---|
| MQTT 5.0 | `0x05` | Yes | Full feature set: user properties, reason codes, shared subs, RPC flow. |
| MQTT 3.1.1 | `0x04` | Yes | No user properties, no RPC, no `$share` queue consume. |
| MQTT 3.1 | `0x03` | **Rejected** | CONNECT is refused for an unsupported protocol level (`mochi` `server.go:548-549`, `ErrUnsupportedProtocolVersion`). Because a level-3 CONNECT uses the MQTT 3.x CONNACK format, the byte returned is the v3 return code `0x01` ("unacceptable protocol version") — not one of the MQTT 5.0 reason codes listed in [reference/reason-codes.md](reason-codes.md). |

`MinimumProtocolVersion=4` is the hardcoded floor. Set
`CONNECTORSMQTT_CAPABILITIES_MIN_PROTOCOL_VERSION=5` to allow MQTT 5.0 clients only.

---

## Session Semantics

Sessions are **in-memory and node-local**:

| Property | Behavior |
|---|---|
| `clean_start=false` (v5) / `clean_session=0` (v3.1.1) | Session is preserved in the node's memory. A reconnect to the **same node** within `SessionExpiryInterval` restores subscriptions and queued QoS 1/2 messages without re-subscribing. |
| Reconnect to a **different node** | Session is lost (not replicated across nodes). |
| `clean_start=true` | Fresh session; no state carried over. |

`StartNewOnly` for Events-Store: restored sessions do **not** replay historical
Events-Store messages. The bridge re-subscribes at `StartNewOnly` after reconnect.
See [Gotcha: Events-Store No Replay](#gotcha-events-store).

---

## Transport Listeners

| Listener | Default port | URL scheme | Notes |
|---|---|---|---|
| TCP (plain) | `1883` | `tcp://host:1883` | Always active when `Port` is set (default `1883`). |
| TLS | `8883` | `tls://host:8883` | Active **only** when `Security` config is present. TLS min 1.2; mTLS supported. If no TLS material is configured, the listener is silently skipped with a warning. |
| WebSocket | `8083` | `ws://host:8083/` | Active when `WsPort` is set. TLS WebSocket (`wss://`) when Security config present. |

Environment variable: `KUBEMQ_MQTT_URL` (examples) defaults to `tcp://localhost:1883`.

---

## User Properties (MQTT 5.0 Only) and KubeMQ Tags

MQTT 5.0 user properties map bidirectionally to KubeMQ message tags for Events,
Events-Store, and Queues patterns. MQTT 3.1.1 has no user properties.

| Direction | Mapping |
|---|---|
| PUBLISH → KubeMQ | `Properties.User` entries copied 1:1 into KubeMQ `Tags` (duplicate keys: last-wins). |
| KubeMQ delivery → MQTT subscriber | KubeMQ `Tags` copied into `Properties.User` on injected PUBLISH. |

Caps (hardcoded):

| Cap | Value | Violation result |
|---|---|---|
| Maximum user properties per message | **32** | MQTT 5.0: PUBACK `0x97`; MQTT 3.1.1: silent drop + `publish.error` audit. |
| Maximum total bytes (sum of all key+value lengths) | **4096 bytes** | Same as above. |

---

## Gotcha Reference

The following connector behaviors surprise most users. Each is expanded in the relevant
guide or example README.

### Gotcha 1 — Retain Is Silently Dropped at Runtime {#gotcha-retain}

`RetainAvailable=0` is advertised to clients at CONNECT. If a client sends a retained
PUBLISH anyway:

- `mochi` silently **strips the retain flag**.
- The connector then **drops the message** (`CodeSuccessIgnore`) and audits
  `publish.error` with text `"retain not supported"`.
- The **PUBACK succeeds** (`0x00`). The sender has no indication that the message was
  dropped.
- No retained copy is stored; late subscribers receive nothing.

The **only** case that yields a CONNACK error is `Will-retain=true` at CONNECT, which
returns CONNACK `0x9A` (retain-not-supported). This is enforced before the session is
established.

### Gotcha 2 — Events-Store Has No Historical Replay {#gotcha-events-store}

Events-Store subscriptions over MQTT always use `StartNewOnly`. Unlike the REST/gRPC
API (which offers six replay positions), MQTT subscribers receive **only messages
published after the subscription is established**. Messages stored before a client
subscribes are never delivered.

### Gotcha 3 — `$share` Group Name Is Audit-Only; All Groups Compete in One Pool {#gotcha-share-pool}

Standard MQTT brokers deliver a separate copy of each message to each `$share` group.
The KubeMQ connector does **not**. All `$share` subscribers on all groups compete in
a **single KubeMQ queue pool**: each message is consumed exactly once.

### Gotcha 4 — RPC Requires MQTT 5.0 and PUBACK Is Immediate {#gotcha-rpc}

Publishing to `commands/` or `queries/` from an MQTT 3.1.1 client is silently dropped
(PUBACK `0x00`, no RPC issued). The PUBACK for a valid MQTT 5.0 RPC publish is also
sent **immediately**, before the response arrives. Clients must implement their own
response-wait timeout.

### Gotcha 5 — Literal `.` in a Topic Segment Conflates with `/` {#gotcha-dot}

`events/a.b/c` and `events/a/b/c` both produce KubeMQ channel `a.b.c`. Avoid dots
inside path segments.

### Gotcha 6 — Overlapping Wildcard Filters Deliver Multiple Copies {#gotcha-wildcards}

Each unique subscribe filter has its own independent bridge entry. A publish matching
N active wildcard filters results in N deliveries to the client. There is no
cross-entry deduplication.

---

See also: [Topic Grammar](topic-grammar.md) · [Reason Codes](reason-codes.md) ·
[Connections Endpoint](connections-endpoint.md)
