# Reason Codes

This page lists every MQTT reason code the KubeMQ MQTT connector produces, the packet
type it appears on, and the exact conditions that trigger it.

MQTT 3.1.1 does not carry reason codes on PUBACK or SUBACK (the fields do not exist).
The codes below are MQTT 5.0 reason codes. For MQTT 3.1.1 connections the connector
either drops the message silently or closes the connection without a reason code.

---

## Full Reason Code Table

| Code | Hex | Packet(s) | Meaning | Triggering conditions |
|---|---|---|---|---|
| Success | `0x00` | PUBACK, SUBACK, CONNACK | Operation succeeded | Normal success. Also returned when a retained PUBLISH is **silently dropped** — the PUBACK still succeeds even though the message was discarded (see [Gotcha: Retain Silently Dropped](#0x00-retain-gotcha)). |
| Granted QoS 1 | `0x01` | SUBACK | Subscription granted at QoS 1 | The normal success code for any QoS-1 subscribe (Events, Events-Store, or `$share` Queues). Also the granted code when a QoS-2 subscribe is downgraded to QoS 1 — e.g. a `$share/<group>/queues/<ch>` subscription requested at QoS 2. |
| Unspecified error | `0x80` | PUBACK, SUBACK | Broker not ready or array error | (1) Broker is starting up or shutting down (`acceptTraffic=false`): all incoming publishes and subscribes are rejected with this code until the broker signals ready. (2) KubeMQ array (internal message bus) returned an error for a publish routed to Events, Events-Store, or Queues. No reason string is included in the PUBACK (information boundary — clients receive the code only; the actual error is in audit + logs). |
| Implementation-specific error | `0x83` | SUBACK, PUBACK | Valid request, not supported by this connector | **SUBACK** — subscription rejected for a policy reason: (a) `$share/<group>` on a non-queues topic; (b) plain `queues/<ch>` subscribe without `$share`; (c) `$share/<group>/queues/<ch>` at QoS 0; (d) subscribe to `commands/<ch>` or `queries/<ch>` (MQTT clients cannot be RPC responders); (e) subscribe to another client's `$reply/<other-clientID>/...` namespace. **PUBACK** — RPC publish rejected: (f) `ResponseTopic` is missing; (g) `ResponseTopic` is outside the publisher's own `$reply/<own-clientID>/...` namespace. |
| Bad user name or password | `0x86` | CONNACK | Authentication failure | (a) Auth is enabled and the CONNECT `Password` field is empty. (b) Auth is enabled and the JWT in `Password` is invalid or expired. The `Username` field is not used for authentication (display-only). |
| Not authorized | `0x87` | PUBACK, SUBACK | ACL denied the publish or subscribe | The authorization policy denied access to the resolved (pattern, channel) for this client. **PUBACK** — write (publish) was denied. **SUBACK** — read (subscribe) was denied (`mochi` `server.go:1259` returns `ErrNotAuthorized.Code`). The resolved channel and pattern are checked but not included in the ack (information boundary). |
| Server shutting down | `0x8B` | DISCONNECT | Graceful server shutdown | The KubeMQ node is stopping. The connector sends DISCONNECT `0x8B` to all connected clients before closing. Clients should reconnect to another node or retry. |
| Topic filter invalid | `0x8F` | SUBACK | Topic filter is structurally malformed | (a) Any segment of the topic filter is empty (leading `/`, trailing `/`, or double `/`). (b) `DefaultPattern=none` and the filter has no recognized prefix — the filter cannot be mapped to any pattern. (c) Empty `$share` group: `$share//queues/<ch>`. |
| Topic name invalid | `0x90` | PUBACK | Topic name is structurally malformed or unmappable | (a) Any segment of the topic is empty. (b) `DefaultPattern=none` and the topic has no recognized prefix — the message cannot be routed. (c) The topic resolves to an empty KubeMQ channel. |
| Quota exceeded | `0x97` | PUBACK | Server-side quota exhausted | (a) The in-flight RPC pending map is full: `RpcMaxPending` (default `1024`) concurrent outstanding RPC requests already exist. Retry after existing requests complete. (b) The PUBLISH carries more than **32** MQTT 5.0 user properties, or the total byte length of all user-property key+value pairs exceeds **4096 bytes**. |
| Retain not supported | `0x9A` | CONNACK | Retain is disabled; Will-retain requested | The CONNECT packet includes `Will-retain=true`. The connector rejects the connection before the session is established. Note: a retained PUBLISH **at runtime** does **not** produce this code — it is silently dropped with a success PUBACK `0x00` (see code `0x00` above). |
| Wildcard subscriptions not supported | `0xA2` | SUBACK | Wildcard used on an unsupported pattern | A subscription filter contains `+` or `#` but the resolved pattern is **not** Events. Wildcards are allowed only on Events (`events/<ch>`) subscriptions. Attempting `queues/+/foo`, `store/#`, `commands/+/svc`, etc. triggers this code. |

---

## Per-Pattern Quick Reference

### Events (`events/<ch>`)

| Scenario | Code |
|---|---|
| Publish success | `0x00` |
| Subscribe success | `0x00` / `0x01` / `0x02` (granted QoS) |
| Wildcard subscribe (`+`, `#`) success | `0x00` / `0x01` |
| Array error on publish | `0x80` |
| Broker not ready | `0x80` |
| ACL deny on publish | `0x87` (PUBACK) |
| ACL deny on subscribe | `0x87` (SUBACK) |
| Empty channel (`events/` only) | `0x90` (publish) / `0x8F` (subscribe) |

### Events-Store (`store/<ch>`)

Same as Events except wildcard subscribes return `0xA2` (wildcards not allowed on
Events-Store).

### Queues (`queues/<ch>` produce / `$share/<g>/queues/<ch>` consume)

| Scenario | Code |
|---|---|
| Produce (publish) success | `0x00` |
| `$share/<g>/queues/<ch>` subscribe at QoS 1 | `0x01` |
| `$share/<g>/queues/<ch>` subscribe at QoS 2 (downgraded) | `0x01` |
| Plain `queues/<ch>` subscribe | `0x83` |
| `$share/<g>/queues/<ch>` at QoS 0 | `0x83` |
| `$share/<g>` on non-queues topic | `0x83` |
| Array error on produce | `0x80` |
| ACL deny on produce | `0x87` (PUBACK) |
| ACL deny on `$share/<g>/queues/<ch>` subscribe | `0x87` (SUBACK) |

### Commands and Queries (RPC, MQTT 5.0 only)

| Scenario | Code |
|---|---|
| Valid MQTT 5.0 RPC publish (acknowledged immediately) | `0x00` (PUBACK sent before response) |
| Subscribe to `commands/<ch>` or `queries/<ch>` | `0x83` (MQTT clients are not responders) |
| MQTT 3.1.1 publish to `commands/` or `queries/` | `0x00` (silently dropped — no RPC) |
| Missing `ResponseTopic` property | `0x83` |
| `ResponseTopic` outside own `$reply/<clientID>/` | `0x83` |
| `RpcMaxPending` exceeded | `0x97` |

### RPC Timeout

There is **no reason code** for an RPC timeout. The server audits `rpc.timeout` and
discards the pending entry, but the PUBACK was already sent at `0x00` when the publish
was received. The client must detect the missing response via its own timeout.

---

## Code `0x00` — The Retain Silent-Drop Gotcha {#0x00-retain-gotcha}

`RetainAvailable=0` is advertised to connecting clients. If a client ignores this and
sends a retained PUBLISH:

1. `mochi` silently strips the retain flag.
2. The bridge detects the retain flag and drops the message without routing it to the
   KubeMQ array.
3. The bridge audits `publish.error` with description `"retain not supported"`.
4. The **PUBACK is `0x00` (success)**. The sender has no wire-level indication that
   the message was dropped.

This is the only case where `0x00` does not mean the message was delivered.

The **only** retain-related error code is `0x9A`, and it fires only on CONNECT when
the Will message has `retain=true`.

---

## MQTT 3.1.1 Behavior

MQTT 3.1.1 does not support reason codes on PUBACK or SUBACK. The connector handles
this as follows:

| Situation | v5 code | v3.1.1 behavior |
|---|---|---|
| Publish success | `0x00` | Normal PUBACK (no reason code field) |
| Broker not ready | `0x80` | Message silently dropped; no wire indication |
| Array error | `0x80` | Message silently dropped |
| Retain publish | `0x00` (silent drop) | Message silently dropped |
| RPC publish to commands/queries | `0x00` (silent drop) | Message silently dropped |
| User-prop cap exceeded | `0x97` | Message silently dropped |
| Wildcard on non-events | `0xA2` | Subscription silently rejected |
| Unsupported subscribe pattern | `0x83` | Subscription silently rejected |
| `$share` queue at QoS 0 | `0x83` | Subscription silently rejected |

All rejections for v3.1.1 connections are audited as `publish.error` or
`subscription.error` in the server audit log.

---

## Reason Codes in Order

| Hex | Decimal | Packet | Name |
|---|---|---|---|
| `0x00` | 0 | PUBACK / SUBACK / CONNACK | Success |
| `0x01` | 1 | SUBACK | Granted QoS 1 |
| `0x80` | 128 | PUBACK / SUBACK | Unspecified error |
| `0x83` | 131 | PUBACK / SUBACK | Implementation-specific error |
| `0x86` | 134 | CONNACK | Bad user name or password |
| `0x87` | 135 | PUBACK / SUBACK | Not authorized |
| `0x8B` | 139 | DISCONNECT | Server shutting down |
| `0x8F` | 143 | SUBACK | Topic filter invalid |
| `0x90` | 144 | PUBACK | Topic name invalid |
| `0x97` | 151 | PUBACK | Quota exceeded |
| `0x9A` | 154 | CONNACK | Retain not supported |
| `0xA2` | 162 | SUBACK | Wildcard subscriptions not supported |

---

See also: [Topic Grammar](topic-grammar.md) · [Capabilities](capabilities.md) ·
[Connections Endpoint](connections-endpoint.md) ·
[Events examples](../../examples/go/events/) ·
[Queues examples](../../examples/go/queues/) ·
[RPC examples](../../examples/go/rpc/)
