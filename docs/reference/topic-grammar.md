# Topic Grammar

The KubeMQ MQTT connector maps every MQTT topic (on publish) and filter (on subscribe)
to one of five KubeMQ messaging patterns. The first segment of the topic string is the
**pattern prefix**; the remaining segments become the **KubeMQ channel**.

---

## Pattern Prefix Table

| MQTT topic / filter | KubeMQ pattern | Direction | Example topic | KubeMQ channel |
|---|---|---|---|---|
| `events/<ch>` | Events | publish + subscribe | `events/site1/temp` | `site1.temp` |
| `store/<ch>` | Events-Store | publish + subscribe (StartNewOnly) | `store/site1/temp` | `site1.temp` |
| `queues/<ch>` | Queues — produce only | publish | `queues/jobs/email` | `jobs.email` |
| `$share/<group>/queues/<ch>` | Queues — consume | subscribe (QoS >= 1) | `$share/g1/queues/jobs/email` | `jobs.email` |
| `commands/<ch>` | Commands (RPC send) | publish — MQTT 5.0 only | `commands/svc/reboot` | `svc.reboot` |
| `queries/<ch>` | Queries (RPC send) | publish — MQTT 5.0 only | `queries/svc/status` | `svc.status` |
| `$reply/<clientID>/<suffix>` | (mochi-local) | subscribe + RPC ResponseTopic | `$reply/c1/inbox` | not routed |
| *(no prefix)* | DefaultPattern | publish + subscribe | `site1/temp` | `site1.temp` |

A prefixless topic is routed to `DefaultPattern` (`events` by default). When
`DefaultPattern=none` the connector rejects the message: PUBACK `0x90` on publish,
SUBACK `0x8F` on subscribe.

---

## Path-Separator Mapping: `/` Becomes `.`

Topic segments are joined with `.` to form the KubeMQ channel:

```
events/site1/factory/temp  →  channel: site1.factory.temp
store/orders/europe        →  channel: orders.europe
queues/jobs/email          →  channel: jobs.email
```

The connector calls `strings.Split(topic, "/")` on the non-prefix segments and then
`strings.Join(segs, ".")`.

### Gotcha — Literal `.` in a Segment Conflates with `/` {#gotcha-dot-conflation}

A literal `.` inside a segment passes through unchanged, making the channel
indistinguishable from one produced by an extra `/`:

```
events/a.b/c   →  channel: a.b.c
events/a/b/c   →  channel: a.b.c   ← same result
```

These two topics map to **identical** KubeMQ channels. This is a **lossy round-trip**:
`ToTopic` converts `.` back to `/`, so the injected delivery topic is `events/a/b/c`
regardless of which form was published. Avoid dots inside individual path segments to
prevent ambiguity.

---

## Wildcard Subscriptions

MQTT wildcards are translated to KubeMQ wildcard syntax **only on Events subscriptions**.

| MQTT wildcard | KubeMQ equivalent | Position rule |
|---|---|---|
| `+` | `*` (single-level) | any segment |
| `#` | `>` (multi-level) | final segment only |

Examples:

| MQTT subscribe filter | KubeMQ channel filter | Matches |
|---|---|---|
| `events/+/temp` | `*.temp` | `events/site1/temp`, `events/site2/temp` |
| `events/site1/#` | `site1.>` | `events/site1/temp`, `events/site1/a/b` |
| `events/#` | `>` | all events channels |

**Non-Events wildcards are rejected**: subscribing to `queues/+/foo` or `store/#` returns
SUBACK `0xA2` (wildcard-subscriptions-not-supported).

### Gotcha — Overlapping Wildcard Filters Deliver Multiple Copies {#gotcha-overlapping-wildcards}

Each distinct subscribe filter creates an independent bridge registry entry (one array
subscription per `(pattern, filter)` pair). A publish that matches **N** overlapping
filters fires **N** deliveries — one per matching entry. There is **no cross-entry
deduplication** in the connector.

Example: a client subscribes to both `events/#` and `events/site1/#`; a publish to
`events/site1/temp` matches both entries and the client receives **two** copies.

---

## Shared Subscriptions — Queue Consume

Queue consumption uses MQTT 5.0 shared subscriptions:

```
$share/<group>/queues/<channel>
```

Rules:

| Condition | Result |
|---|---|
| `$share/<group>/queues/<ch>` at QoS >= 1 | SUBACK `0x00` / `0x01` (granted) |
| `$share/<group>/queues/<ch>` at QoS 0 | SUBACK `0x83` (impl-specific error) |
| Plain `queues/<ch>` subscribe (no `$share`) | SUBACK `0x83` |
| `$share/<group>` on any non-queues prefix | SUBACK `0x83` |
| Empty group: `$share//queues/<ch>` | SUBACK `0x8F` (invalid) |

**QoS 2 is downgraded to QoS 1**: the queue bridge operates at QoS 1
(PUBACK = acknowledge; no QoS 2 two-phase commit in the bridge).

### Gotcha — The `$share` Group Name Is Audit-Only; All Groups Compete in One Pool {#gotcha-share-pool}

The KubeMQ queue is a **single shared pool**. Unlike standard MQTT brokers — which deliver
a separate copy per `$share` group — the KubeMQ MQTT connector routes **all** `$share`
group subscribers into one competing consumer pool. Every message is consumed exactly once
across all subscribers regardless of group name. The group name is recorded in audit and
metrics only.

---

## RPC Topics — MQTT 5.0 Only

Commands and Queries are **publish-only** patterns over MQTT:

```
commands/<channel>   # send a command RPC
queries/<channel>    # send a query RPC
```

### Gotcha — RPC Requires MQTT 5.0; v3.1.1 Publish Is Silently Dropped {#gotcha-rpc-v5-only}

A v3.1.1 (or earlier) client publishing to `commands/` or `queries/` receives a
**success PUBACK** (`0x00`), but the message is **silently dropped** and a
`publish.error` audit event is emitted. No RPC is issued.

MQTT 5.0 is required because:
- The `ResponseTopic` property carries the reply address.
- The `CorrelationData` property associates the response.
- User Properties carry RPC metadata.

### RPC Subscribe Not Allowed for MQTT Clients

MQTT clients **cannot** subscribe to `commands/<ch>` or `queries/<ch>` as responders.
These subscriptions return SUBACK `0x83`. RPC responders must be implemented on the
gRPC side using the KubeMQ Go SDK or another KubeMQ client.

### Reply Namespace

```
$reply/<own-clientID>/<suffix>
```

- A client must **subscribe** to its own `$reply/<clientID>/<suffix>` before issuing an
  RPC publish.
- The `ResponseTopic` property of the RPC PUBLISH **must** point to the client's own
  `$reply/<clientID>/...` namespace. Using another client's `$reply` namespace returns
  PUBACK `0x83`.
- `$reply` topics are **mochi-local** — they are never routed to the KubeMQ array.

### Gotcha — PUBACK Is Sent Immediately; The Client Must Implement Its Own Timeout {#gotcha-rpc-immediate-puback}

The PUBACK for an RPC PUBLISH is sent **immediately on receipt**, before the response
arrives. The response is later injected as a separate PUBLISH on the `$reply` topic.
The client must implement its own timeout to detect missing responses.
`RpcTimeoutSeconds` (default 30 s) is the server-side bound; after that the server
audits `rpc.timeout` and discards the pending entry, but the client receives no
notification.

---

## Empty Segments and Structural Errors

The connector calls `splitChecked`, which rejects topics with empty segments
(leading slash, trailing slash, or double slash):

| Offending topic | Error | Wire code |
|---|---|---|
| `/events/foo` | empty segment (leading `/`) | PUBACK `0x90` / SUBACK `0x8F` |
| `events//foo` | empty segment (double `/`) | PUBACK `0x90` / SUBACK `0x8F` |
| `events/foo/` | empty segment (trailing `/`) | PUBACK `0x90` / SUBACK `0x8F` |
| `events/` (prefix only, no channel) | empty channel | PUBACK `0x90` / SUBACK `0x8F` |

---

## Reverse Mapping: Delivery Topics

When the connector injects an array delivery back to subscribing MQTT clients it uses
`ToTopic(pattern, channel)`, which replaces every `.` with `/` and prepends the pattern
prefix:

```
channel: site1.factory.temp  +  pattern: events  →  events/site1/factory/temp
channel: jobs.email          +  pattern: queues  →  queues/jobs/email
```

For wildcard subscriptions the injected topic is the **concrete per-message channel**
(e.g. `events/site1/temp`), not the wildcard filter (e.g. `events/+/temp`).
For exact subscriptions the injected topic is the original filter string.

---

## Summary

```
MQTT publish/subscribe topic
        │
        ├── starts with "events/"   → PatternEvents
        ├── starts with "store/"    → PatternEventsStore
        ├── starts with "queues/"   → PatternQueues (produce)
        ├── starts with "commands/" → PatternCommands (MQTT 5.0 only)
        ├── starts with "queries/"  → PatternQueries  (MQTT 5.0 only)
        ├── starts with "$share/<g>/queues/"
        │                           → PatternQueues (consume, QoS ≥ 1)
        ├── starts with "$reply/<own-clientID>/"
        │                           → PatternLocal (mochi-local; no array)
        └── no prefix               → DefaultPattern (default: events)
                                      DefaultPattern=none → rejected
```

See also: [Capabilities](capabilities.md) · [Reason Codes](reason-codes.md) ·
[Events examples](../../examples/go/events/) ·
[Queues examples](../../examples/go/queues/) ·
[RPC examples](../../examples/go/rpc/)
