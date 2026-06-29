# JavaScript/TypeScript — Protocol: MQTT 3.1.1 vs 5.0

Didactic contrast of MQTT 3.1.1 (protocol level 4) and MQTT 5.0 (level 5) over the KubeMQ MQTT connector.
MQTT 5.0 unlocks three features that are entirely absent in v3.1.1: **User Properties** (mapped to/from KubeMQ Tags), **shared subscriptions** (`$share/...`) required for Queue consume, and **RPC** (Commands / Queries via `ResponseTopic` + `CorrelationData`).

## Prerequisites

- Node.js 18+
- `npm install` run once inside `examples/javascript/`
- KubeMQ server with MQTT connector enabled (default port 1883)
- **Part 2 only** — a gRPC command responder running on channel `demo.cmd`:
  ```bash
  cd .work/test-harness/responder
  go run . -channel demo.cmd -type command
  ```
  If no responder is running, Part 2 times out gracefully and the example continues
  to Part 3 (the v3.1.1 contrast is demonstrated either way).

## How to Run

```bash
cd examples/javascript
npm install
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883   # optional; this is the default
npx tsx protocol/mqtt-v311-vs-v5/index.ts
```

## Expected Output

With a gRPC command responder running on channel `demo.cmd`:

```
=== MQTT v3.1.1 vs v5 — feature contrast ===
broker: tcp://127.0.0.1:1883

v5 unlocks: User-Properties (KubeMQ Tags), $share queue consume, RPC
v3.1.1 has: basic pub/sub only (Events/EventsStore/Queues produce)

--- Part 1: Events pub/sub (both v3.1.1 and v5 work) ---
[v5 sub]    connected (MQTT 5.0)
[v3.1.1 sub] connected (MQTT 3.1.1)
[subs]       both subscribed to events/demo/v3  QoS 1
[v5 pub]     connected (MQTT 5.0)
[v5 pub]     published to events/demo/v3 QoS 1  user-props={k1:v1, proto:v5}

[v5 sub]    payload: {"msg":"hello from v5 publisher"}
[v5 sub]    user-props (KubeMQ tags): {"k1":"v1","proto":"v5"}
[v3.1.1 sub] payload: {"msg":"hello from v5 publisher"}
[v3.1.1 sub] user-props: null  (v3.1.1 wire has no user-properties)

[result] User-Properties: v5 = YES | v3.1.1 = NO (nothing carried, not degraded)

--- Part 2: RPC command round-trip (MQTT 5.0 only; requires gRPC responder) ---
[v5 rpc]  connected (MQTT 5.0)
[v5 rpc]  subscribed to reply inbox: $reply/js-v311v5-rpc5/inbox
[v5 rpc]  command published to commands/demo/cmd (PUBACK received immediately)
[v5 rpc]  waiting for response...
[v5 rpc]  response received on $reply/js-v311v5-rpc5/inbox
[v5 rpc]    CorrelationData echoed: contrast-corr-1
[v5 rpc]    body: (empty — expected for commands)
[v5 rpc]    kubemq-executed: true
[v5 rpc]    kubemq-error: (none)
[result] RPC: v5 = SUPPORTED (ResponseTopic + CorrelationData + immediate PUBACK)

--- Part 3: v3.1.1 RPC — silently dropped (test #12 RPC311Dropped) ---
[v3.1.1 rpc] connected (MQTT 3.1.1)
[v3.1.1 rpc] PUBACK received (0x00) — connector acknowledged but SILENTLY DROPPED
[v3.1.1 rpc] confirmed: NO response received (publish.error audited server-side)
[result] RPC: v3.1.1 = SILENTLY DROPPED (PUBACK 0x00, no routing, no response)

=== Summary ===
Feature                         | MQTT 3.1.1 | MQTT 5.0
--------------------------------|------------|--------------------
Events pub/sub                  | YES        | YES
Events-Store pub/sub            | YES        | YES
Queues produce                  | YES        | YES
Queues consume ($share, QoS>=1) | NO         | YES
User-Properties / KubeMQ Tags   | NO         | YES (32 props/4096B)
RPC — Commands / Queries        | NO (silent drop) | YES
ResponseTopic + CorrelationData | NO         | YES
[done]
```

## What's Happening

### Part 1 — Events pub/sub with both protocol versions

1. A v5 subscriber and a v3.1.1 subscriber both connect and subscribe to `events/demo/v3` at QoS 1.
2. A v5 publisher sends a message with MQTT 5.0 User Properties `{k1: "v1", proto: "v5"}`.
3. The broker delivers the message to both subscribers.
4. The **v5 subscriber** sees the full User Properties (which are KubeMQ Tags, echoed bidirectionally).
5. The **v3.1.1 subscriber** sees the same payload but `userProps: null` — the v3.1.1 wire protocol has
   no concept of User Properties, so nothing is carried in either direction.

### Part 2 — v5 RPC command round-trip (positive case)

1. The v5 client subscribes to its own reply inbox `$reply/js-v311v5-rpc5/inbox` at QoS 1 **before** publishing.
2. The client publishes to `commands/demo/cmd` with `Properties.ResponseTopic` pointing to the reply inbox
   and `Properties.CorrelationData = "contrast-corr-1"`.
3. **PUBACK arrives immediately** on receipt of the PUBLISH — NOT after the gRPC responder replies.
   The client waits on `responsePromise` independently (client-side timeout = 15 s).
4. The gRPC responder processes the command and the KubeMQ connector routes the response to the reply inbox.
5. The response has **no body** (commands carry no body); outcome is in user-props:
   `kubemq-executed: "true"` (success) or `"false"` + `kubemq-error: "<msg>"` (failure).
6. `CorrelationData` is echoed so the client can correlate request to response.

### Part 3 — v3.1.1 RPC silent drop (test #12)

1. A v3.1.1 client publishes to `commands/demo/x`.
2. The connector sends PUBACK 0x00 — the packet is acknowledged at the wire level.
3. BUT the message is **silently dropped**: not routed to KubeMQ Commands, `publish.error` audited
   server-side, no response ever dispatched.  This is intentional — RPC requires MQTT 5.0 properties
   (`ResponseTopic`, `CorrelationData`) which v3.1.1 cannot carry.
4. The client waits 2 s and confirms no response arrived.

## MQTT Specifics

| Property | Value |
|----------|-------|
| Events topic (Parts 1) | `events/demo/v3` — KubeMQ channel `demo.v3` |
| v3.1.1 protocol level | 4 (`protocolVersion: 4` in MQTT.js) |
| v5 protocol level | 5 (`protocolVersion: 5` in MQTT.js) |
| QoS (all operations) | 1 |
| v5 User Properties (Part 1) | `{k1: "v1", proto: "v5"}` — round-trip as KubeMQ Tags |
| User-Properties cap | 32 properties / 4096 bytes total; exceeding -> PUBACK `0x97` |
| v5 reply topic (Part 2) | `$reply/js-v311v5-rpc5/inbox` (own `$reply/<clientID>/...` namespace) |
| v5 command topic (Part 2) | `commands/demo/cmd` — KubeMQ channel `demo.cmd` |
| `Properties.ResponseTopic` | `$reply/js-v311v5-rpc5/inbox` |
| `Properties.CorrelationData` | `"contrast-corr-1"` |
| v3.1.1 RPC drop topic (Part 3) | `commands/demo/x` — KubeMQ channel `demo.x` |
| Queue consume (v5 only) | `$share/<group>/queues/<ch>` at QoS >= 1 |

## Gotchas

**Gotcha #4 — RPC is MQTT 5.0 only; PUBACK is immediate.**
A v3.1.1 `PUBLISH` to `commands/<ch>` or `queries/<ch>` receives PUBACK 0x00 but is **silently
dropped** by the connector — no error visible to the client, no response ever arrives.
For v5 RPC, PUBACK arrives immediately on receipt of the PUBLISH, **not** after the gRPC responder
replies.  The client must implement a response-wait timeout independently.

**Gotcha #1 — Retain is silently stripped.**
Setting `retain: true` on a PUBLISH returns PUBACK 0x00 but the message is **never delivered and not
stored**.  Always use `retain: false`.

**Queue consume requires v5 + `$share/...` + QoS >= 1.**
A v3.1.1 client cannot use shared subscriptions.  A plain `queues/<ch>` subscribe (without `$share/`)
returns SUBACK `0x83` regardless of protocol version.  A QoS 0 `$share/...` subscribe also returns
`0x83`.

**User-Properties are NOT silently degraded on v3.1.1.**
There is no fallback: v3.1.1 carries zero user properties in either direction.  Tags from KubeMQ are
never surfaced to a v3.1.1 subscriber.

## Related Examples

- [rpc/command-round-trip](../../rpc/command-round-trip/) — full v5 RPC command flow in isolation
- [rpc/query-round-trip](../../rpc/query-round-trip/) — v5 RPC query flow (response includes body)
- [queues/shared-consume](../../queues/shared-consume/) — `$share/...` queue consume (v5 only)
- [events/basic-pubsub](../../events/basic-pubsub/) — v5 User Properties / KubeMQ Tags in detail
