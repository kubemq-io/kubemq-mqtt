# python — Protocol: MQTT 3.1.1 vs 5.0

Contrasts MQTT 3.1.1 (protocol level 4) and MQTT 5.0 (protocol level 5) against the
KubeMQ MQTT connector, showing what v5 unlocks: User Properties (KubeMQ Tags),
shared-subscription queue consume, and RPC (Commands / Queries via ResponseTopic +
CorrelationData).

Checklist items: **S6.1.11** (`events/demo/v3`; `commands/demo/x`; tests #13
V311PubSub + #12 RPC311Dropped).

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` (from `examples/python/`) — requires `paho-mqtt>=2.1.0`
- KubeMQ server with the MQTT connector enabled (`CONNECTORSMQTT_ENABLE=true`, default)
- No external gRPC responder needed (Phase C demonstrates the *absence* of RPC delivery on v3.1.1)

## How to Run

```bash
cd examples/python
export KUBEMQ_MQTT_URL=tcp://localhost:1883   # default; also accepts ws:// tls://
python protocol/mqtt_v311_vs_v5/main.py
```

## Expected Output

Captured from a live run against `tcp://127.0.0.1:1883`. In Phase C the PUBACK
callback fires before the publishing print returns, so those two lines can appear
in either order.

```
=== MQTT 3.1.1 vs 5.0 — KubeMQ connector comparison ===
Broker  : tcp://127.0.0.1:1883
Topics  : events='events/demo/v3'  command='commands/demo/x'

--- Phase A: events pub/sub via MQTT 3.1.1 ---
[v3.1.1-sub] connected; subscribing to 'events/demo/v3' QoS 1
[v3.1.1-sub] SUBACK mid=1 granted_qos=[1]
[v3.1.1-pub] published to 'events/demo/v3'  (no user-properties)
[v3.1.1-sub] received on 'events/demo/v3'
             payload       : '{"proto": "3.1.1", "msg": "hello from v3.1.1"}'
             user-props    : False  (expected: False — v3.1.1 has no user-properties)

--- Phase B: events pub/sub via MQTT 5.0 (User Properties -> KubeMQ Tags) ---
[v5-sub] connected; subscribing to 'events/demo/v3' QoS 1
[v5-sub] SUBACK 0x01
[v5-pub] published to 'events/demo/v3'  user-props=[env=demo, version=5.0]
[v5-sub] received on 'events/demo/v3'
         payload    : '{"proto": "5.0", "msg": "hello from v5"}'
         user-props : [('env', 'demo'), ('version', '5.0')]  (forwarded as KubeMQ Tags)
[v5-sub] user-props verified (KubeMQ Tags round-trip OK)

--- Phase C: v3.1.1 publish to commands/demo/x (RPC silently dropped) ---
  topic   : commands/demo/x  (KubeMQ channel demo.x)
  gotcha  : v3.1.1 PUBLISH to commands/ is dropped; PUBACK is still 0x00.
  fix     : use MQTT 5.0 for RPC.
[v3.1.1-rpc] connected
[v3.1.1-rpc] PUBACK received for mid=1  (code=0x00 — broker accepted; command is SILENTLY DROPPED)
[v3.1.1-rpc] published to 'commands/demo/x'
[v3.1.1-rpc] no response received (expected: v3.1.1 RPC is silently dropped)
             The server audits publish.error but the MQTT client sees PUBACK 0x00.

=== Summary ===
  MQTT 3.1.1
    - basic events pub/sub: works
    - user-properties (KubeMQ Tags): NOT available (no wire support)
    - shared-subscription queue consume: NOT available (protocol lacks syntax)
    - RPC (commands/queries): SILENTLY DROPPED — PUBACK 0x00 but no delivery
  MQTT 5.0
    - basic events pub/sub: works
    - user-properties forwarded as KubeMQ Tags: YES (cap 32 props / 4096 bytes)
    - shared-subscription queue consume: YES ($share/<group>/queues/<ch> QoS>=1)
    - RPC via ResponseTopic + CorrelationData: YES (PUBACK immediate; await response separately)
    - fine-grained reason codes: 0x83 / 0x87 / 0x97 / 0x9A / 0xA2 …
Done.
```

## What's Happening

### Phase A — MQTT 3.1.1 events pub/sub

1. A `MQTTv311` subscriber connects and subscribes to `events/demo/v3` at QoS 1.
2. A `MQTTv311` publisher connects and publishes a message **without** user-properties
   (the protocol has no such field).
3. The connector routes the PUBLISH to the KubeMQ Events pattern on channel `demo.v3`
   (`/` becomes `.`).
4. The subscriber receives the message.  `msg.properties` is `None` — no user-props
   are delivered over a v3.1.1 connection.

### Phase B — MQTT 5.0 events pub/sub (User Properties)

1. A `MQTTv5` subscriber connects and subscribes to the same topic `events/demo/v3`.
2. A `MQTTv5` publisher attaches two MQTT 5.0 User Properties to the PUBLISH:
   `env=demo` and `version=5.0`.
3. The connector maps those User Properties to KubeMQ message Tags.
4. On delivery the connector maps the Tags back to MQTT 5.0 User Properties
   and delivers them to the v5 subscriber.

### Phase C — v3.1.1 RPC silently dropped (gotcha #4)

1. A `MQTTv311` client publishes to `commands/demo/x` (KubeMQ Commands pattern).
2. The broker returns **PUBACK `0x00`** (success) — no error signal.
3. The connector detects that the client is v3.1.1 and **silently drops** the command
   (a `publish.error` is audited server-side).
4. No response ever arrives.  **Always use MQTT 5.0 for RPC.**

## MQTT Specifics

| Aspect | Phase A (v3.1.1) | Phase B (v5) | Phase C (v3.1.1) |
|---|---|---|---|
| Topic | `events/demo/v3` | `events/demo/v3` | `commands/demo/x` |
| KubeMQ channel | `demo.v3` | `demo.v3` | `demo.x` |
| KubeMQ pattern | Events | Events | Commands |
| QoS | 1 | 1 | 1 |
| MQTT 5.0 User Properties | n/a (no wire support) | `env=demo`, `version=5.0` | n/a |
| User Properties as KubeMQ Tags | not delivered | forwarded bidirectionally | n/a |
| RPC ResponseTopic | not sent | not sent | not sent (dropped) |
| Outcome | message delivered | message + Tags delivered | PUBACK 0x00; message dropped |

## Protocol Acceptance

| Protocol | Level | Accepted by KubeMQ MQTT connector |
|---|---|---|
| MQTT 3.1 | 3 | **Rejected** — `MinProtocolVersion=4` (`CONNECTORSMQTT_CAPABILITIES_MIN_PROTOCOL_VERSION`) |
| MQTT 3.1.1 | 4 | Accepted |
| MQTT 5.0 | 5 | Accepted (default for all other examples) |

## Capability Comparison

| Capability | MQTT 3.1.1 | MQTT 5.0 |
|---|---|---|
| Basic events pub/sub | Yes | Yes |
| Events wildcards (`+`, `#`) | Yes — only on `events/` topics | Yes |
| Events-Store pub/sub | Yes (StartNewOnly; no historical replay) | Yes |
| User Properties / KubeMQ Tags | No wire support | Full bidirectional mapping |
| Queue consume (`$share/`) | No syntax in protocol | Yes (`$share/<group>/queues/<ch>` QoS>=1) |
| RPC (Commands / Queries) | Silently dropped | Yes — ResponseTopic + CorrelationData |
| Fine-grained reason codes | No — SUBACK = granted-QoS byte; no PUBACK reason code | Yes (0x83/0x87/0x97/0x9A/0xA2...) |
| `clean_start` / `clean_session` | `clean_session` | `clean_start` |

## Gotchas

**Gotcha #1 — retain silently dropped:**
A runtime PUBLISH with the retain flag set is **silently stripped** by the broker.
PUBACK returns `0x00` (success) but the message is never stored or delivered.
`publish.error` is audited server-side.  The ONLY way to trigger CONNACK `0x9A` is
setting Will-retain at CONNECT time.

**Gotcha #4 — RPC silently dropped on v3.1.1:**
A v3.1.1 PUBLISH to `commands/<ch>` or `queries/<ch>` returns PUBACK `0x00`
but the connector **drops the message** silently.  There is no indication of
failure on the wire.  Use MQTT 5.0 for any RPC interaction.

**Gotcha #4 (continued) — RPC PUBACK is immediate:**
Even on MQTT 5.0 the broker sends PUBACK **immediately on receipt** of the RPC
PUBLISH — NOT after the responder replies.  The client MUST implement its own
response-wait timeout (see `rpc/query_round_trip` and `rpc/command_round_trip`).

**Gotcha #2 — Events-Store is StartNewOnly:**
`store/<ch>` subscriptions over MQTT always use StartNewOnly.  There is no
historical replay; messages published before the subscription was created are
not delivered.

**Gotcha #3 — `$share` group name is audit-only:**
All `$share` groups compete in a **single** KubeMQ queue pool.  The group name
is used only for metrics/audit, not for isolation.  Different group names do NOT
produce per-group copies (unlike MQTT's spec intent for shared subscriptions).

## Related Examples

- [events/basic_pubsub](../../events/basic_pubsub/) — MQTT 5.0 events pub/sub with user-props (KubeMQ Tags)
- [rpc/query_round_trip](../../rpc/query_round_trip/) — Full MQTT 5.0 query RPC flow (ResponseTopic + CorrelationData)
- [rpc/command_round_trip](../../rpc/command_round_trip/) — Full MQTT 5.0 command RPC flow
- [queues/shared_consume](../../queues/shared_consume/) — MQTT 5.0 shared-subscription queue consume (`$share/`)
