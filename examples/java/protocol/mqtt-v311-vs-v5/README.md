# java — Protocol: MQTT 3.1.1 vs 5.0

Side-by-side comparison of MQTT 3.1.1 and MQTT 5.0 against the KubeMQ MQTT connector.
Shows exactly what each protocol level can and cannot do: basic events work in both;
User Properties (KubeMQ Tags), shared queue subscriptions, and RPC are v5-only.

Both Paho libraries are used:
- `org.eclipse.paho.client.mqttv3` for MQTT 3.1.1 (protocol level 4)
- `org.eclipse.paho.mqttv5.client` for MQTT 5.0 (protocol level 5)

## Prerequisites

- Java 21+, Maven 3.8+
- KubeMQ server with the MQTT connector enabled
- For Section B3 (RPC round-trip): a gRPC-side command responder on channel `demo.cmd`
  type `command` (start via `.work/test-harness/responder/` in this repo)

## How to Run

```bash
export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883

# Optional: start the gRPC responder for the RPC section (B3)
# cd .work/test-harness/responder && go run . -channel demo.cmd -type command &

cd examples/java/protocol/mqtt-v311-vs-v5
mvn compile exec:java
```

## Expected Output

> Regenerated from a real run in the LiveTest stage (gRPC responder on channel
> `demo.cmd` type `command` running). Topic strings shown below are canonical
> (`events/demo/v3`, `commands/demo/x` for the v3.1.1 drop, `commands/demo/cmd`
> for the v5 RPC contrast). The `<id>`/`<uuid>` parts are per-run unique.

```
=== protocol/mqtt-v311-vs-v5 ===
Broker: tcp://127.0.0.1:1883

--- SECTION A: MQTT 3.1.1 (org.eclipse.paho.client.mqttv3) ---

[A1] Events pub/sub over MQTT 3.1.1
    Topic   : events/demo/v3
    Channel : demo.v3  (MQTT '/' -> KubeMQ '.')
    Connecting v3.1.1 subscriber ...
    v3.1.1 subscriber connected (protocol level 4)
    Subscribed to 'events/demo/v3' at QoS 1
    Connecting v3.1.1 publisher ...
    v3.1.1 publisher connected
    Publishing 'hello-from-v311' to 'events/demo/v3' QoS=1 ...
    Published OK (no User Properties — v3.1.1 limitation)
    [A1] Message received:
         topic   = events/demo/v3
         payload = hello-from-v311
         QoS     = 1
         User Properties: N/A (MQTT 3.1.1 has no user properties)
    [A1] Events pub/sub PASSED (v3.1.1)

[A2] RPC attempt over MQTT 3.1.1 (should be silently dropped)
    Command topic : commands/demo/x
    NOTE: v3.1.1 has no ResponseTopic/CorrelationData properties.
          The connector SILENTLY DROPS commands/queries published over v3.1.1.
    Publishing command to 'commands/demo/x' via v3.1.1 ...
    PUBACK received (code 0x00 = success at wire level)
    Waiting 3 s for a reply that will NOT arrive ...
    [A2] No reply received (EXPECTED — v3.1.1 RPC is silently dropped)
         To use RPC, upgrade the client to MQTT 5.0 (see Section B).

--- SECTION B: MQTT 5.0 (org.eclipse.paho.mqttv5.client) ---

[B1] Events pub/sub with User Properties (KubeMQ Tags) over MQTT 5.0
    Topic   : events/demo/v3
    Channel : demo.v3
    User Properties -> KubeMQ Tags (v5 only; cap 32 props / 4096 B)
    v5 subscriber connected to tcp://127.0.0.1:1883
    Subscribed; SUBACK granted QoS=1
    v5-events-pub connected to tcp://127.0.0.1:1883
    Publishing with User Properties: protocol=v5, source=java-example ...
    PUBACK 0x00 (success)
    [B1] Message received:
         topic   = events/demo/v3
         payload = hello-from-v5
         QoS     = 1
         User Properties (KubeMQ Tags):
           protocol = v5
           source = java-example
           variant = protocol/mqtt-v311-vs-v5
    [B1] Events pub/sub with User Properties PASSED (v5)

[B2] Queues shared subscription over MQTT 5.0
    Producer topic : queues/demo/proto-q
    Consumer topic : $share/g1/queues/demo/proto-q (shared, QoS 1)
    NOTE: $share group name is audit-only — ALL groups share ONE pool.
    Testing plain 'queues/demo/proto-q' subscribe (expect SUBACK 0x83) ...
    v5 consumer-1 connected
    v5-q-reject connected to tcp://127.0.0.1:1883
    SUBACK 0x83 received for plain queues/... (EXPECTED — use $share prefix)
    $share consumer-1 subscribed; granted QoS=1
    v5-q-producer connected to tcp://127.0.0.1:1883
    [B2] produced: task-1
    [B2] produced: task-2
    [B2] consumer-1 received: task-1 (Paho auto-acks on PUBACK after this callback)
    [B2] consumer-1 received: task-2 (Paho auto-acks on PUBACK after this callback)
    [B2] Shared queue consume PASSED (v5)

[B3] RPC command round-trip over MQTT 5.0
    Command topic  : commands/demo/cmd  -> KubeMQ channel demo.cmd
    Reply inbox    : $reply/v5-rpc-<id>/rq1
    CorrelationData: corr-<uuid>
    NOTE: PUBACK is immediate (on receipt); response arrives separately.
          Command response: no body + kubemq-executed user-prop.
    Requires gRPC responder on channel demo.cmd (type command).
    [B3] Step 1: subscribing reply inbox BEFORE publishing command ...
    v5 RPC client connected to tcp://127.0.0.1:1883
    Reply inbox subscribed; SUBACK granted QoS=1
    [B3] Step 2: publishing command to 'commands/demo/cmd' ...
    [B3] Step 3: PUBACK received: 0x00 (immediate, success — response comes separately)
    [B3] Step 4: waiting for response on reply inbox (up to 10 s) ...
    [B3] Response received on: $reply/v5-rpc-<id>/rq1
         body    : '' (command response has no body)
         user-prop: kubemq-executed = true
         CorrelationData echoed: corr-<uuid> (matches)
    [B3] kubemq-executed = true
    [B3] RPC command round-trip PASSED (v5)

=== Feature comparison summary ===
Feature                          v3.1.1        v5.0
-------                          ------        ----
Events pub/sub                   YES           YES
User Properties (KubeMQ Tags)    NO            YES (32 props / 4096 B cap)
RPC (commands/queries)           DROPPED       YES (ResponseTopic + CorrelationData)
Shared queue consume             NO            YES ($share/<g>/queues/<ch>, QoS>=1)
Detailed SUBACK/PUBACK codes     NO (0/0x80)   YES (0x83 / 0x97 / 0xA2 etc.)
Will-Retain                      YES           Rejected (0x9A) - broker-side
MQTT 3.1 (level 3)               N/A           N/A (rejected by KubeMQ)
```

If no gRPC responder is running, Section B3 will time out after 10 s with:
```
[B3] No response within 10 s.
     If no gRPC responder is running on channel demo.cmd, this is expected.
```
The PUBACK 0x00 still confirms the command was accepted at the broker level.

## What's Happening

### Section A — MQTT 3.1.1

**A1 — Events pub/sub**
A subscriber client connects at protocol level 4 and subscribes to `events/demo/v3`
at QoS 1. A publisher connects and publishes a message. KubeMQ routes it through the
Events pattern (fire-and-forget). Because v3.1.1 has no User Properties, no KubeMQ Tags
are forwarded.

**A2 — RPC attempt (silently dropped)**
The client publishes to `commands/demo/x`. The PUBACK returns 0x00 at the wire level,
but the connector silently discards the message: MQTT 3.1.1 has no ResponseTopic or
CorrelationData properties, which are required for KubeMQ RPC dispatch. No reply ever
arrives on the fake reply topic.

### Section B — MQTT 5.0

**B1 — Events with User Properties**
The publisher targets the same `events/demo/v3` channel and attaches three User
Properties (`protocol`, `source`, `variant`). The connector maps these 1:1 to KubeMQ
Tags, which are forwarded back to the subscriber as User Properties on the received
PUBLISH — the v5-exclusive capability the v3.1.1 A1 case cannot provide.

**B2 — Queues shared subscription**
First, a plain `queues/demo/proto-q` subscribe is attempted — the broker rejects it
with SUBACK 0x83 (implementation-specific: plain queue subscribe is not supported).
Then the correct `$share/g1/queues/demo/proto-q` form at QoS 1 is used and accepted.
Two messages are produced and consumed. The Paho library auto-sends PUBACK after the
callback returns, which acknowledges each message.

**B3 — RPC command round-trip**
1. The reply inbox `$reply/<clientId>/rq1` is subscribed **before** publishing.
2. The command is published to `commands/demo/cmd` (KubeMQ channel `demo.cmd`) with
   `ResponseTopic` and `CorrelationData` in MQTT 5.0 Properties.
3. PUBACK 0x00 arrives **immediately** on receipt — not after the response.
4. The gRPC responder processes the command and delivers the response to `$reply/...`.
5. The response has no body; the `kubemq-executed` user property carries the result.
6. CorrelationData is echoed on the response for request correlation.

## MQTT Specifics

| Property | Section A (v3.1.1) | Section B (v5.0) |
|---|---|---|
| Topic (events) | `events/demo/v3` | `events/demo/v3` |
| Topic (command) | `commands/demo/x` (silently dropped) | `commands/demo/cmd` |
| Topic (queue produce) | — | `queues/demo/proto-q` |
| Topic (queue consume) | — | `$share/g1/queues/demo/proto-q` |
| Topic (reply inbox) | — | `$reply/<clientId>/rq1` |
| KubeMQ channel | `demo.v3` | `demo.v3`, `demo.cmd`, `demo.proto-q` |
| QoS | 1 | 1 |
| Protocol library | `org.eclipse.paho.client.mqttv3` | `org.eclipse.paho.mqttv5.client` |
| User Properties | Not available | Bidirectional (max 32 props / 4096 B) |
| ResponseTopic | Not available | `$reply/<own-clientId>/rq1` |
| CorrelationData | Not available | Echoed on response |

## Gotchas

- **Retain silently dropped**: setting `retain=true` on any PUBLISH returns PUBACK 0x00
  but the message is never delivered. KubeMQ forces `RetainAvailable=0`.

- **MQTT 3.1 (level 3) rejected**: only 3.1.1 (level 4) and 5.0 (level 5) are accepted.
  MQTT 3.1 gets a CONNACK rejection.

- **RPC PUBACK is immediate**: the PUBACK for a command/query publish arrives as soon as
  the broker receives the message, not when the responder replies. Always implement
  your own response-wait timeout.

- **ResponseTopic must be in own namespace**: the `$reply/<clientId>/` prefix is mandatory.
  A foreign `$reply/` path returns PUBACK 0x83 (not authorized).

- **Plain `queues/` subscribe rejected**: always use `$share/<group>/queues/<ch>` at QoS >= 1.
  QoS 0 shared subscribe also returns SUBACK 0x83.

- **$share group name is audit-only**: ALL `$share` groups compete in ONE KubeMQ queue pool.
  There are no per-group message copies, unlike standard MQTT 5.0 broker semantics.

- **Events-Store StartNewOnly**: `store/` topics give no historical replay over MQTT;
  every subscription starts from the next new message only.

- **Wildcard N-copies**: overlapping event filter subscriptions each deliver one copy per
  matching bridge registry entry (N overlapping filters -> N copies; no cross-entry dedup).

## Related Examples

- [events/basic-pubsub](../../events/basic-pubsub/) — v5 events with User Properties in depth
- [queues/shared-consume](../../queues/shared-consume/) — shared queue consume patterns
- [rpc/query-round-trip](../../rpc/query-round-trip/) — full v5 RPC query round-trip
- [rpc/command-round-trip](../../rpc/command-round-trip/) — v5 RPC command round-trip
