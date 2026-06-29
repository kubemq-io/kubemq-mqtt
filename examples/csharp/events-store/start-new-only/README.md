# csharp — Events-Store: Start-New-Only (no replay)

Demonstrates persistent pub/sub via KubeMQ Events-Store over MQTT 5.0, proving
that MQTT subscriptions are **always `StartNewOnly`** — there is no historical
replay from an MQTT client.

## Prerequisites

- .NET 8+
- KubeMQ server with MQTT connector enabled
  (default `tcp://localhost:1883`, override via `KUBEMQ_MQTT_URL`)

## How to Run

```bash
cd examples/csharp/events-store/start-new-only
dotnet run

# Point to a non-default broker:
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run
```

## Expected Output

```
Publisher  connected to 127.0.0.1:1883

=== STEP 1: Publish 3 messages BEFORE subscribing (these will NOT be replayed) ===
  Published [pre-sub-1]: {"seq":1,"label":"pre-sub-1","channel":"demo.s","ts":"..."}
  Published [pre-sub-2]: {"seq":2,"label":"pre-sub-2","channel":"demo.s","ts":"..."}
  Published [pre-sub-3]: {"seq":3,"label":"pre-sub-3","channel":"demo.s","ts":"..."}
  -> Pre-subscription messages published and stored in KubeMQ Events-Store.
     A new subscriber will NOT receive these — MQTT always uses StartNewOnly.

=== STEP 2: Subscribe to the store channel ===
  Subscribed to 'store/demo/s' (QoS GrantedQoS1)
  StartNewOnly in effect — only messages published from NOW on will arrive.

=== STEP 3: Publish 1 message AFTER subscribing (this WILL be delivered) ===
  Published [post-sub]: {"seq":4,"label":"post-sub","channel":"demo.s","ts":"..."}

=== STEP 4: Wait for delivery and verify exactly 1 message received ===
  Received: {"seq":4,"label":"post-sub","channel":"demo.s","ts":"..."}

  Total messages received: 1
  Expected: 1  (only the post-subscribe message)

PASS: StartNewOnly confirmed — pre-subscribe messages were NOT replayed.
      Only the 1 post-subscribe message was delivered.
Done.
```

## What's Happening

1. **Publisher connects** using MQTT 5.0 to the broker parsed from `KUBEMQ_MQTT_URL`.

2. **Step 1 — publish before subscribing (×3).**
   Three messages are published to `store/demo/s`
   before any subscriber exists. KubeMQ stores them in the Events-Store (persistent
   log), but no subscriber will ever receive them — MQTT does not expose a "start
   from first" or "start from sequence" subscription mode.

3. **Step 2 — subscribe.**
   The subscriber connects and calls `SubscribeAsync` at QoS 1. The SUBACK returns
   `GrantedQoS1` (reason code `0x01`). KubeMQ registers this as a `StartNewOnly`
   subscription: delivery begins at the next message published _after_ this
   subscription is created.

4. **Step 3 — publish after subscribing (×1).**
   One post-subscribe message is published. KubeMQ delivers it to the subscriber.

5. **Step 4 — verify.**
   The program asserts exactly **1** message was received (never 4). If a replay had
   occurred, 4 messages would arrive. The assertion failing is evidence of a broker
   misconfiguration.

## MQTT Specifics

| Property | Value |
|---|---|
| Topic | `store/demo/s` |
| KubeMQ channel | `demo.s` |
| Topic prefix | `store/` → KubeMQ Events-Store |
| MQTT version | 5.0 (`MqttProtocolVersion.V500`) |
| QoS | 1 (AtLeastOnce) — both publish and subscribe |
| Retain | Not used (retain is silently stripped by KubeMQ; `RetainAvailable=0`) |
| User Properties | Not used in this example |
| Subscribe mode | Always `StartNewOnly` — enforced by the MQTT connector, not configurable |

## Gotcha: StartNewOnly is not configurable over MQTT

Unlike the KubeMQ gRPC API (which supports `SeqFirst`, `SeqRange`, `TimeRange`,
`Last`, `LastN`, `NewOnly`), the MQTT connector **hardcodes `StartNewOnly`** for
all Events-Store subscriptions. There is no MQTT Subscribe property or User
Property to request a different replay mode. If you need historical replay, use
the gRPC / REST / WebSocket API directly.

## Related Examples

- [events/basic-pubsub](../../events/basic-pubsub/) — non-persistent events (no storage)
- [events/wildcard-subscribe](../../events/wildcard-subscribe/) — subscribe with `+` / `#` wildcards
