# rust — Events: Basic Pub/Sub

Fire-and-forget event pub/sub using `rumqttc` (MQTT 5.0) and `tokio`.  
Demonstrates the core KubeMQ Events pattern: subscribe with a single-level `+` wildcard filter (`events/demo/+`), publish a message to `events/demo/x`, receive it — and shows how the MQTT publish topic `events/demo/x` maps to the KubeMQ channel `demo.x`.

## Prerequisites

- Rust 1.75+ (stable toolchain)
- KubeMQ server with the MQTT connector enabled on port 1883 (default `tcp://localhost:1883`)

## How to Run

```bash
cd examples/rust
KUBEMQ_MQTT_URL=tcp://localhost:1883 cargo run -p basic-pubsub
```

The `KUBEMQ_MQTT_URL` variable accepts `tcp://`, `tls://`, and `ws://` schemes.  
If omitted it defaults to `tcp://localhost:1883`.

## Expected Output

> Regenerated from a live run in the LiveTest stage. Topic strings below are canonical.

```
[1] Broker URL: tcp://localhost:1883
[2] Subscriber client ID: mqtt-rust-pubsub-sub-<random>
[3] Publisher client ID: mqtt-rust-pubsub-pub-<random>
[4] Subscribing to 'events/demo/+' at QoS 1 ...
    SUBACK received — subscription active
[5] Publishing to 'events/demo/x' — payload='hello', user-prop k1=v1 ...
    Publish sent (PUBACK expected 0x00)
[6] Received on 'events/demo/x': hello
    v5 user-prop (KubeMQ Tag): k1=v1
[7] EVENT_RECEIVED — round-trip successful
[8] Done.
```

## What's Happening (step by step)

1. **Two connections are opened** — a dedicated subscriber client and a dedicated publisher client, each with a unique random client ID.  Two separate connections are used here as a clarity/best-practice choice that cleanly separates the publish and subscribe roles; it is **not** required because of any loopback restriction — the KubeMQ MQTT connector *does* deliver a published event back to the same connection that is subscribed to the matching topic (verified against the live connector).
2. **Subscribe** — the subscriber connects and subscribes to the single-level wildcard filter `events/demo/+` at QoS 1.  
   The KubeMQ connector strips the `events/` prefix and maps the remaining filter `demo/+` to the KubeMQ channel filter `demo.*` (every `/` becomes a `.`, and `+` maps to `*`), which matches the published `demo.x` channel.  
   The example waits for the SUBACK before publishing so no message is missed.
3. **Publish** — the publisher client sends payload `hello` to `events/demo/x` at QoS 1, with a single MQTT 5.0 User Property `k1=v1`.  
   The connector forwards this to KubeMQ as an Events message on channel `demo.x`, carrying the user property as a KubeMQ Tag.
4. **Receive** — the subscriber's event loop receives the message, prints the topic, payload, and the round-tripped User Property (KubeMQ Tag).
5. **Exit** — both connections are dropped and the process exits cleanly.

## MQTT Specifics

| Property | Value |
|---|---|
| MQTT version | 5.0 |
| Subscribe topic | `events/demo/+` (single-level `+` wildcard → channel filter `demo.*`) |
| Publish topic | `events/demo/x` |
| KubeMQ pattern | Events |
| KubeMQ channel | `demo.x` (topic path with `/` → `.`) |
| QoS | 1 (AtLeastOnce) |
| MQTT 5.0 User Property | `k1=v1` — forwarded as KubeMQ Tag, echoed back to subscriber |
| Payload | `hello` |
| Retain | `false` — retain is forced off by the connector (`RetainAvailable=0`); see Gotchas |
| PUBACK reason code (success) | `0x00` |
| SUBACK reason code (success) | `0x00` |

## Gotchas

- **Two connections are a best-practice choice, not a requirement.** This example uses separate subscriber and publisher connections to keep the two roles cleanly separated. Same-connection loopback *is* supported — the connector delivers a published event back to the same connection if it is subscribed to the matching topic (verified against the live connector). A single connection that both publishes and subscribes will receive its own message.
- **Retain is silently dropped.** Setting `retain=true` on a publish returns PUBACK `0x00` (success), but the message is silently discarded by the connector — it is never stored or delivered.  Do not rely on retained messages over MQTT with KubeMQ.
- **`/` and `.` are interchangeable in channel names.** `events/demo.x` and `events/demo/x` both map to KubeMQ channel `demo.x`.  Avoid embedding `.` in MQTT topic segments if you need distinct channels.
- **User Properties require MQTT 5.0.** MQTT 3.1.1 has no User Properties.  Publishing over v3.1.1 means KubeMQ Tags cannot be set from the MQTT side.
- **User Property cap:** 32 properties / 4096 bytes total per message.  Exceeding the cap causes PUBACK `0x97` (quota exceeded) and the message is rejected.

## Related Examples

- [events/wildcard-subscribe](../wildcard-subscribe/) — single-level `+` and multi-level `#` wildcard subscriptions; overlapping filters deliver N copies
- [events-store/start-new-only](../../events-store/start-new-only/) — persistent events over `store/<ch>`; always `StartNewOnly`, no historical replay over MQTT
- [connectivity/tls](../../connectivity/tls/) — same pub/sub flow over a TLS-encrypted connection
- [connectivity/websocket](../../connectivity/websocket/) — same pub/sub flow over a WebSocket transport
- [protocol/mqtt-v311-vs-v5](../../protocol/mqtt-v311-vs-v5/) — side-by-side comparison of MQTT 3.1.1 and 5.0 behaviours
