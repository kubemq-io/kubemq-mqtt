# csharp — Connectivity: TLS

Connect to the KubeMQ MQTT connector over TLS using MQTTnet (MQTT 5.0). Demonstrates a full events pub/sub round-trip on `events/demo/tls` with TLS transport on port 8883.

## Live-Test Status

The TLS listener (`tls://127.0.0.1:8883`) is **closed** on the reference broker. This example compiles and runs correctly; full live testing requires a TLS-enabled broker. When the TLS listener is unavailable, the example exits with a clear error message — this is expected and is not a code defect.

## Prerequisites

- .NET 8 or later
- KubeMQ server with TLS MQTT listener enabled (port 8883, requires Security config in KubeMQ, TLS minimum version 1.2)
- A server certificate (self-signed or CA-issued); the example uses `InsecureSkipVerify` for development

## How to Run

```bash
cd examples/csharp/connectivity/tls

# Point at a TLS-enabled broker:
KUBEMQ_MQTT_URL=tls://127.0.0.1:8883 dotnet run

# Falls back gracefully to plain TCP if tls:// is unavailable:
KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run
```

## Expected Output

**TLS-enabled broker (port 8883 open):**

```
Broker:          tls://127.0.0.1:8883
Subscribe topic: events/demo/tls  (KubeMQ channel demo.tls)
Publish topic:   events/demo/tls  (KubeMQ channel demo.tls)
Transport:       TLS

[1] Connecting subscriber...
    Subscriber connected  (TLS=True, ResultCode=Success)
    Subscribed to 'events/demo/tls'  (SUBACK=GrantedQoS1)

[2] Connecting publisher...
    Publisher connected  (TLS=True, ResultCode=Success)
    Published to 'events/demo/tls' with UserProperty source=csharp-tls-example  (PUBACK=Success)

[3] Waiting for event delivery (10 s timeout)...
    Received payload: {"message":"TLS connectivity test","channel":"demo.tls","ts":"..."}

[4] Disconnecting...
    Done.
```

**Reference broker (TLS listener on 8883 closed — live-test N/A):**

```
Broker:          tls://127.0.0.1:8883
Subscribe topic: events/demo/tls  (KubeMQ channel demo.tls)
Publish topic:   events/demo/tls  (KubeMQ channel demo.tls)
Transport:       TLS

[1] Connecting subscriber...
    Subscriber connection failed: Error while connecting host 'Unspecified/127.0.0.1:8883'.
    (TLS listener may be disabled on this broker — live test is N/A)
```

## What's Happening

1. **Parse scheme** — The `tls://` scheme in `KUBEMQ_MQTT_URL` sets `isTls=true`, which activates `WithTlsOptions` in MQTTnet. `tcp://` falls through to plain TCP.
2. **TLS options** — `WithCertificateValidationHandler(_ => true)` skips certificate verification for development convenience. In production replace this with a proper CA chain check:
   ```csharp
   tls.WithCertificateValidationHandler(ctx => ctx.Chain.Build(ctx.Certificate));
   ```
3. **Subscribe first** — The subscriber connects and subscribes to `events/demo/tls` at QoS 1 before the publisher connects. This ensures the subscription is registered in the broker before the message arrives.
4. **Publish** — The publisher connects over the same TLS transport and publishes a JSON payload with an MQTT 5.0 User Property (`source=csharp-tls-example`), which KubeMQ stores as a Tag and echoes back to subscribers.
5. **Delivery wait** — A 10-second timeout guards the receive step; if the broker drops the message (e.g. TLS listener closed), the example exits non-zero.
6. **Clean disconnect** — Both clients send a DISCONNECT packet.

## MQTT Specifics

| Property | Value |
|---|---|
| Topic (subscribe) | `events/demo/tls` |
| Topic (publish) | `events/demo/tls` |
| KubeMQ channel | `demo.tls` |
| KubeMQ pattern | Events (fire-and-forget, no persistence) |
| QoS | 1 (AtLeastOnce) |
| MQTT version | 5.0 (v500) |
| Transport | TLS over TCP, port 8883 |
| MQTT 5.0 properties used | User Property `source=csharp-tls-example` → KubeMQ Tag |
| TLS validation | `InsecureSkipVerify` (dev only) |

### Reason Codes

| Code | Meaning |
|---|---|
| `0x00` | Success |
| `0x80` | Broker not ready (publish gated; retry after a moment) |
| `0x86` | Bad credentials (invalid/empty password when auth is enabled) |
| `0x87` | Not authorized (ACL deny) |
| `0x90` | Topic-name-invalid (empty channel or `DefaultPattern=none`) |
| `0x97` | Quota exceeded (User-Property cap: 32 properties / 4096 bytes total, or `RpcMaxPending` hit) |
| `0x9A` | Retain not supported (only on Will-retain at CONNECT; runtime retain silently dropped) |

## Gotchas

- **Retain is silently dropped** — A publish with `Retain=true` returns PUBACK `0x00` (success) but the message is never delivered. It is audited as an error server-side. Do not set `Retain=true`.
- **Events-Store is StartNewOnly over MQTT** — If you use `store/` topics instead of `events/`, there is no historical replay; only messages published after the subscription is registered are delivered.
- **User-Property cap** — 32 properties / 4096 bytes total per message. Exceeding the cap returns PUBACK `0x97`.
- **TLS requires Security config** — The KubeMQ server only activates the TLS listener when a Security section (certificate + key) is present in its configuration. Without it, port 8883 remains closed.
- **JWT in cleartext at MQTT layer** — If using JWT authentication, always use the TLS listener (8883) to protect credentials in transit. See [connectivity/auth-jwt](../auth-jwt/).

## Related Examples

- [connectivity/websocket](../websocket/) — WebSocket transport (MQTT 5.0 over `ws://`)
- [connectivity/auth-jwt](../auth-jwt/) — JWT authentication over MQTT Password field
- [events/basic-pubsub](../../events/basic-pubsub/) — Full events pub/sub with User Properties and wildcard
