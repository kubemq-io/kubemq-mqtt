# Ruby — Connectivity: Auth JWT

Demonstrates password-as-JWT authentication to the KubeMQ MQTT connector. The KubeMQ JWT token is passed in the MQTT CONNECT `Password` field (`PasswordFlag=true`). The `Username` field is display-only and is not used for authentication.

> **Live-test note:** The default development broker has auth disabled. Connecting with a JWT in the password field succeeds, but JWT rejection is not enforced. Full JWT enforcement requires an auth-enabled broker. This example is marked **partial/NA** for live testing: it connects and pub/subs successfully, but JWT validation is not demonstrated without an auth-enabled broker.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server (auth disabled: any token accepted; auth enabled: valid KubeMQ JWT required)
- `mqtt` gem ~> 0.7

## How to Run

```bash
cd examples/ruby
bundle install
export KUBEMQ_MQTT_URL=tcp://localhost:1883
export KUBEMQ_JWT_TOKEN=your-kubemq-jwt-here   # omit for open broker
ruby connectivity/auth_jwt/main.rb
```

## Expected Output

Captured from a live run against `tcp://127.0.0.1:1883` with auth **disabled** (open broker). The connect and round-trip succeed; JWT rejection is not exercised here (that requires an auth-enabled broker), so this variant is **partial** for live testing — not a failure.

```
Subscriber connected (JWT auth; broker enforces if auth is enabled)
Published to events/demo/auth
Received:
  topic:   events/demo/auth
  payload: Hello with JWT auth!
```

## What's Happening

- Both the subscriber and publisher connect with `password: JWT_TOKEN`.
- The `username:` field is accepted by the mqtt gem and sent to the broker, but KubeMQ uses it for display/audit only — authentication is done via the `password:` field only.
- When KubeMQ auth is enabled, the broker validates the JWT at connect time. An empty or invalid password results in CONNACK `0x86` (bad username/password). Auth is checked at connect time only — no mid-connection recheck.
- When auth is disabled (open broker), any password (including dummy values) is accepted.

## MQTT Specifics

| Property | Value |
|----------|-------|
| Topic | `events/demo/auth` |
| QoS | 1 |
| MQTT version | 3.1.1 (mqtt gem) |
| Auth mechanism | `password:` = KubeMQ JWT token |
| `username:` | Display-only (not used for auth) |
| CONNACK on bad password (auth on) | `0x86` |

## ACL Authorization

When ACL is enabled on the broker, publish/subscribe permissions are checked per pattern and channel. An ACL-denied publish returns PUBACK `0x87` (not authorized). See `docs/guides/authentication.md`.

## Related Examples

- [connectivity/tls](../tls/) — TLS-secured connection
- [events/basic_pubsub](../../events/basic_pubsub/) — plain TCP pub/sub without auth
