# Authentication

## Overview

The KubeMQ MQTT connector uses **password-as-JWT** authentication. The MQTT `Password` field carries a KubeMQ JWT token; the `Username` field is accepted but is **display-only** and plays no role in authentication. When authentication is not configured the broker is open, matching KubeMQ's gRPC and REST parity behaviour.

Authentication is checked **once at CONNECT time** only. There is no mid-connection token recheck; revoking a token does not disconnect an already-authenticated session.

---

## How Password-as-JWT Works

| MQTT CONNECT field | Role | Notes |
|--------------------|------|-------|
| `Password` | KubeMQ JWT token | Must be set; `PasswordFlag=true` is required by the MQTT spec when a password is present |
| `Username` | Display / audit label | Stored for the connections endpoint (`/api/mqtt/connections`) but never checked against the token |

```
CONNECT
  ClientID: my-client
  Username:  alice              ← display label only
  Password:  <KubeMQ JWT>       ← the actual credential
  PasswordFlag: true
```

---

## Reason Codes

| Scenario | Packet | Reason code |
|----------|--------|-------------|
| Auth disabled — any password accepted | CONNACK | `0x00` success |
| Auth enabled — valid JWT | CONNACK | `0x00` success |
| Auth enabled — empty password | CONNACK | `0x86` bad username/password |
| Auth enabled — invalid JWT or bad signature | CONNACK | `0x86` bad username/password |

> **Note:** On this broker auth appears disabled (connecting with any JWT in the Password field succeeds; JWT rejection is not enforced). Full JWT enforcement requires an auth-enabled broker.

---

## Empty Password Behaviour

When authentication is enabled, a CONNECT with an empty `Password` (or `PasswordFlag=false`) is immediately refused with CONNACK reason code `0x86`. The connection is never established. Clients must always supply the token in the `Password` field.

---

## ACL Authorization

Once connected, every publish and subscribe is checked against the KubeMQ ACL. ACL rules are evaluated per `(pattern, channel, read/write)` tuple.

| Outcome | Packet | Reason code |
|---------|--------|-------------|
| Allowed | PUBACK / SUBACK | `0x00` / `0x01` |
| Denied on publish | PUBACK | `0x87` not authorized |
| Denied on subscribe | SUBACK | `0x87` not authorized |

Special namespace rules:

- `$reply/<clientID>/<suffix>` — a client's **own** reply namespace is always allowed regardless of ACL rules.
- Other `$`-prefixed topics (except `$share/`) are denied by default.

---

## Open (No-Auth) Configuration

When no auth provider is configured the broker is open: the `Password` field is ignored, every CONNECT succeeds, and ACL checks are skipped. All examples in this repository assume open mode.

To enable authentication, configure the KubeMQ server with an auth provider and set `KUBEMQ_AUTH_TYPE` / the server auth block. No MQTT-specific environment variables control auth — it is a shared server-level setting.

---

## Connect Example (MQTT 5.0, paho.golang)

```go
// Auth-enabled broker: supply JWT in the Password field.
conn, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
    BrokerUrls:    []*url.URL{brokerURL},
    KeepAlive:     30,
    ConnectPacket: &connect.Connect{
        ClientID:     "my-client",
        Username:     "alice",        // display-only
        Password:     []byte(jwtToken),
        PasswordFlag: true,
    },
    // ...
})
```

On an auth-disabled broker the same code still works; the server simply does not validate the token.

---

## Connect Example (MQTT 3.1.1, paho.mqtt.golang)

```go
opts := mqtt.NewClientOptions().
    AddBroker("tcp://127.0.0.1:1883").
    SetClientID("my-client").
    SetUsername("alice").        // display-only
    SetPassword(jwtToken)        // KubeMQ JWT token
client := mqtt.NewClient(opts)
if token := client.Connect(); token.Wait() && token.Error() != nil {
    log.Fatal(token.Error())
}
```

---

## See Also

- [guides/protocol-versions.md](protocol-versions.md) — MQTT 3.1.1 vs v5 feature matrix
- [reference/reason-codes.md](../reference/reason-codes.md) — full reason code table including `0x86` / `0x87`
- [examples/go/connectivity/auth-jwt/](../../examples/go/connectivity/auth-jwt/)
- [examples/python/connectivity/auth_jwt/](../../examples/python/connectivity/auth_jwt/)
- [examples/javascript/connectivity/auth-jwt/](../../examples/javascript/connectivity/auth-jwt/)
