# Ruby — Connectivity: TLS

Demonstrates a TLS-secured MQTT 3.1.1 connection to the KubeMQ MQTT connector on port 8883.
The `mqtt` gem connects over TCP with TLS using an `OpenSSL::SSL::SSLContext`.
`VERIFY_NONE` (InsecureSkipVerify) is used so the example works against a KubeMQ server
with a self-signed certificate — the standard dev/test setup.
In production, supply a real CA bundle and use `VERIFY_PEER`.

> **Live-test note:** The TLS listener on port 8883 requires the KubeMQ server to be configured
> with `Security.CertFile` + `Security.KeyFile`. The default development broker has the TLS
> listener closed (port 8883 not open). This example compiles and runs correctly but the live
> connection is **N/A** until a TLS-enabled broker is available.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with the MQTT TLS listener enabled on port 8883
- `mqtt` gem ~> 0.7
- OpenSSL (standard Ruby distribution)

## How to Run

```bash
cd examples/ruby
bundle install
export KUBEMQ_MQTT_URL=tls://localhost:8883
ruby connectivity/tls/main.rb
```

To target a different host:

```bash
export KUBEMQ_MQTT_URL=tls://my-kubemq-host:8883
ruby connectivity/tls/main.rb
```

For mutual TLS (mTLS), edit `build_ssl_context` in `main.rb` to set `ctx.cert` and `ctx.key`.

## Expected Output

### Against a TLS-enabled broker (port 8883 open)

```
[TLS] Connecting to tls://localhost:8883
[TLS] ssl context: VERIFY_NONE (dev; use VERIFY_PEER + CA in production)
[sub] Connected over TLS
[sub] Subscribed to events/demo/tls at QoS 1
[pub] Connected over TLS
[pub] Published to events/demo/tls: Hello over TLS from Ruby!
[sub] Received:
  topic:   events/demo/tls
  payload: Hello over TLS from Ruby!
```

### Live-test result on this broker (port 8883 closed — N/A)

The current development broker has the TLS listener closed, so a live run reports a clean connection-refused and exits non-zero. The code is correct; the live connection is **N/A** until a TLS-enabled broker is available. Captured against `tls://127.0.0.1:8883`:

```
[TLS] Connecting to tls://localhost:8883
[TLS] ssl context: VERIFY_NONE (dev; use VERIFY_PEER + CA in production)
[error] Could not establish the TLS subscriber connection: Errno::ECONNREFUSED: Connection refused - connect(2) for "localhost" port 8883
[error] Is the broker's TLS listener enabled on localhost:8883?
```

## What's Happening

1. `KUBEMQ_MQTT_URL` is parsed; scheme `tls` sets `conn[:tls] = true`.
2. An `OpenSSL::SSL::SSLContext` is created with `VERIFY_NONE` — acceptable for
   self-signed certificates in development. In production use `VERIFY_PEER` with
   `ctx.ca_file` pointing to the server's CA certificate.
3. A subscriber thread opens a TLS connection, subscribes to `events/demo/tls` at
   QoS 1, and signals `sub_ready` so the publisher knows the subscription is active.
4. The publisher opens a second TLS connection and publishes one message to the same
   topic at QoS 1.
5. The MQTT topic `events/demo/tls` maps to the KubeMQ channel `demo.tls` under the
   **Events** pattern (prefix `events/`). Slashes become dots in the KubeMQ channel name.
6. The subscriber receives the message and both connections close cleanly.

## MQTT Specifics

| Property | Value |
|---|---|
| Topic | `events/demo/tls` |
| KubeMQ channel | `demo.tls` (Events pattern; `events/` prefix; `/` → `.`) |
| QoS | 1 |
| MQTT version | 3.1.1 (mqtt gem — MQTT 3.1.1 only) |
| Transport | TLS over TCP, port 8883 |
| KubeMQ pattern | Events |
| TLS verification | `VERIFY_NONE` (dev); use `VERIFY_PEER` + CA in production |
| MQTT 5.0 properties | None (mqtt gem is 3.1.1 only) |

## TLS Configuration (KubeMQ Server)

Enable the TLS listener by providing certificate material. Example with Docker:

```bash
docker run -d \
  -p 1883:1883 \
  -p 8883:8883 \
  -v /path/to/certs:/certs \
  -e KUBEMQCONFIG_SECURITY_CERTFILE=/certs/server.crt \
  -e KUBEMQCONFIG_SECURITY_KEYFILE=/certs/server.key \
  kubemq/kubemq
```

See `docs/guides/tls-and-websocket.md` for full TLS and mTLS configuration details.

## Production TLS (VERIFY_PEER)

Edit `build_ssl_context` in `main.rb`:

```ruby
def build_ssl_context
  ctx = OpenSSL::SSL::SSLContext.new
  ctx.verify_mode = OpenSSL::SSL::VERIFY_PEER
  ctx.ca_file     = "/path/to/ca.crt"
  # For mTLS (client certificate):
  # ctx.cert = OpenSSL::X509::Certificate.new(File.read("/path/to/client.crt"))
  # ctx.key  = OpenSSL::PKey::RSA.new(File.read("/path/to/client.key"))
  ctx
end
```

## Related Examples

- [connectivity/auth_jwt](../auth_jwt/) — password-as-JWT authentication over plain TCP
- [events/basic_pubsub](../../events/basic_pubsub/) — plain TCP pub/sub (no TLS)
