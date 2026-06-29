# TLS and WebSocket

## Overview

The KubeMQ MQTT connector exposes up to three listeners simultaneously: plain TCP (`1883`), TLS over TCP (`8883`), and WebSocket (`8083`). All three listeners support both MQTT 3.1.1 and MQTT 5.0. Examples default to MQTT 5.0 (per project convention D7).

Listener enablement is independent — each port can be disabled by setting the corresponding config field to an empty string.

---

## Listeners at a Glance

| Listener | Default port | URL scheme | Env var to disable |
|----------|-------------|------------|--------------------|
| TCP (plain) | 1883 | `tcp://host:1883` | `CONNECTORSMQTT_PORT=""` |
| TLS | 8883 | `tls://host:8883` | `CONNECTORSMQTT_TLS_PORT=""` |
| WebSocket | 8083 | `ws://host:8083/` | `CONNECTORSMQTT_WS_PORT=""` |

The TCP listener is always enabled by default. The TLS listener is **active only when a Security configuration is present** in the KubeMQ server — on a default (no-TLS-config) deployment the port is open but the listener is not started. The WebSocket listener is always enabled by default.

> **Live broker note:** on the reference broker at `127.0.0.1`, TLS (`8883`) is not enabled (no Security config present). TLS examples compile and are structurally correct but live testing is N/A on this broker. WebSocket (`ws://127.0.0.1:8083/`) is active.

---

## TLS Listener (`tls://host:8883`)

### Requirements

| Requirement | Details |
|-------------|---------|
| KubeMQ Security config | Must be configured (`Security.CertFile`, `Security.KeyFile`); connector derives TLS from it |
| Minimum TLS version | 1.2 |
| Mutual TLS (mTLS) | Supported: set `Security.CAFile`; client must present a certificate |
| MQTT protocols | Both 3.1.1 and 5.0 |

### Connecting (Go, paho.golang)

```go
import (
    "crypto/tls"
    "net/url"
)

tlsCfg := &tls.Config{
    // For mTLS, load client cert + key here.
    // For server-only TLS, set InsecureSkipVerify: true in dev
    // or supply RootCAs pointing to the CA cert.
    MinVersion: tls.VersionTLS12,
}

brokerURL, _ := url.Parse("tls://127.0.0.1:8883")
conn, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
    BrokerUrls: []*url.URL{brokerURL},
    TlsCfg:     tlsCfg,
    KeepAlive:  30,
    // ...
})
```

### Connecting (Python, paho-mqtt)

```python
import ssl
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

client = mqtt.Client(
    callback_api_version=CallbackAPIVersion.VERSION2,
    protocol=mqtt.MQTTv5,
)
client.tls_set(
    ca_certs="ca.crt",          # server CA certificate
    certfile="client.crt",      # for mTLS; omit for server-only TLS
    keyfile="client.key",
    tls_version=ssl.PROTOCOL_TLS_CLIENT,
)
client.connect("127.0.0.1", 8883, keepalive=30)
```

### Connecting (Ruby, mqtt gem — TCP TLS)

The Ruby `mqtt` gem supports TLS over TCP via `ssl: true`:

```ruby
require 'mqtt'
client = MQTT::Client.connect(
  host:    '127.0.0.1',
  port:    8883,
  ssl:     true,
  version: '3.1.1'
)
```

---

## WebSocket Listener (`ws://host:8083/`)

The WebSocket listener accepts connections at path `/` (`ws://host:8083/`). Both MQTT 3.1.1 and 5.0 are supported. The examples in this repository use MQTT 5.0 over WebSocket (per project convention D7).

> **Note:** MQTT 5.0 over WebSocket is equally valid and is what the examples use.

### Connecting (JavaScript/TypeScript, MQTT.js)

```typescript
import * as mqtt from 'mqtt';

const client = mqtt.connect('ws://127.0.0.1:8083/', {
  protocolVersion: 5,
  clientId: 'my-ws-client',
  keepalive: 30,
  clean: true,
});
```

### Connecting (Go, paho.golang)

```go
import "net/url"

brokerURL, _ := url.Parse("ws://127.0.0.1:8083/")
conn, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
    BrokerUrls: []*url.URL{brokerURL},
    KeepAlive:  30,
    // ...
})
```

### Connecting (Python, paho-mqtt)

```python
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

client = mqtt.Client(
    callback_api_version=CallbackAPIVersion.VERSION2,
    transport="websockets",
    protocol=mqtt.MQTTv5,
)
client.ws_set_options(path="/")
client.connect("127.0.0.1", 8083, keepalive=30)
```

### Connecting (Java, Eclipse Paho MQTTv5)

```java
MqttConnectionOptions opts = new MqttConnectionOptions();
opts.setCleanStart(true);
opts.setKeepAliveInterval(30);

IMqttAsyncClient client = new MqttAsyncClient(
    "ws://127.0.0.1:8083/",
    "my-ws-client",
    new MemoryPersistence()
);
client.connect(opts).waitForCompletion();
```

---

## Ruby WebSocket — Not Available

> **Note:** The `mqtt` Ruby gem (njh/ruby-mqtt) supports **TCP and TLS transports only** — there is no WebSocket transport. The `connectivity/websocket` example is **N/A for Ruby**. Use the `connectivity/tls` example for an encrypted channel from Ruby.

---

## Disabling Listeners

Set the port to an empty string via environment variable to disable a listener:

```bash
# Disable TLS listener
CONNECTORSMQTT_TLS_PORT=""

# Disable WebSocket listener
CONNECTORSMQTT_WS_PORT=""

# Disable plain TCP listener (requires TLS or WS to be active)
CONNECTORSMQTT_PORT=""
```

Validation: at least one of `Port`, `TlsPort`, `WsPort` must be non-empty; all active ports must be distinct.

---

## Single `KUBEMQ_MQTT_URL` Environment Variable

All examples read a single `KUBEMQ_MQTT_URL` environment variable (default `tcp://127.0.0.1:1883`) and parse its scheme to select the transport:

| URL scheme | Transport |
|------------|-----------|
| `tcp://host:1883` | Plain TCP |
| `tls://host:8883` | TLS over TCP |
| `ws://host:8083/` | WebSocket |

Override at runtime:

```bash
# TLS example
KUBEMQ_MQTT_URL=tls://127.0.0.1:8883 go run .

# WebSocket example
KUBEMQ_MQTT_URL=ws://127.0.0.1:8083/ node index.js
```

---

## See Also

- [guides/authentication.md](authentication.md) — JWT auth over any transport
- [guides/protocol-versions.md](protocol-versions.md) — v3.1.1 vs v5 feature matrix
- [examples/go/connectivity/tls/](../../examples/go/connectivity/tls/)
- [examples/go/connectivity/websocket/](../../examples/go/connectivity/websocket/)
- [examples/javascript/connectivity/websocket/](../../examples/javascript/connectivity/websocket/)
- [examples/ruby/connectivity/tls/](../../examples/ruby/connectivity/tls/)
