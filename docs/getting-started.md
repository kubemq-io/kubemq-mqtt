# Getting Started

Get an MQTT message flowing through KubeMQ in under 5 minutes.

## Prerequisites

- Docker
- An MQTT client: `mosquitto_pub` / `mosquitto_sub` (from `mosquitto-clients`), or any MQTT 5.0 library

## Step 1: Start KubeMQ

The MQTT connector is **enabled by default** on port `1883`. No extra flags needed:

```bash
docker run -d \
  --name kubemq \
  -p 1883:1883 \
  -p 8883:8883 \
  -p 8083:8083 \
  -p 50000:50000 \
  -p 8080:8080 \
  europe-docker.pkg.dev/kubemq/images/kubemq-next:latest
```

To expose all three listeners: TCP on `1883`, TLS on `8883` (inactive without a certificate), WebSocket on `8083`.

To **disable** the MQTT connector entirely:

```bash
docker run -d ... -e CONNECTORSMQTT_ENABLE=false europe-docker.pkg.dev/kubemq/images/kubemq-next:latest
```

Verify the broker accepts connections:

```bash
curl -s http://localhost:8080/api/mqtt/connections | jq .
# {"error":false,"error_string":"","data":{"connections":[],"total":0}}
```

## Step 2: Subscribe to Events

Open a terminal and subscribe using `mosquitto_sub` (MQTT 5.0):

```bash
mosquitto_sub -h localhost -p 1883 -V 5 -t "events/demo/#" -q 1 -v
```

Or using the Python paho library:

```python
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

def on_message(client, userdata, msg):
    print(f"{msg.topic}: {msg.payload.decode()}")

# paho-mqtt 2.x requires an explicit callback API version.
client = mqtt.Client(
    callback_api_version=CallbackAPIVersion.VERSION2,
    protocol=mqtt.MQTTv5,
)
client.connect("localhost", 1883, 30)
client.subscribe("events/demo/#", qos=1)
client.on_message = on_message
client.loop_forever()
```

Leave this running.

## Step 3: Publish an Event

In a second terminal, publish using `mosquitto_pub`:

```bash
mosquitto_pub -h localhost -p 1883 -V 5 -t "events/demo/hello" -q 1 -m "Hello, KubeMQ!"
```

The subscribing terminal displays:

```
events/demo/hello: Hello, KubeMQ!
```

The topic `events/demo/hello` maps to KubeMQ channel `demo.hello` in the **Events** pattern (prefix `events/` selects the pattern; `/` becomes `.`).

## Environment Variable

All examples read `KUBEMQ_MQTT_URL` to override the broker address. The default is `tcp://localhost:1883`. The scheme selects the transport:

| Scheme | Transport | Default port |
|--------|-----------|--------------|
| `tcp://` | Plain TCP | `1883` |
| `tls://` | TLS | `8883` |
| `ws://` | WebSocket | `8083` |

```bash
export KUBEMQ_MQTT_URL=tcp://my-kubemq-host:1883
```

## Step 4: Run a Language Example

Pick your language and run the basic pub/sub example:

| Language | Prerequisites | Command |
|----------|--------------|---------|
| Go | Go ≥ 1.24 | `cd examples/go && go run ./events/basic-pubsub/` |
| Python | Python ≥ 3.11, `paho-mqtt ≥ 2.1.0` | `cd examples/python && python events/basic_pubsub/main.py` |
| JavaScript | Node ≥ 18, `mqtt ≥ 5.15.1` | `cd examples/javascript && npm install && npx tsx events/basic-pubsub/index.ts` |
| Java | JDK ≥ 17, Maven | `cd examples/java/events/basic-pubsub && mvn compile exec:java` |
| C# | .NET ≥ 8, `MQTTnet ≥ 4.3.7` | `cd examples/csharp/events/basic-pubsub && dotnet run` |
| Ruby | Ruby ≥ 3.2, `mqtt gem ≥ 0.6.0` | `ruby examples/ruby/events/basic_pubsub/main.rb` |
| Rust | Rust stable, `rumqttc ≥ 0.24.0` | `cd examples/rust && cargo run -p basic-pubsub` |

## Topic Basics

The **first segment** of an MQTT topic selects the KubeMQ messaging pattern:

| Topic prefix | KubeMQ pattern | Example |
|-------------|----------------|---------|
| `events/` | Events (fire-and-forget pub/sub) | `events/sensors/temp` |
| `store/` | Events-Store (persistent pub/sub) | `store/sensors/temp` |
| `queues/` | Queues (produce only; consume via `$share`) | `queues/jobs/email` |
| `commands/` | Commands RPC (MQTT 5.0 only) | `commands/svc/restart` |
| `queries/` | Queries RPC (MQTT 5.0 only) | `queries/svc/status` |

No prefix → routes to `DefaultPattern` (default: `events`).

## Next Steps

- [Architecture](architecture.md) — understand how MQTT topics map to KubeMQ channels
- [Configuration](configuration.md) — all 17 env vars and defaults
- [Patterns](patterns/events.md) — deep dive into each messaging pattern
- [Guides](guides/topic-mapping.md) — topic grammar, auth, QoS, TLS, WebSocket
- [Six Gotchas](README.md#six-gotchas) — non-obvious behaviors to read before writing code
