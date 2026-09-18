# KubeMQ MQTT

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Languages](https://img.shields.io/badge/languages-7-informational.svg)](examples/)
[![Examples](https://img.shields.io/badge/examples-11-success.svg)](examples/)
[![Protocol](https://img.shields.io/badge/protocol-MQTT%203.1.1%2F5.0-orange.svg)](docs/)
[![Direct Connect](https://img.shields.io/badge/KubeMQ-direct--connect-9cf.svg)](https://kubemq.io/)

Documentation, working examples, and a burn-in soak harness for the **KubeMQ MQTT connector** — the bridge that lets standard MQTT 3.1.1 / 5.0 clients talk to KubeMQ's Events, Events-Store, Queues, Commands, and Queries patterns over the MQTT wire protocol.

The examples use **standard third-party MQTT libraries only** (no KubeMQ proto bindings). They speak to the connector exactly the way any off-the-shelf MQTT client would.

## What's here

| Path | What it is |
|------|------------|
| [`docs/`](docs/) | Connector reference: architecture, getting-started, configuration, pattern guides, topic grammar, capabilities, and reason codes. |
| [`examples/`](examples/) | Runnable, per-pattern examples in Go, Python, JavaScript/TypeScript, Java, C#, Rust, and Ruby (Ruby ships the v3.1.1-capable subset). |
| [`burnin/`](burnin/) | Standalone Go soak-test harness that exercises the connector under sustained multi-pattern load. |

## Prerequisites

A running **KubeMQ server with the MQTT connector** — the connector is **enabled by default**, listening on MQTT port `1883` (TLS `8883`, WebSocket `8083`).

```bash
docker run -d \
  -p 1883:1883 \
  -p 8883:8883 \
  -p 8083:8083 \
  -p 50000:50000 \
  europe-docker.pkg.dev/kubemq/images/kubemq-next:latest
```

> The connector is on by default (`CONNECTORSMQTT_ENABLE=true`); set `CONNECTORSMQTT_ENABLE=false` to disable it.

## Configuration

Every example reads a **single** environment variable for the broker endpoint:

```bash
# default: tcp://localhost:1883
export KUBEMQ_MQTT_URL=tcp://localhost:1883
```

The scheme selects the transport:

| Scheme | Transport | Default endpoint |
|--------|-----------|------------------|
| `tcp://`  | Plain TCP    | `tcp://localhost:1883` |
| `tls://`  | TLS over TCP | `tls://localhost:8883` |
| `ws://`   | WebSocket    | `ws://localhost:8083/` |

## Topic grammar (at a glance)

The MQTT topic prefix selects the KubeMQ pattern; `/` in a topic becomes `.` in the KubeMQ channel:

| MQTT topic | KubeMQ pattern | Channel |
|------------|----------------|---------|
| `events/site1/temp`   | Events        | `site1.temp` |
| `store/orders`        | Events-Store  | `orders` |
| `queues/jobs`         | Queues        | `jobs` |
| `commands/restart`    | Commands (RPC)| `restart` |
| `queries/status`      | Queries (RPC) | `status` |

See [`docs/`](docs/) for the full grammar (wildcards, `$share/<group>/...` queue consume, `$reply/<clientID>/...` RPC responses) and the connector's forced capabilities and MQTT reason codes.

## Getting started

1. Start a KubeMQ server with the MQTT connector enabled (see [Prerequisites](#prerequisites)).
2. Read [`docs/getting-started.md`](docs/getting-started.md).
3. Pick a language under [`examples/`](examples/) and follow its README.

## License

[Apache-2.0](LICENSE)
