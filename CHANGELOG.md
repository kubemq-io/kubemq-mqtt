# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This repository ships **documentation, runnable examples, and a burn-in soak harness** for the
`kubemq-mqtt` connector — there is **no published package**. Version numbers track the
state of the docs and examples in this repo, not a client library release.

## [Unreleased]

## [1.0.0] - 2026-06-17

### Added

- Initial release of the `kubemq-mqtt` direct-connect documentation and examples repository.
- Connector overview: bridging **MQTT 3.1.1 / 5.0** onto KubeMQ's native messaging patterns —
  Events, Events-Store, Queues, Commands, and Queries — over the embedded
  [mochi-mqtt](https://github.com/mochi-mqtt/server) broker hosted inside the KubeMQ server.
- `docs/`: architecture, getting-started, configuration (`MqttConfig` / `CONNECTORSMQTT_*` env
  vars), per-pattern guides (events, events-store, queues, commands, queries), topic-mapping /
  authentication / QoS-and-sessions / TLS-and-WebSocket / protocol-versions guides, and a
  reference section (topic grammar, forced capabilities, connections endpoint, reason codes).
- `examples/`: the 11-variant pattern matrix across **7 languages** (Go, Python, JavaScript/
  TypeScript, Java, C#, Ruby, Rust) — Events basic and wildcard pub/sub, Events-Store
  StartNewOnly, Queues shared-consume and ack/redelivery, RPC query and command round-trips,
  Connectivity over TLS / WebSocket / JWT-auth, and an MQTT 3.1.1-vs-5.0 protocol comparison.
  Examples use standard third-party MQTT libraries only (no KubeMQ proto bindings). Ruby ships
  the MQTT-3.1.1-capable subset (the v5-only RPC and shared-consume variants and the WebSocket
  variant are N/A for Ruby).
- `burnin/`: standalone Go soak-test harness exercising the connector under sustained
  multi-pattern load.

[Unreleased]: https://github.com/kubemq-io/kubemq-mqtt/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/kubemq-io/kubemq-mqtt/releases/tag/v1.0.0
