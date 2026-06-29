# Versioning & Migration

## Overview

This is the **v1.0.0 initial release** of the `kubemq-mqtt` documentation and examples
repository. There is **no prior version to migrate from** — if you are reading this on the
first release, you are already on the latest version and there are no breaking changes to
reconcile.

This repository ships **documentation, runnable examples, and a burn-in soak harness** for the
KubeMQ MQTT connector. It contains **no installable package, no proto/gRPC bindings, and no
published client library**. The examples drive the connector exclusively through standard,
off-the-shelf MQTT 3.1.1 / 5.0 client libraries. Version numbers therefore track the state of
the docs and examples in this repo — not a client-library release.

---

## What "version" means here

| Versioned thing | Where it lives |
|-----------------|----------------|
| Docs + examples + burn-in harness (this repo) | Tagged releases, recorded in [`CHANGELOG.md`](../../CHANGELOG.md) |
| The MQTT connector itself | Shipped inside the KubeMQ server; versioned with the server |
| The MQTT wire protocol your client speaks | MQTT 3.1.1 / 5.0 — chosen by your MQTT client library, not by this repo |

Because the contract is the MQTT protocol plus the connector's topic grammar, your own client
code does not depend on a package published from this repository. You upgrade by pulling newer
docs and examples, not by bumping a dependency.

---

## Forward compatibility policy

This repository follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) applied to
the docs and examples:

- **MAJOR** — a change that breaks an existing example's run steps, removes a documented
  behavior, or contradicts a previously documented guarantee (for example, a connector change
  that alters the topic grammar or a forced capability).
- **MINOR** — new examples, new language coverage, new guides, or newly documented connector
  behavior that does not invalidate existing material.
- **PATCH** — corrections, clarifications, dependency pin bumps, and editorial fixes that do
  not change documented behavior.

Every notable change is recorded in [`CHANGELOG.md`](../../CHANGELOG.md) under the appropriate
version, in [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) style. When a future
release changes connector behavior, this guide will gain a per-version migration section
describing exactly what to update in your client code.

For the protocol-level differences between MQTT 3.1.1 and MQTT 5.0 (and what the connector
forces regardless of version), see [protocol-versions.md](protocol-versions.md) and
[../reference/capabilities.md](../reference/capabilities.md).
