# Connections Endpoint

The KubeMQ internal HTTP API exposes a **node-local** snapshot of active MQTT client
connections and their subscriptions.

---

## Endpoint

```
GET http://<host>:8080/api/mqtt/connections
```

| Property | Value |
|---|---|
| Port | `8080` (internal HTTP API — network-protected, not exposed to public internet) |
| Authentication | None (internal network access control) |
| Scope | **Node-local**: data reflects only the connections to the node you query. In a multi-node cluster, query each node separately and sum totals. |
| Inline clients | Filtered out — the internal bridge client is never included in the response. |

---

## Response Envelope

All responses use the standard KubeMQ API envelope:

```json
{
  "error": false,
  "error_string": "",
  "data": {
    "connections": [ ... ],
    "total": 3
  }
}
```

| Field | Type | Notes |
|---|---|---|
| `error` | `bool` | `false` on success |
| `error_string` | `string` | Empty string on success; error description on failure |
| `data` | object | Contains `connections` array and `total` count |
| `data.connections` | array of connection objects | See schema below. Empty array `[]` when no clients are connected or when the MQTT connector is disabled. |
| `data.total` | `int` | Count of entries in `data.connections` |

### Connection Object Schema

Each entry in `data.connections`:

```json
{
  "client_id":        "my-sensor-01",
  "protocol_version": 5,
  "remote_addr":      "10.0.0.42:54321",
  "connected_at":     "2026-06-12T10:30:00Z",
  "clean_session":    false,
  "username":         "display-only",
  "subscriptions": [
    {
      "filter":  "events/site1/+",
      "qos":     1,
      "pattern": "events",
      "channel": "site1.*"
    }
  ]
}
```

| Field | Type | Notes |
|---|---|---|
| `client_id` | `string` | MQTT client identifier |
| `protocol_version` | `int` | `4` = MQTT 3.1.1; `5` = MQTT 5.0 |
| `remote_addr` | `string` | Client IP and ephemeral port |
| `connected_at` | `string` | RFC 3339 UTC timestamp of session establishment |
| `clean_session` | `bool` | `true` = clean start; `false` = persistent session |
| `username` | `string` | MQTT `Username` field; display-only (not used for authentication). Omitted when empty. |
| `subscriptions` | array | Active subscriptions for this client. Empty array when none. |

### Subscription Object Schema

Each entry in `subscriptions`:

| Field | Type | Notes |
|---|---|---|
| `filter` | `string` | Original MQTT topic filter string |
| `qos` | `int` | Granted QoS (`0`, `1`, or `2`) |
| `pattern` | `string` | Mapped KubeMQ pattern: `events`, `store`, `queues`, `commands`, `queries`, or empty for local (`$reply`) topics |
| `channel` | `string` | Mapped KubeMQ channel (dot-separated; may contain `*` or `>` for wildcard subscriptions). Empty for `$reply` local topics. |
| `shared_group` | `string` | Non-empty for `$share/<group>/queues/<ch>` subscriptions; the group name. |

---

## Live Example

```
GET http://127.0.0.1:8080/api/mqtt/connections
```

Response with one connected MQTT 5.0 client:

```json
{
  "error": false,
  "error_string": "",
  "data": {
    "connections": [
      {
        "client_id": "go-example-01",
        "protocol_version": 5,
        "remote_addr": "127.0.0.1:55412",
        "connected_at": "2026-06-12T10:30:00Z",
        "clean_session": true,
        "subscriptions": [
          {
            "filter": "events/site1/#",
            "qos": 1,
            "pattern": "events",
            "channel": "site1.>",
            "shared_group": ""
          },
          {
            "filter": "queues/jobs/email",
            "qos": 1,
            "pattern": "queues",
            "channel": "jobs.email",
            "shared_group": "workers"
          }
        ]
      }
    ],
    "total": 1
  }
}
```

Response when no clients are connected:

```json
{
  "error": false,
  "error_string": "",
  "data": {
    "connections": [],
    "total": 0
  }
}
```

---

## Prometheus Metrics

The connector exports three Prometheus metrics.

### `kubemq_mqtt_connections` (Gauge)

```
kubemq_mqtt_connections
```

Current number of active MQTT client connections on this node. Incremented on
`OnSessionEstablished`, decremented on `OnDisconnect`. Session takeovers (same client
ID reconnect) do not double-count.

### `kubemq_mqtt_operations_total` (Counter)

```
kubemq_mqtt_operations_total{operation="<op>", status="<status>"}
```

Total bridge operations since process start.

**Labels:**

| Label | Values |
|---|---|
| `operation` | `publish_events`, `publish_store`, `publish_queues`, `rpc_command`, `rpc_query`, `deliver_events`, `deliver_store`, `deliver_queues`, `rpc_response` |
| `status` | `success`, `error`, `dropped` |

**Operation semantics:**

| `operation` | Description |
|---|---|
| `publish_events` | MQTT PUBLISH routed to KubeMQ Events |
| `publish_store` | MQTT PUBLISH routed to KubeMQ Events-Store |
| `publish_queues` | MQTT PUBLISH routed to KubeMQ Queues (produce) |
| `rpc_command` | MQTT 5.0 PUBLISH routed to KubeMQ Commands |
| `rpc_query` | MQTT 5.0 PUBLISH routed to KubeMQ Queries |
| `deliver_events` | Events/Events-Store message injected to MQTT subscriber |
| `deliver_store` | Events-Store message injected to MQTT subscriber |
| `deliver_queues` | Queue message injected to MQTT subscriber (PUBACK = ack) |
| `rpc_response` | RPC response injected to requester's `$reply` topic |

### `kubemq_mqtt_operation_duration_seconds` (Histogram)

```
kubemq_mqtt_operation_duration_seconds{operation="<op>"}
```

Latency histogram for MQTT bridge operations. Duration is recorded only for
request-response operations (`rpc_command`, `rpc_query`, `rpc_response`); per-message
stream deliver operations (`deliver_events`, `deliver_store`, `deliver_queues`) pass
duration 0 and are counted but not histogrammed.

**Buckets:** `0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60` seconds.

---

## curl Examples

List all current connections:

```bash
curl -s http://127.0.0.1:8080/api/mqtt/connections | jq .
```

Count connected clients:

```bash
curl -s http://127.0.0.1:8080/api/mqtt/connections | jq '.data.total'
```

List all client IDs:

```bash
curl -s http://127.0.0.1:8080/api/mqtt/connections \
  | jq '[.data.connections[].client_id]'
```

Filter for MQTT 5.0 clients only:

```bash
curl -s http://127.0.0.1:8080/api/mqtt/connections \
  | jq '[.data.connections[] | select(.protocol_version == 5)]'
```

List all active queue subscriptions with their groups:

```bash
curl -s http://127.0.0.1:8080/api/mqtt/connections \
  | jq '[.data.connections[].subscriptions[]
          | select(.pattern == "queues")
          | {filter, channel, shared_group}]'
```

---

See also: [Capabilities](capabilities.md) · [Reason Codes](reason-codes.md) ·
[Topic Grammar](topic-grammar.md)
