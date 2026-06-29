"""
Example: rpc/query_round_trip

Demonstrates a KubeMQ Queries RPC round-trip via MQTT 5.0.

MQTT 5.0 ONLY — v3.1.1 publishes to ``queries/`` are silently dropped (gotcha).
The broker sends PUBACK IMMEDIATELY on receipt; PUBACK does NOT mean the response
has arrived — the client MUST implement its own response-wait timeout.

Flow (MQTT client = caller; responder is on the gRPC side):
  1. Client subscribes to its own reply inbox:
       $reply/<clientID>/inbox   (QoS 1, BEFORE publishing)
     The $reply/<clientID>/... namespace is mochi-local and is never routed to
     the KubeMQ array.  Using another client's namespace → PUBACK 0x83.
  2. Client publishes to ``queries/demo/rpc`` with MQTT 5.0 properties:
       Properties.ResponseTopic  = "$reply/<clientID>/inbox"
       Properties.CorrelationData = <unique bytes>          (echoed in response)
  3. PUBACK arrives IMMEDIATELY (not after the response).
  4. KubeMQ routes the query to the gRPC-side responder on channel ``demo.rpc``.
  5. Responder calls SendQueryResponse; broker delivers the reply to the inbox:
       Properties.CorrelationData = echoed from the request
       Properties.UserProperty    = kubemq-metadata + optional tags

Relevant reason codes:
  0x83  ResponseTopic outside own $reply/<clientID>/... namespace
  0x97  RpcMaxPending quota exceeded
  0x9A  Retain flag set on the PUBLISH (silently dropped at runtime)
  0xA2  Wildcard subscribe on a non-events topic

REQUIREMENT: a gRPC-side responder must be running on channel ``demo.rpc`` (type
query) before starting this example.  Without it, the connector audits
``rpc.timeout`` after RpcTimeoutSeconds (default 30 s) and no reply arrives.
See: kubemq-mqtt/.work/test-harness/responder/

Run:
  export KUBEMQ_MQTT_URL=tcp://localhost:1883
  # Terminal 1 — start gRPC responder
  cd kubemq-mqtt/.work/test-harness/responder
  go run . -channel demo.rpc -type query
  # Terminal 2 — run this example
  cd examples/python
  python rpc/query_round_trip/main.py
"""

from __future__ import annotations

import os
import threading
import time
import uuid

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes


def _rc_int(rc: object) -> int:
    """Normalize a paho reason code (ReasonCode or int) to a plain int."""
    return rc.value if hasattr(rc, "value") else int(rc)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLIENT_ID = "python-query-rpc-client"
# Topic grammar: queries/<ch> → KubeMQ Queries pattern; '/' → '.' in channel.
# queries/demo/rpc → channel demo.rpc
QUERY_TOPIC = "queries/demo/rpc"
# Reply inbox: $reply/<own-clientID>/<suffix>  (mochi-local, never routed to array).
# Must use OWN clientID namespace — foreign namespace → PUBACK 0x83.
REPLY_TOPIC = f"$reply/{CLIENT_ID}/inbox"
# Unique correlation bytes echoed back by the broker on the response.
CORRELATION = uuid.uuid4().bytes


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def broker_url() -> str:
    return os.environ.get("KUBEMQ_MQTT_URL", "tcp://localhost:1883")


def parse_url(url: str) -> tuple[str, str, int]:
    """Return (scheme, host, port) from a tcp://host:port or ws://host:port URL."""
    scheme, rest = url.split("://", 1)
    host_port = rest.rstrip("/")
    if ":" in host_port:
        host, port_str = host_port.rsplit(":", 1)
        port = int(port_str)
    else:
        host = host_port
        port = {"tcp": 1883, "tls": 8883, "ws": 8083}.get(scheme, 1883)
    return scheme, host, port


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)
    print(f"[client] connecting to {url} as client-id={CLIENT_ID!r}")

    response_received = threading.Event()
    response_data: list[dict] = []

    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=CLIENT_ID,
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def on_connect(c: mqtt.Client, userdata, flags, reason_code, props):  # noqa: ANN001
        rc = _rc_int(reason_code)
        if rc != 0:
            print(f"[client] ERROR: CONNACK reason code 0x{rc:02x}")
            return
        print(f"[client] connected (rc=0x{rc:02x})")
        # Step 1: subscribe to own reply inbox BEFORE publishing the query.
        # QoS 1 required — QoS 0 on a $reply topic is allowed (mochi-local),
        # but QoS 1 ensures delivery acknowledgement from the broker.
        c.subscribe(REPLY_TOPIC, qos=1)
        print(f"[client] subscribing to reply inbox {REPLY_TOPIC!r} (QoS 1)")

    def on_subscribe(c: mqtt.Client, userdata, mid, reason_codes, props):  # noqa: ANN001
        codes = reason_codes if hasattr(reason_codes, "__iter__") else [reason_codes]
        for rc in codes:
            code = rc.value if hasattr(rc, "value") else int(rc)
            if code <= 1:
                print(f"[client] SUBACK 0x{code:02x} — subscribed to reply inbox OK")
            elif code == 0x83:
                print("[client] ERROR SUBACK 0x83 — foreign $reply namespace; "
                      "ResponseTopic must use own clientID")
            elif code == 0xA2:
                print("[client] ERROR SUBACK 0xA2 — wildcard not allowed on non-events topic")
            else:
                print(f"[client] WARNING SUBACK 0x{code:02x} — unexpected reason code")

    def on_message(c: mqtt.Client, userdata, msg: mqtt.MQTTMessage):  # noqa: ANN001
        """Receive the query response delivered to $reply/<clientID>/inbox."""
        user_props: dict[str, str] = {}
        if msg.properties and hasattr(msg.properties, "UserProperty"):
            for k, v in (msg.properties.UserProperty or []):
                user_props[k] = v
        response_data.append({
            "topic": msg.topic,
            "payload": msg.payload,
            "correlation": getattr(msg.properties, "CorrelationData", None),
            "user_props": user_props,
        })
        response_received.set()

    def on_publish(c: mqtt.Client, userdata, mid, reason_codes=None, props=None):  # noqa: ANN001
        # PUBACK arrives immediately — it does NOT signal that the response is ready.
        code = 0x00
        if reason_codes is not None:
            codes = reason_codes if hasattr(reason_codes, "__iter__") else [reason_codes]
            for rc in codes:
                code = rc.value if hasattr(rc, "value") else int(rc)
                break
        if code == 0x00:
            print(f"[client] PUBACK 0x{code:02x} — broker accepted the query "
                  "(PUBACK is immediate; response arrives separately)")
        elif code == 0x83:
            print("[client] ERROR PUBACK 0x83 — ResponseTopic uses a foreign $reply namespace")
        elif code == 0x97:
            print("[client] ERROR PUBACK 0x97 — RpcMaxPending quota exceeded")
        else:
            print(f"[client] PUBACK 0x{code:02x} — unexpected reason code")

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_publish = on_publish

    if scheme == "tls":
        client.tls_set()

    client.connect(host, port, keepalive=30)
    client.loop_start()

    # Wait for the subscription to be established before publishing.
    time.sleep(0.5)

    # ------------------------------------------------------------------
    # Step 2: publish query to queries/demo/rpc with MQTT 5.0 properties.
    # ------------------------------------------------------------------
    pub_props = Properties(PacketTypes.PUBLISH)
    # ResponseTopic MUST be within own $reply/<clientID>/... namespace.
    pub_props.ResponseTopic = REPLY_TOPIC
    # CorrelationData is echoed verbatim on the response.
    pub_props.CorrelationData = CORRELATION
    # User properties become KubeMQ Tags (v5 only; max 32 props / 4096 bytes total).
    pub_props.UserProperty = [("request-id", str(uuid.uuid4())), ("source", "mqtt-python")]

    payload = b'{"query": "hello from MQTT"}'
    print(f"[client] publishing query to {QUERY_TOPIC!r}")
    print(f"  payload:          {payload.decode()}")
    print(f"  ResponseTopic:    {REPLY_TOPIC}")
    print(f"  CorrelationData:  {CORRELATION.hex()}")

    result = client.publish(
        QUERY_TOPIC,
        payload=payload,
        qos=1,
        properties=pub_props,
    )
    result.wait_for_publish(timeout=10)
    # NOTE: wait_for_publish() returning means PUBACK was received.
    # It does NOT mean the query response has arrived.

    # ------------------------------------------------------------------
    # Step 3: wait for response on the reply inbox.
    # The broker forwards the response only after the gRPC responder replies.
    # Without a responder the connector logs rpc.timeout after RpcTimeoutSeconds.
    # ------------------------------------------------------------------
    rpc_timeout = int(os.environ.get("KUBEMQ_RPC_TIMEOUT", "30"))
    print(f"[client] waiting up to {rpc_timeout}s for query response on {REPLY_TOPIC!r}...")

    if not response_received.wait(timeout=rpc_timeout + 2):
        print(
            f"[client] TIMEOUT — no response within {rpc_timeout}s.\n"
            "  Ensure a gRPC-side responder is running on channel 'demo.rpc' (type=query).\n"
            "  Start it with:\n"
            "    cd kubemq-mqtt/.work/test-harness/responder\n"
            "    go run . -channel demo.rpc -type query"
        )
    else:
        resp = response_data[0]
        corr_echo = resp["correlation"]
        corr_match = corr_echo == CORRELATION
        print("[client] query response received:")
        print(f"  inbox topic:      {resp['topic']}")
        print(f"  payload:          {resp['payload'].decode() if resp['payload'] else '(empty)'}")
        print(f"  CorrelationData:  {corr_echo.hex() if corr_echo else None} "
              f"({'matched' if corr_match else 'MISMATCH!'})")
        print("  user_props:")
        for k, v in resp["user_props"].items():
            print(f"    {k}: {v}")

    client.loop_stop()
    client.disconnect()
    print("[client] disconnected")


if __name__ == "__main__":
    run()

# ---------------------------------------------------------------------------
# Expected output (with gRPC responder running on channel demo.rpc):
#
# [client] connecting to tcp://localhost:1883 as client-id='python-query-rpc-client'
# [client] connected (rc=0x00)
# [client] subscribing to reply inbox '$reply/python-query-rpc-client/inbox' (QoS 1)
# [client] SUBACK 0x01 — subscribed to reply inbox OK
# [client] publishing query to 'queries/demo/rpc'
#   payload:          {"query": "hello from MQTT"}
#   ResponseTopic:    $reply/python-query-rpc-client/inbox
#   CorrelationData:  <hex>
# [client] PUBACK 0x00 — broker accepted the query (PUBACK is immediate; response arrives separately)
# [client] waiting up to 30s for query response on '$reply/python-query-rpc-client/inbox'...
# [client] query response received:
#   inbox topic:      $reply/python-query-rpc-client/inbox
#   payload:          pong:{"query": "hello from MQTT"}
#   CorrelationData:  <hex> (matched)
#   user_props:
#     kubemq-metadata: responder/query
#     responder: mqtt-test
#     rpc-type: query
# [client] disconnected
# ---------------------------------------------------------------------------
