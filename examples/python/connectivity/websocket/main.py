"""
Example: connectivity/websocket

Demonstrates connecting to the KubeMQ MQTT connector over WebSocket
(port 8083, path ``/``) using MQTT 5.0.

WebSocket URL : ws://localhost:8083/
  paho-mqtt transport = "websockets"

The KubeMQ MQTT connector exposes three transport listeners:
  tcp://host:1883   — plain TCP (default)
  tls://host:8883   — TLS over TCP
  ws://host:8083/   — MQTT over WebSocket

This example exercises the WebSocket listener with MQTT 5.0:
  - A subscriber connects over ``ws://`` and subscribes to ``events/demo/ws`` (QoS 1).
  - A publisher connects over ``ws://`` and publishes one MQTT 5.0 message carrying a
    user property (``transport=websocket``), which is forwarded as a KubeMQ Tag.
  - The subscriber confirms receipt and prints the user properties.

Topic prefix ``events/`` selects the KubeMQ Events pattern (fire-and-forget pub/sub).
``/`` in the topic is translated to ``.`` in the KubeMQ channel name:
  MQTT topic  events/demo/ws
  KubeMQ ch   demo.ws

MQTT 5.0 User Properties are forwarded 1:1 as KubeMQ message Tags.
Cap: 32 properties / 4096 bytes total — exceeding the cap returns PUBACK 0x97.

Run:
  export KUBEMQ_MQTT_URL=ws://localhost:8083/
  python connectivity/websocket/main.py
"""

from __future__ import annotations

import os
import threading
import time

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode


# ---------------------------------------------------------------------------
# Broker URL parsing
# ---------------------------------------------------------------------------

def broker_url() -> str:
    return os.environ.get("KUBEMQ_MQTT_URL", "ws://localhost:8083/")


def parse_url(url: str) -> tuple[str, str, int]:
    """Return (scheme, host, port) from a tcp/tls/ws URL."""
    scheme, rest = url.split("://", 1)
    host_port = rest.rstrip("/")
    if ":" in host_port:
        host, port_str = host_port.rsplit(":", 1)
        port = int(port_str)
    else:
        host = host_port
        port = {"tcp": 1883, "tls": 8883, "ws": 8083}.get(scheme, 1883)
    return scheme, host, port


def rc_value(rc: ReasonCode | int) -> int:
    """Extract the integer reason code from a paho ReasonCode (or plain int)."""
    return rc.value if isinstance(rc, ReasonCode) else int(rc)


def make_client(client_id: str, scheme: str) -> mqtt.Client:
    """Create an MQTTv5 client using the modern (VERSION2) callback API."""
    return mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme in ("ws", "wss") else "tcp",
    )


# ---------------------------------------------------------------------------
# Example
# ---------------------------------------------------------------------------

TOPIC = "events/demo/ws"
CHANNEL = "demo.ws"  # KubeMQ channel after "/" -> "." translation


def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)
    transport = "websockets" if scheme in ("ws", "wss") else "tcp"

    print(f"Broker     : {url}  (scheme={scheme}, host={host}, port={port})")
    print(f"Transport  : {transport}")
    print(f"MQTT topic : {TOPIC}")
    print(f"KubeMQ ch  : {CHANNEL}  (/ -> . translation)")

    received_event = threading.Event()
    received_payload: list[bytes] = []
    received_user_props: list[list] = []

    # -----------------------------------------------------------------------
    # Subscriber — connects over WebSocket, subscribes to events/demo/ws
    # -----------------------------------------------------------------------
    sub = make_client("python-ws-sub", scheme)

    # VERSION2: on_connect(client, userdata, connect_flags, reason_code, properties)
    def on_sub_connect(
        client: mqtt.Client,
        userdata: object,
        connect_flags: object,
        reason_code: ReasonCode,
        properties: object,
    ) -> None:
        code = rc_value(reason_code)
        if code == 0:
            print(f"[subscriber] Connected over WebSocket to ws://{host}:{port}/")
            client.subscribe(TOPIC, qos=1)
        elif code == 0x80:
            print("[subscriber] CONNACK 0x80: broker not ready (KubeMQ still starting)")
        elif code == 0x86:
            print("[subscriber] CONNACK 0x86: bad username or password")
        elif code == 0x87:
            print("[subscriber] CONNACK 0x87: not authorized (ACL denied)")
        else:
            print(f"[subscriber] Connection failed: reason code 0x{code:02x}")

    # VERSION2: on_subscribe(client, userdata, mid, reason_codes, properties)
    def on_subscribe(
        client: mqtt.Client,
        userdata: object,
        mid: int,
        reason_codes: list[ReasonCode],
        properties: object,
    ) -> None:
        code = rc_value(reason_codes[0]) if reason_codes else None
        print(f"[subscriber] SUBACK mid={mid} reason_code=0x{code:02x}")
        if code == 0xA2:
            print(
                "[subscriber] 0xA2: wildcard subscriptions not supported on "
                "non-events patterns — only events/# and events/+/... are valid"
            )

    def on_message(
        client: mqtt.Client,
        userdata: object,
        msg: mqtt.MQTTMessage,
    ) -> None:
        user_props: list = []
        if msg.properties and hasattr(msg.properties, "UserProperty"):
            user_props = list(msg.properties.UserProperty)
        received_user_props.append(user_props)
        received_payload.append(msg.payload)
        print(f"[subscriber] Received on {msg.topic}")
        print(f"             payload    : {msg.payload.decode()!r}")
        print(f"             user-props : {user_props}  (forwarded as KubeMQ Tags)")
        received_event.set()

    sub.on_connect = on_sub_connect
    sub.on_subscribe = on_subscribe
    sub.on_message = on_message

    if scheme in ("tls", "wss"):
        sub.tls_set()

    # clean_start=True discards stale session state from previous runs.
    connect_props = Properties(PacketTypes.CONNECT)
    sub.connect(host, port, keepalive=30, clean_start=True,
                properties=connect_props)
    sub.loop_start()

    # Wait for subscription to establish before publishing.
    time.sleep(0.5)

    # -----------------------------------------------------------------------
    # Publisher — connects over WebSocket, publishes to events/demo/ws
    # -----------------------------------------------------------------------
    pub = make_client("python-ws-pub", scheme)

    if scheme in ("tls", "wss"):
        pub.tls_set()

    pub.connect(host, port, keepalive=30, clean_start=True)
    pub.loop_start()

    # Attach an MQTT 5.0 User Property: transport=websocket -> forwarded as KubeMQ Tag.
    pub_props = Properties(PacketTypes.PUBLISH)
    pub_props.UserProperty = [("transport", "websocket")]

    payload = b'{"via": "websocket"}'
    result = pub.publish(TOPIC, payload=payload, qos=1, properties=pub_props)
    result.wait_for_publish(timeout=10)
    print(
        f"[publisher]  Published to {TOPIC}  "
        f"payload={payload.decode()!r}  "
        f"user-prop transport=websocket  mid={result.mid}"
    )

    # -----------------------------------------------------------------------
    # Verify receipt
    # -----------------------------------------------------------------------
    if not received_event.wait(timeout=10):
        raise TimeoutError("Timed out waiting for the event to arrive via WebSocket")

    assert received_payload[0] == payload, "Payload mismatch"
    assert ("transport", "websocket") in received_user_props[0], \
        "User property 'transport=websocket' not received"
    print("[ok] payload and user-property (KubeMQ tag) round-trip verified over WebSocket")

    pub.loop_stop()
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()
    print("Done.")


if __name__ == "__main__":
    run()

# Expected output:
# Broker     : ws://localhost:8083/  (scheme=ws, host=localhost, port=8083)
# Transport  : websockets
# MQTT topic : events/demo/ws
# KubeMQ ch  : demo.ws  (/ -> . translation)
# [subscriber] Connected over WebSocket to ws://localhost:8083/
# [subscriber] SUBACK mid=1 reason_code=0x01
# [publisher]  Published to events/demo/ws  payload='{"via": "websocket"}'  user-prop transport=websocket  mid=1
# [subscriber] Received on events/demo/ws
#              payload    : '{"via": "websocket"}'
#              user-props : [('transport', 'websocket')]  (forwarded as KubeMQ Tags)
# [ok] payload and user-property (KubeMQ tag) round-trip verified over WebSocket
# Done.
