"""
Example: events/basic_pubsub

Demonstrates basic fire-and-forget event pub/sub over the KubeMQ MQTT connector.

A subscriber connects with MQTT 5.0 and subscribes to the single-level wildcard
``events/demo/+`` at QoS 1.  A publisher connects and publishes one MQTT 5.0 message to
the concrete topic ``events/demo/x`` (which the ``+`` wildcard matches) carrying a
KubeMQ user property (``k1=v1``).  The subscriber prints the received payload and user
properties, then the example exits cleanly.

The ``+`` single-level wildcard is the canonical Events subscribe form (S6.1.1): the
subscriber registers ``events/demo/+`` once and receives every event published under
``events/demo/<anything>``.  Wildcard subscriptions are permitted on the Events pattern
ONLY (``+`` -> KubeMQ ``*``; ``#`` -> KubeMQ ``>``).

Topic prefix ``events/`` selects the KubeMQ Events pattern (fire-and-forget pub/sub).
``/`` is translated to ``.`` in the KubeMQ channel name:
  MQTT subscribe  events/demo/+      (KubeMQ channel demo.*)
  MQTT publish    events/demo/x      (KubeMQ channel demo.x)

MQTT 5.0 User Properties are forwarded 1:1 as KubeMQ message Tags.
Cap: 32 properties / 4096 bytes total — exceeding the cap returns PUBACK 0x97.

Run:
  export KUBEMQ_MQTT_URL=tcp://localhost:1883
  python events/basic_pubsub/main.py
"""

from __future__ import annotations

import os
import threading
import time

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode


# ---------------------------------------------------------------------------
# Broker URL parsing
# ---------------------------------------------------------------------------

def broker_url() -> str:
    return os.environ.get("KUBEMQ_MQTT_URL", "tcp://localhost:1883")


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


# ---------------------------------------------------------------------------
# Example
# ---------------------------------------------------------------------------

# Subscribe with the single-level '+' wildcard (canonical Events subscribe form,
# S6.1.1). Publish to a concrete topic the wildcard matches.
SUB_TOPIC = "events/demo/+"  # KubeMQ channel demo.*  ('+' -> '*')
PUB_TOPIC = "events/demo/x"  # KubeMQ channel demo.x
CHANNEL = "demo.x"  # KubeMQ channel for the published topic ("/" -> "." translation)


def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)
    print(f"Broker: {url}  (scheme={scheme}, host={host}, port={port})")
    print(f"MQTT subscribe : {SUB_TOPIC}  (single-level '+' wildcard -> KubeMQ demo.*)")
    print(f"MQTT publish   : {PUB_TOPIC}")
    print(f"KubeMQ ch      : {CHANNEL}  (/ -> . translation)")

    received_event = threading.Event()
    received_payload: list[bytes] = []
    received_user_props: list[list] = []

    # --- Subscriber ---
    sub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-events-basic-sub",
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )

    # VERSION2: on_connect(client, userdata, connect_flags, reason_code, properties)
    def on_sub_connect(
        client: mqtt.Client, userdata, connect_flags, reason_code, properties
    ) -> None:
        if rc_value(reason_code) == 0:
            client.subscribe(SUB_TOPIC, qos=1)
        else:
            print(f"[subscriber] connect failed, reason_code=0x{rc_value(reason_code):02x}")

    # VERSION2: on_subscribe(client, userdata, mid, reason_code_list, properties)
    def on_subscribe(
        client: mqtt.Client, userdata, mid, reason_codes, properties
    ) -> None:
        code = rc_value(reason_codes[0]) if reason_codes else None
        print(f"[subscriber] SUBACK mid={mid} reason_code={code}")

    def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
        # Collect MQTT 5.0 User Properties carried on the PUBLISH.
        user_props: list = []
        if msg.properties and hasattr(msg.properties, "UserProperty"):
            user_props = list(msg.properties.UserProperty)
        received_user_props.append(user_props)
        received_payload.append(msg.payload)
        print(f"[subscriber] received on {msg.topic}")
        print(f"             payload       : {msg.payload.decode()!r}")
        print(f"             user-props    : {user_props}  (forwarded as KubeMQ Tags)")
        received_event.set()

    sub.on_connect = on_sub_connect
    sub.on_subscribe = on_subscribe
    sub.on_message = on_message

    if scheme == "tls":
        sub.tls_set()

    # clean_start=True discards any stale session state from previous runs.
    connect_props = Properties(PacketTypes.CONNECT)
    sub.connect(host, port, keepalive=30, clean_start=True,
                properties=connect_props)
    sub.loop_start()

    # Wait for subscription to establish before publishing.
    time.sleep(0.5)

    # --- Publisher ---
    pub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-events-basic-pub",
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )

    if scheme == "tls":
        pub.tls_set()

    pub.connect(host, port, keepalive=30, clean_start=True)
    pub.loop_start()

    # Attach an MQTT 5.0 User Property: k1=v1 -> forwarded as KubeMQ Tag.
    pub_props = Properties(PacketTypes.PUBLISH)
    pub_props.UserProperty = [("k1", "v1")]

    payload = b"hello"
    result = pub.publish(PUB_TOPIC, payload=payload, qos=1, properties=pub_props)
    result.wait_for_publish(timeout=10)
    print(f"[publisher]  published to {PUB_TOPIC}  payload={payload.decode()!r}  "
          f"user-prop k1=v1  mid={result.mid}")

    # Wait for the subscriber to receive.
    if not received_event.wait(timeout=10):
        raise TimeoutError("Timed out waiting for the event to arrive")

    assert received_payload[0] == payload, "Payload mismatch"
    assert ("k1", "v1") in received_user_props[0], "User property k1=v1 not received"
    print("[ok] payload and user-property (KubeMQ tag) round-trip verified")

    pub.loop_stop()
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()
    print("Done.")


if __name__ == "__main__":
    run()

# Expected output:
# Broker: tcp://localhost:1883  (scheme=tcp, host=localhost, port=1883)
# MQTT subscribe : events/demo/+  (single-level '+' wildcard -> KubeMQ demo.*)
# MQTT publish   : events/demo/x
# KubeMQ ch      : demo.x  (/ -> . translation)
# [subscriber] SUBACK mid=1 reason_code=1
# [publisher]  published to events/demo/x  payload='hello'  user-prop k1=v1  mid=1
# [subscriber] received on events/demo/x
#              payload       : 'hello'
#              user-props    : [('k1', 'v1')]  (forwarded as KubeMQ Tags)
# [ok] payload and user-property (KubeMQ tag) round-trip verified
# Done.
