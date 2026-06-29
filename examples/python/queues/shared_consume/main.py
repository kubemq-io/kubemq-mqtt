"""
Example: queues/shared_consume

Demonstrates KubeMQ Queues via MQTT 5.0 shared subscriptions.

PRODUCE:  publish to ``queues/demo/q`` (plain topic, any QoS).
CONSUME:  MQTT 5.0 shared subscription ``$share/g1/queues/demo/q`` at QoS 1.

Wire-contract rules enforced by the connector:
  - QoS 0 shared queue subscribe -> SUBACK 0x83 (rejected) [test #8].
  - Plain ``queues/...`` subscribe (no ``$share`` prefix) -> SUBACK 0x83 [test #9].
  - MQTT 5.0 is REQUIRED; v3.1.1 clients cannot use shared queue consume.
  - The ``$share`` group name is audit/metrics-only.  ALL groups — regardless of
    name — compete in ONE shared KubeMQ queue pool.  This is NOT per-group copies
    (gotcha #3).
  - PUBACK from the consumer IS the ack to KubeMQ.  No PUBACK within
    QueueAckTimeoutSeconds (default 30 s) -> message requeued for redelivery.

Topic translation:
  MQTT topic  queues/demo/q          -> KubeMQ channel demo.q
  $share sub  $share/g1/queues/demo/q

Run:
  export KUBEMQ_MQTT_URL=tcp://localhost:1883
  python queues/shared_consume/main.py
"""

from __future__ import annotations

import os
import threading

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
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


# ---------------------------------------------------------------------------
# Canonical topic strings (spec S6.1.4)
# ---------------------------------------------------------------------------

QUEUE_TOPIC = "queues/demo/q"                    # produce plain publish
SHARED_SUB = "$share/g1/queues/demo/q"           # shared-subscription consume (QoS 1)
KUBEMQ_CHANNEL = "demo.q"                        # after / -> . translation
NUM_MESSAGES = 3


def rc_value(rc: ReasonCode | int) -> int:
    """Extract the integer reason code from a paho ReasonCode (or plain int)."""
    return rc.value if isinstance(rc, ReasonCode) else int(rc)


# ---------------------------------------------------------------------------
# Example
# ---------------------------------------------------------------------------

def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)
    print(f"Broker: {url}  (scheme={scheme}, host={host}, port={port})")
    print(f"Produce topic : {QUEUE_TOPIC}")
    print(f"Consume filter: {SHARED_SUB}  (QoS 1)")
    print(f"KubeMQ channel: {KUBEMQ_CHANNEL}  (/ -> . translation)")
    print()

    received: list[bytes] = []
    done = threading.Event()
    subscribed = threading.Event()

    # --- Consumer (shared subscription, QoS 1) ---
    # MQTT 5.0 required; QoS must be >= 1 (QoS 0 -> SUBACK 0x83).
    consumer = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-queue-shared-consumer",
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )

    # VERSION2: on_connect(client, userdata, connect_flags, reason_code, properties)
    def on_connect(client: mqtt.Client, userdata, connect_flags, reason_code, props) -> None:  # noqa: ANN001
        if rc_value(reason_code) == 0:
            # QoS must be >= 1; QoS 0 -> SUBACK 0x83 (rejected by connector).
            client.subscribe(SHARED_SUB, qos=1)
            print(f"[consumer] connected; subscribing to {SHARED_SUB}")
        else:
            print(f"[consumer] connect failed, reason_code=0x{rc_value(reason_code):02x}")

    def on_subscribe(client: mqtt.Client, userdata, mid, reason_codes, props) -> None:  # noqa: ANN001
        code = reason_codes[0] if reason_codes else None
        if code is not None and code.is_failure:
            # 0x83 = impl-specific (QoS0 queue sub or plain queues/ sub rejected)
            print(f"[consumer] SUBACK mid={mid} reason_code={code.value:#04x}  "
                  f"({code!s})  -- subscribe rejected")
        else:
            val = code.value if code is not None else None
            print(f"[consumer] SUBACK mid={mid} reason_code={val}  (subscribed OK)")
            subscribed.set()

    def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:  # noqa: ANN001
        received.append(msg.payload)
        print(f"[consumer] received msg {len(received)}/{NUM_MESSAGES}: "
              f"{msg.payload.decode()!r}  (PUBACK = ack to KubeMQ)")
        if len(received) >= NUM_MESSAGES:
            done.set()

    consumer.on_connect = on_connect
    consumer.on_subscribe = on_subscribe
    consumer.on_message = on_message

    if scheme == "tls":
        consumer.tls_set()
    consumer.connect(host, port, keepalive=30)
    consumer.loop_start()

    # Wait until the SUBACK confirms the shared subscription is established.
    if not subscribed.wait(timeout=10):
        raise TimeoutError("Timed out waiting for SUBACK on shared subscription")

    print()

    # --- Producer ---
    # Publish to plain ``queues/<ch>`` — no $share prefix on the publish topic.
    producer = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-queue-shared-producer",
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )
    if scheme == "tls":
        producer.tls_set()
    producer.connect(host, port, keepalive=30)
    producer.loop_start()

    for i in range(1, NUM_MESSAGES + 1):
        payload = f'{{"order_id": {i}, "item": "widget-{i}"}}'.encode()
        result = producer.publish(QUEUE_TOPIC, payload=payload, qos=1)
        result.wait_for_publish(timeout=10)
        print(f"[producer] published order {i} to {QUEUE_TOPIC}")

    print()

    # Wait for all messages to be consumed and acked.
    if not done.wait(timeout=15):
        raise TimeoutError(
            f"Expected {NUM_MESSAGES} messages, got {len(received)}. "
            "Check that KUBEMQ_MQTT_URL points to a running broker."
        )

    producer.loop_stop()
    producer.disconnect()
    consumer.loop_stop()
    consumer.disconnect()

    print()
    print(f"Done. Round-trip verified: produced {NUM_MESSAGES}, "
          f"consumed {len(received)} via shared subscription.")
    print()
    print("Negative paths (not triggered in this run — for reference):")
    print("  QoS 0 shared-sub  -> SUBACK 0x83  [test #8]")
    print("  Plain queues/ sub -> SUBACK 0x83  [test #9]")


if __name__ == "__main__":
    run()

# Expected output:
# Broker: tcp://localhost:1883  (scheme=tcp, host=localhost, port=1883)
# Produce topic : queues/demo/q
# Consume filter: $share/g1/queues/demo/q  (QoS 1)
# KubeMQ channel: demo.q  (/ -> . translation)
#
# [consumer] connected; subscribing to $share/g1/queues/demo/q
# [consumer] SUBACK mid=1 reason_code=1  (subscribed OK)
#
# [producer] published order 1 to queues/demo/q
# [producer] published order 2 to queues/demo/q
# [producer] published order 3 to queues/demo/q
#
# [consumer] received msg 1/3: '{"order_id": 1, "item": "widget-1"}'  (PUBACK = ack to KubeMQ)
# [consumer] received msg 2/3: '{"order_id": 2, "item": "widget-2"}'  (PUBACK = ack to KubeMQ)
# [consumer] received msg 3/3: '{"order_id": 3, "item": "widget-3"}'  (PUBACK = ack to KubeMQ)
#
# Done. Round-trip verified: produced 3, consumed 3 via shared subscription.
#
# Negative paths (not triggered in this run — for reference):
#   QoS 0 shared-sub  -> SUBACK 0x83  [test #8]
#   Plain queues/ sub -> SUBACK 0x83  [test #9]
