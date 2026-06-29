"""
Example: queues/ack_and_redelivery

Demonstrates KubeMQ Queues ack-on-PUBACK and redelivery over MQTT 5.0.

Ack model
---------
- PUBACK from the consumer IS the ack to KubeMQ.
- No PUBACK within QueueAckTimeoutSeconds (default 30 s) -> NAck -> requeued.
- Disconnect with an unacked in-flight message -> immediate NAck -> requeued (no loss).

This example demonstrates the disconnect-triggered redelivery path (faster than waiting
the full 30-second ack timeout):

  1. Consumer A subscribes on ``$share/g1/queues/demo/rq`` QoS 1 as the SOLE consumer,
     using ``manual_ack_set(True)`` so paho does NOT auto-send the PUBACK on receive.
  2. Producer publishes one message to ``queues/demo/rq`` (QoS 1); Consumer A receives
     it and holds it unacked in-flight.
     NOTE: paho-mqtt 2.x normally sends PUBACK automatically on QoS 1 receive; manual-ack
     suppresses that so the message stays unacked deterministically (no race needed).
  3. Consumer B connects on the same $share group (after A has the message, BEFORE A's
     disconnect completes), so it is ready to receive the requeue.
  4. Consumer A disconnects while holding the unacked message -> immediate NAck ->
     requeue; Consumer B receives the redelivered message and sends its PUBACK — the
     message is acked and removed from the queue.

Topic / $share note
-------------------
``$share/<group>/queues/<ch>`` — the group name is AUDIT/METRICS ONLY.
All groups compete in ONE shared KubeMQ queue pool (not per-group copies).
Using the same group name ``g1`` for both consumers is deliberate and correct;
changing it to ``g2`` would have exactly the same effect.

Negative paths (require QoS >= 1 for shared queue subscribe):
  - QoS 0 shared subscribe -> SUBACK 0x83 (test #8).
  - Plain ``queues/...`` subscribe (no ``$share``) -> SUBACK 0x83 (test #9).

Run:
  export KUBEMQ_MQTT_URL=tcp://localhost:1883
  python queues/ack_and_redelivery/main.py
"""

from __future__ import annotations

import os
import threading
import time

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.reasoncodes import ReasonCode


def broker_url() -> str:
    return os.environ.get("KUBEMQ_MQTT_URL", "tcp://localhost:1883")


def parse_url(url: str) -> tuple[str, str, int]:
    scheme, rest = url.split("://", 1)
    host_port = rest.rstrip("/")
    if ":" in host_port:
        host, port_str = host_port.rsplit(":", 1)
        port = int(port_str)
    else:
        host = host_port
        port = {"tcp": 1883, "tls": 8883, "ws": 8083}.get(scheme, 1883)
    return scheme, host, port


# Canonical topic strings per spec S6.1.5.
QUEUE_TOPIC = "queues/demo/rq"               # produce (plain publish)
SHARED_SUB = "$share/g1/queues/demo/rq"      # consume (QoS >= 1 required)


def rc_value(rc: ReasonCode | int) -> int:
    """Extract the integer reason code from a paho ReasonCode (or plain int)."""
    return rc.value if isinstance(rc, ReasonCode) else int(rc)


def make_client(client_id: str, scheme: str) -> mqtt.Client:
    return mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )


def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)
    print(f"Broker: {url}")
    print(f"Produce topic : {QUEUE_TOPIC}")
    print(f"Consume filter: {SHARED_SUB}  (QoS 1, MQTT 5.0 required)")
    print()

    # ------------------------------------------------------------------
    # Step 1: Consumer A subscribes FIRST as the SOLE consumer so the
    #         message is delivered to A (mirrors connector test #6:
    #         sub1 is the sole subscriber when the message is published).
    #
    #         Consumer A uses manual_ack=True so paho does NOT auto-send the
    #         PUBACK on receive — A holds the message unacked in-flight, then
    #         disconnects to trigger the NAck -> requeue path deterministically.
    #         (Without manual_ack, paho 2.x auto-acks QoS 1 immediately and the
    #         message would be removed before the disconnect could requeue it.)
    # ------------------------------------------------------------------
    received_a: list[bytes] = []
    a_subscribed = threading.Event()
    disconnect_done = threading.Event()

    consumer_a = make_client("python-rq-consumer-A", scheme)
    # Suppress paho's automatic PUBACK on QoS 1 so A holds the message unacked.
    consumer_a.manual_ack_set(True)
    if scheme == "tls":
        consumer_a.tls_set()

    def on_connect_a(client: mqtt.Client, userdata, connect_flags, rc, props):  # noqa: ANN001
        if rc_value(rc) == 0:
            client.subscribe(SHARED_SUB, qos=1)
            print(f"[consumer-A] subscribed to {SHARED_SUB} (QoS 1, manual-ack)")

    def on_subscribe_a(client: mqtt.Client, userdata, mid, rcs, props):  # noqa: ANN001
        a_subscribed.set()

    def on_message_a(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):  # noqa: ANN001
        received_a.append(msg.payload)
        print(f"[consumer-A] received (NOT acked): {msg.payload.decode()}")
        # Do NOT call client.ack(...) — leave the message unacked in-flight.
        # Disconnect -> connector detects the unacked in-flight message ->
        # immediate NAck -> requeue -> redelivered to Consumer B.
        # Slower alternative: never PUBACK within QueueAckTimeoutSeconds (30 s).
        def _disconnect():
            time.sleep(0.05)  # tiny yield to let the event loop tick
            client.disconnect()
            disconnect_done.set()
        threading.Thread(target=_disconnect, daemon=True).start()

    consumer_a.on_connect = on_connect_a
    consumer_a.on_subscribe = on_subscribe_a
    consumer_a.on_message = on_message_a
    consumer_a.connect(host, port, keepalive=30)
    consumer_a.loop_start()

    if not a_subscribed.wait(timeout=10):
        raise TimeoutError("Consumer A did not establish its subscription in time")
    time.sleep(0.3)  # let the shared-group registration settle

    # ------------------------------------------------------------------
    # Step 2: Publish one message — Consumer A is the sole consumer, so it
    #         receives it (and holds it unacked).
    # ------------------------------------------------------------------
    producer = make_client("python-rq-producer", scheme)
    if scheme == "tls":
        producer.tls_set()
    producer.connect(host, port, keepalive=30)
    producer.loop_start()

    payload = b'{"task": "redelivery-demo", "attempt": 1}'
    result = producer.publish(QUEUE_TOPIC, payload=payload, qos=1)
    result.wait_for_publish(timeout=10)
    print(f"[producer] published to {QUEUE_TOPIC}: {payload.decode()}")
    producer.loop_stop()
    producer.disconnect()

    # Wait for A to receive the message.
    deadline = time.time() + 10
    while not received_a and time.time() < deadline:
        time.sleep(0.05)

    if not received_a:
        raise TimeoutError("Consumer A did not receive the message within 10 s")

    # ------------------------------------------------------------------
    # Step 3: Consumer B connects on the same $share group BEFORE A's
    #         disconnect completes, so it is ready to receive the requeue.
    #         (Mirrors test #6: sub2 connects before sub1 disconnects.)
    # ------------------------------------------------------------------
    received_b: list[bytes] = []
    done_b = threading.Event()

    consumer_b = make_client("python-rq-consumer-B", scheme)
    if scheme == "tls":
        consumer_b.tls_set()

    def on_connect_b(client: mqtt.Client, userdata, connect_flags, rc, props):  # noqa: ANN001
        if rc_value(rc) == 0:
            client.subscribe(SHARED_SUB, qos=1)
            print(f"[consumer-B] subscribed to {SHARED_SUB} (QoS 1)")

    def on_message_b(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):  # noqa: ANN001
        received_b.append(msg.payload)
        print(f"[consumer-B] received redelivered: {msg.payload.decode()}")
        done_b.set()

    consumer_b.on_connect = on_connect_b
    consumer_b.on_message = on_message_b
    consumer_b.connect(host, port, keepalive=30)
    consumer_b.loop_start()
    time.sleep(0.4)  # let B's subscription register before A disconnects

    # Wait for A's disconnect to complete -> NAck -> requeue.
    disconnect_done.wait(timeout=5)
    consumer_a.loop_stop()
    print("[consumer-A] disconnected (unacked) — message requeued for redelivery")
    print("             Alternatively: QueueAckTimeoutSeconds (default 30 s) also triggers requeue.")

    # ------------------------------------------------------------------
    # Step 4: Consumer B receives the redelivered message.
    # ------------------------------------------------------------------
    if not done_b.wait(timeout=35):
        # 35 s covers the default 30 s ack-timeout fallback (test #7).
        print("[consumer-B] WARNING: redelivery not observed within 35 s.")
        print("  This can happen if Consumer A's PUBACK was flushed before the disconnect.")
        print("  The message will eventually be requeued by the ack-timeout (30 s default).")
    else:
        print("[consumer-B] PUBACK sent — message acked and removed from queue.")
        print()
        print("Done. At-least-once delivery confirmed:")
        print(f"  A received : {received_a[0].decode()}")
        print(f"  B received : {received_b[0].decode()}")

    consumer_b.loop_stop()
    consumer_b.disconnect()


if __name__ == "__main__":
    run()

# Expected output (timing-sensitive — A's PUBACK may or may not race the disconnect):
#
# Broker: tcp://localhost:1883
# Produce topic : queues/demo/rq
# Consume filter: $share/g1/queues/demo/rq  (QoS 1, MQTT 5.0 required)
#
# [consumer-A] subscribed to $share/g1/queues/demo/rq (QoS 1, manual-ack)
# [producer] published to queues/demo/rq: {"task": "redelivery-demo", "attempt": 1}
# [consumer-A] received (NOT acked): {"task": "redelivery-demo", "attempt": 1}
# [consumer-B] subscribed to $share/g1/queues/demo/rq (QoS 1)
# [consumer-A] disconnected (unacked) — message requeued for redelivery
#              Alternatively: QueueAckTimeoutSeconds (default 30 s) also triggers requeue.
# [consumer-B] received redelivered: {"task": "redelivery-demo", "attempt": 1}
# [consumer-B] PUBACK sent — message acked and removed from queue.
#
# Done. At-least-once delivery confirmed:
#   A received : {"task": "redelivery-demo", "attempt": 1}
#   B received : {"task": "redelivery-demo", "attempt": 1}
