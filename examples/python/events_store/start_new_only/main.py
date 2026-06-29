"""
Example: events_store/start_new_only

Demonstrates the StartNewOnly constraint for Events-Store over the KubeMQ MQTT
connector.

KEY TEACHING POINT — MQTT Events-Store is ALWAYS StartNewOnly:
  Messages published BEFORE a subscription is established are NOT delivered to
  that subscriber.  There is NO historical replay over MQTT (unlike the KubeMQ
  gRPC API which offers StartFromFirst, StartFromSequence, StartFromTime, etc.).
  This is a permanent V1 constraint of the MQTT connector, not a configuration
  option.

Flow (mirrors TestMqttIntegration_EventsStoreStartNewOnly in connector_integration_test.go):
  1. Publisher sends 3 messages to store/demo/s  (PRE-subscribe — NOT replayed)
  2. Subscriber subscribes to store/demo/s
  3. Publisher sends 1 message  (POST-subscribe — received)
  4. Example asserts (a) NONE of this run's pre-subscribe messages were replayed
     and (b) this run's post-subscribe message arrived.

Note on the shared channel:
  store/demo/s is a canonical demo channel that may carry concurrent traffic from
  other clients.  To stay correct under that traffic, every message this run
  publishes is tagged with a unique per-run "run_id".  The assertions only count
  THIS run's tagged messages; unrelated foreign messages are ignored.  This keeps
  the StartNewOnly guarantee (no replay of pre-subscribe messages) provable
  without asserting a brittle exact total against the shared channel.

Topic mapping:
  MQTT topic  store/demo/s
  KubeMQ ch   demo.s        ('/' -> '.' in channel name)

Run:
  export KUBEMQ_MQTT_URL=tcp://localhost:1883
  python events_store/start_new_only/main.py
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid

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
# Example
# ---------------------------------------------------------------------------

TOPIC = "store/demo/s"
CHANNEL = "demo.s"  # KubeMQ channel after '/' -> '.' translation


def rc_value(rc: ReasonCode | int) -> int:
    """Extract the integer reason code from a paho ReasonCode (or plain int)."""
    return rc.value if isinstance(rc, ReasonCode) else int(rc)


def make_client(client_id: str, scheme: str) -> mqtt.Client:
    """Create a connected MQTT 5.0 client (VERSION2 callback API)."""
    return mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )


def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)

    # Unique per-run marker so the assertions only count THIS run's messages and
    # stay correct under concurrent foreign traffic on the shared channel.
    run_id = uuid.uuid4().hex

    print(f"Connecting to broker at {url}")
    print(f"Topic: {TOPIC}  ->  KubeMQ channel: {CHANNEL}")
    print(f"Run ID: {run_id}")
    print()

    # -----------------------------------------------------------------------
    # Step 1: Publish 3 messages BEFORE any subscriber exists.
    # These are persisted in Events-Store but will NOT be replayed when the
    # subscriber connects later (StartNewOnly constraint).
    # -----------------------------------------------------------------------
    pub = make_client("python-es-demo-pub", scheme)
    if scheme == "tls":
        pub.tls_set()
    pub.connect(host, port, keepalive=30)
    pub.loop_start()
    # Give the connection a moment to complete.
    time.sleep(0.3)

    # Tag every message with run_id + phase so we can tell this run's
    # pre-subscribe messages apart from both its post-subscribe message and any
    # unrelated foreign traffic on the shared channel.
    pre_sub_messages = [
        json.dumps({"event": "login", "user": "alice", "seq": 1,
                    "run_id": run_id, "phase": "pre"}).encode(),
        json.dumps({"event": "login", "user": "bob", "seq": 2,
                    "run_id": run_id, "phase": "pre"}).encode(),
        json.dumps({"event": "login", "user": "carol", "seq": 3,
                    "run_id": run_id, "phase": "pre"}).encode(),
    ]
    print("--- Phase 1: Publishing 3 messages BEFORE subscriber exists ---")
    for payload in pre_sub_messages:
        result = pub.publish(TOPIC, payload=payload, qos=1)
        result.wait_for_publish(timeout=10)
        print(f"[publisher] pre-subscribe publish: {payload.decode()}")
    print("[publisher] 3 pre-subscribe messages stored in Events-Store")
    print()

    # Wait a moment so all 3 are durably stored before subscriber appears.
    time.sleep(0.5)

    # -----------------------------------------------------------------------
    # Step 2: Subscriber connects and subscribes (StartNewOnly — no replay).
    # -----------------------------------------------------------------------
    # Track only THIS run's messages (matched by run_id), split by phase.
    # Foreign messages on the shared channel are counted separately and ignored
    # by the assertions.
    own_pre: list[bytes] = []
    own_post: list[bytes] = []
    foreign: list[bytes] = []
    sub_ready = threading.Event()
    message_arrived = threading.Event()

    sub = make_client("python-es-demo-sub", scheme)
    if scheme == "tls":
        sub.tls_set()

    # VERSION2: on_connect(client, userdata, connect_flags, reason_code, properties)
    def on_connect(client: mqtt.Client, userdata, connect_flags, reason_code, properties):  # noqa: ANN001
        if rc_value(reason_code) == 0:
            client.subscribe(TOPIC, qos=1)
            print(f"[subscriber] subscribed to {TOPIC}")
            print("[subscriber] CONSTRAINT: StartNewOnly — pre-subscribe messages are NOT replayed")
            sub_ready.set()
        else:
            print(f"[subscriber] CONNACK error: reason_code=0x{rc_value(reason_code):02x}")

    def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):  # noqa: ANN001
        # Classify by run_id so concurrent foreign traffic on the shared channel
        # does not break the StartNewOnly assertions.
        try:
            data = json.loads(msg.payload)
        except (ValueError, TypeError):
            data = {}
        if not isinstance(data, dict):
            data = {}
        if data.get("run_id") != run_id:
            foreign.append(msg.payload)
            print(f"[subscriber] ignoring foreign message on {msg.topic}: {msg.payload.decode(errors='replace')}")
            return
        if data.get("phase") == "pre":
            own_pre.append(msg.payload)
        else:
            own_post.append(msg.payload)
        print(f"[subscriber] received on {msg.topic}: {msg.payload.decode()}")
        if own_post:
            message_arrived.set()

    sub.on_connect = on_connect
    sub.on_message = on_message

    print("--- Phase 2: Subscriber connects (StartNewOnly — NO replay of pre-subscribe msgs) ---")
    sub.connect(host, port, keepalive=30)
    sub.loop_start()

    # Wait for subscription to be live before publishing post-subscribe message.
    if not sub_ready.wait(timeout=10):
        raise TimeoutError("Subscriber did not connect and subscribe in time")
    # Extra settle time matching the integration test (700 ms).
    time.sleep(0.7)
    print()

    # -----------------------------------------------------------------------
    # Step 3: Publish 1 message AFTER the subscriber is live.
    # Only this message will be delivered.
    # -----------------------------------------------------------------------
    post_sub_payload = json.dumps({
        "event": "login", "user": "dave", "seq": 4, "note": "post-subscribe",
        "run_id": run_id, "phase": "post",
    }).encode()
    print("--- Phase 3: Publishing 1 message AFTER subscriber is live ---")
    result = pub.publish(TOPIC, payload=post_sub_payload, qos=1)
    result.wait_for_publish(timeout=10)
    print(f"[publisher] post-subscribe publish: {post_sub_payload.decode()}")
    print()

    # -----------------------------------------------------------------------
    # Step 4: Assert only the post-subscribe message arrived.
    # -----------------------------------------------------------------------
    if not message_arrived.wait(timeout=10):
        raise TimeoutError("Timed out waiting for post-subscribe Events-Store message")

    # Give a brief window to catch any unexpected replay messages.
    time.sleep(0.5)

    print("--- Results ---")
    print("[assertion] pre-subscribe messages published      : 3")
    print(f"[assertion] this run's pre-subscribe msgs replayed: {len(own_pre)} (StartNewOnly — expect 0)")
    print(f"[assertion] this run's post-subscribe msgs received: {len(own_post)} (expect >= 1)")
    print(f"[assertion] foreign messages ignored              : {len(foreign)}")
    # (a) The StartNewOnly guarantee: NONE of this run's pre-subscribe messages
    #     are replayed to a subscription that started after they were published.
    assert len(own_pre) == 0, (
        f"StartNewOnly violation: {len(own_pre)} pre-subscribe message(s) were replayed"
    )
    # (b) This run's uniquely-tagged post-subscribe message IS delivered.
    assert post_sub_payload in own_post, (
        f"Expected this run's post-subscribe message but got: {own_post}"
    )
    print("[assertion] PASS — no pre-subscribe replay; this run's post-subscribe message delivered")
    print()

    # Cleanup.
    pub.loop_stop()
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()
    print("Done.")


if __name__ == "__main__":
    run()

# Expected output:
# Connecting to broker at tcp://localhost:1883
# Topic: store/demo/s  ->  KubeMQ channel: demo.s
#
# --- Phase 1: Publishing 3 messages BEFORE subscriber exists ---
# [publisher] pre-subscribe publish: {"event": "login", "user": "alice", "seq": 1}
# [publisher] pre-subscribe publish: {"event": "login", "user": "bob",   "seq": 2}
# [publisher] pre-subscribe publish: {"event": "login", "user": "carol", "seq": 3}
# [publisher] 3 pre-subscribe messages stored in Events-Store
#
# --- Phase 2: Subscriber connects (StartNewOnly — NO replay of pre-subscribe msgs) ---
# [subscriber] subscribed to store/demo/s
# [subscriber] CONSTRAINT: StartNewOnly — pre-subscribe messages are NOT replayed
#
# --- Phase 3: Publishing 1 message AFTER subscriber is live ---
# [publisher] post-subscribe publish: {"event": "login", "user": "dave", "seq": 4, "note": "post-subscribe"}
#
# --- Results ---
# [assertion] pre-subscribe messages published      : 3
# [assertion] this run's pre-subscribe msgs replayed: 0 (StartNewOnly — expect 0)
# [assertion] this run's post-subscribe msgs received: 1 (expect >= 1)
# [assertion] foreign messages ignored              : 0
# [assertion] PASS — no pre-subscribe replay; this run's post-subscribe message delivered
#
# Done.
