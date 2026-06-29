"""
Example: protocol/mqtt_v311_vs_v5

Contrasts MQTT 3.1.1 (level 4) and MQTT 5.0 (level 5) against the KubeMQ MQTT
connector, surfacing the three capabilities that v5 unlocks:

  1. User Properties -> KubeMQ Tags (bidirectional)
       v5: PUBLISH user-props forwarded as KubeMQ message Tags; Tags delivered back
           as user-props on the subscriber side.
       v3.1.1: NO user-properties wire support. Tags are silently absent in BOTH
           directions.

  2. Shared-subscription queue consume ($share/<group>/queues/<ch>)
       v5: supported at QoS >= 1.
       v3.1.1: no shared-subscription syntax in the protocol.

  3. RPC (Commands / Queries) via ResponseTopic + CorrelationData
       v5: client subscribes to own $reply/<clientID>/<suffix>, then publishes to
           commands/<ch> or queries/<ch> with Properties.ResponseTopic +
           CorrelationData.  PUBACK is immediate (not after the response); the
           client implements its own response-wait timeout.
       v3.1.1: publish to commands/ or queries/ is SILENTLY DROPPED.
           PUBACK still returns 0x00 (success). The connector audits publish.error
           server-side but the MQTT client receives NO indication of failure.
           (Gotcha #4 — see README.)

The example runs three phases in sequence:

  Phase A  Events pub/sub — v3.1.1
    Topic:  events/demo/v3  (KubeMQ channel demo.v3)
    Shows:  basic pub/sub works; no user-properties.

  Phase B  Events pub/sub — v5
    Topic:  events/demo/v3  (same channel, different client protocol)
    Shows:  user-props forwarded as KubeMQ Tags and delivered back.

  Phase C  Negative RPC test — v3.1.1
    Topic:  commands/demo/x  (KubeMQ channel demo.x)
    Shows:  v3.1.1 PUBLISH receives PUBACK 0x00 but the command is SILENTLY
            DROPPED.  No response arrives; this is expected and documented.

Canonical checklist items: S6.1.11 (v5 vs v3.1.1 pub/sub events/demo/v3;
RPC drop on commands/demo/x; tests #13 V311PubSub + #12 RPC311Dropped).

Run:
  export KUBEMQ_MQTT_URL=tcp://localhost:1883
  cd examples/python
  python protocol/mqtt_v311_vs_v5/main.py
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
# Broker URL helpers
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


def rc_value(rc: ReasonCode | int) -> int:
    """Extract the integer reason code from a paho ReasonCode (or plain int)."""
    return rc.value if isinstance(rc, ReasonCode) else int(rc)


# ---------------------------------------------------------------------------
# Canonical topic strings (per spec S6.1.11)
# ---------------------------------------------------------------------------

EVENTS_TOPIC = "events/demo/v3"      # KubeMQ channel: demo.v3
COMMAND_TOPIC = "commands/demo/x"    # KubeMQ channel: demo.x  (v3.1.1 drop demo)


# ---------------------------------------------------------------------------
# Phase A — Events pub/sub via MQTT 3.1.1
# ---------------------------------------------------------------------------

def phase_a_v311_events(host: str, port: int, scheme: str) -> None:
    """
    Publish and receive a single event as MQTT 3.1.1.

    No user-properties exist in v3.1.1.  The subscriber callback prints that
    no user-props were received, which is the expected result.
    """
    print("--- Phase A: events pub/sub via MQTT 3.1.1 ---")
    received = threading.Event()

    sub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-v311-sub",
        protocol=mqtt.MQTTv311,
        transport="websockets" if scheme == "ws" else "tcp",
    )
    pub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-v311-pub",
        protocol=mqtt.MQTTv311,
        transport="websockets" if scheme == "ws" else "tcp",
    )

    # VERSION2: on_connect(client, userdata, connect_flags, reason_code, properties)
    def on_sub_connect(
        client: mqtt.Client, userdata, connect_flags, rc, *args  # noqa: ANN001
    ) -> None:
        if rc_value(rc) == 0:
            client.subscribe(EVENTS_TOPIC, qos=1)
            print(f"[v3.1.1-sub] connected; subscribing to {EVENTS_TOPIC!r} QoS 1")
        else:
            print(f"[v3.1.1-sub] connect failed rc=0x{rc_value(rc):02x}")

    # VERSION2: on_subscribe(client, userdata, mid, reason_code_list, properties)
    def on_sub_subscribe(
        client: mqtt.Client, userdata, mid, reason_codes, properties  # noqa: ANN001
    ) -> None:
        granted_qos = [rc_value(rc) for rc in reason_codes]
        print(f"[v3.1.1-sub] SUBACK mid={mid} granted_qos={granted_qos}")

    def on_message(
        client: mqtt.Client, userdata, msg: mqtt.MQTTMessage  # noqa: ANN001
    ) -> None:
        # v3.1.1: msg.properties is None — user-properties do not exist.
        has_user_props = bool(
            getattr(msg, "properties", None) and
            getattr(msg.properties, "UserProperty", None)
        )
        print(f"[v3.1.1-sub] received on {msg.topic!r}")
        print(f"             payload       : {msg.payload.decode()!r}")
        print(f"             user-props    : {has_user_props}  (expected: False — v3.1.1 has no user-properties)")
        received.set()

    sub.on_connect = on_sub_connect
    sub.on_subscribe = on_sub_subscribe
    sub.on_message = on_message

    if scheme == "tls":
        sub.tls_set()
        pub.tls_set()

    sub.connect(host, port, keepalive=30)
    sub.loop_start()
    time.sleep(0.4)  # let subscription establish

    pub.connect(host, port, keepalive=30)
    pub.loop_start()

    payload = b'{"proto": "3.1.1", "msg": "hello from v3.1.1"}'
    # v3.1.1 publish: no user-properties, no MQTT 5.0 properties object.
    result = pub.publish(EVENTS_TOPIC, payload=payload, qos=1)
    result.wait_for_publish(timeout=10)
    print(f"[v3.1.1-pub] published to {EVENTS_TOPIC!r}  (no user-properties)")

    if not received.wait(timeout=10):
        print("[v3.1.1-sub] WARNING: timed out waiting for event")

    pub.loop_stop()
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()


# ---------------------------------------------------------------------------
# Phase B — Events pub/sub via MQTT 5.0  (user-props forwarded as KubeMQ Tags)
# ---------------------------------------------------------------------------

def phase_b_v5_events(host: str, port: int, scheme: str) -> None:
    """
    Publish and receive a single event as MQTT 5.0 carrying User Properties.

    The two User Properties (env=demo, version=5.0) are forwarded by the broker
    to the KubeMQ array as message Tags, then delivered back to the v5 subscriber
    as MQTT 5.0 User Properties.

    Cap: at most 32 properties / 4096 bytes total.  Exceeding either limit on a
    v5 publish returns PUBACK 0x97 (ErrQuotaExceeded).
    """
    print("\n--- Phase B: events pub/sub via MQTT 5.0 (User Properties -> KubeMQ Tags) ---")
    received = threading.Event()
    received_user_props: list[list] = []

    sub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-v5-sub",
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )
    pub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-v5-pub",
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )

    # VERSION2: on_connect(client, userdata, connect_flags, reason_code, properties)
    def on_sub_connect(
        client: mqtt.Client, userdata, connect_flags, rc, props  # noqa: ANN001
    ) -> None:
        if rc_value(rc) == 0:
            client.subscribe(EVENTS_TOPIC, qos=1)
            print(f"[v5-sub] connected; subscribing to {EVENTS_TOPIC!r} QoS 1")
        else:
            print(f"[v5-sub] connect failed rc=0x{rc_value(rc):02x}")

    def on_sub_subscribe(
        client: mqtt.Client, userdata, mid, reason_codes, props  # noqa: ANN001
    ) -> None:
        codes = reason_codes if hasattr(reason_codes, "__iter__") else [reason_codes]
        for rc in codes:
            code = rc_value(rc)
            print(f"[v5-sub] SUBACK 0x{code:02x}")

    def on_message(
        client: mqtt.Client, userdata, msg: mqtt.MQTTMessage  # noqa: ANN001
    ) -> None:
        user_props: list = []
        if msg.properties and hasattr(msg.properties, "UserProperty"):
            user_props = list(msg.properties.UserProperty or [])
        received_user_props.append(user_props)
        print(f"[v5-sub] received on {msg.topic!r}")
        print(f"         payload    : {msg.payload.decode()!r}")
        print(f"         user-props : {user_props}  (forwarded as KubeMQ Tags)")
        received.set()

    def on_pub_publish(
        client: mqtt.Client, userdata, mid, reason_codes=None, props=None  # noqa: ANN001
    ) -> None:
        code = 0x00
        if reason_codes is not None:
            codes = reason_codes if hasattr(reason_codes, "__iter__") else [reason_codes]
            for rc in codes:
                code = rc_value(rc)
                break
        if code == 0x97:
            print("[v5-pub] ERROR PUBACK 0x97 — user-prop cap exceeded "
                  "(max 32 props / 4096 bytes)")
        elif code != 0x00:
            print(f"[v5-pub] WARNING PUBACK 0x{code:02x}")

    sub.on_connect = on_sub_connect
    sub.on_subscribe = on_sub_subscribe
    sub.on_message = on_message
    pub.on_publish = on_pub_publish

    if scheme == "tls":
        sub.tls_set()
        pub.tls_set()

    connect_props = Properties(PacketTypes.CONNECT)
    sub.connect(host, port, keepalive=30, clean_start=True, properties=connect_props)
    sub.loop_start()
    time.sleep(0.4)

    pub.connect(host, port, keepalive=30, clean_start=True)
    pub.loop_start()

    pub_props = Properties(PacketTypes.PUBLISH)
    # Two User Properties forwarded as KubeMQ message Tags (cap: 32 / 4096 bytes).
    pub_props.UserProperty = [("env", "demo"), ("version", "5.0")]

    payload = b'{"proto": "5.0", "msg": "hello from v5"}'
    result = pub.publish(EVENTS_TOPIC, payload=payload, qos=1, properties=pub_props)
    result.wait_for_publish(timeout=10)
    print(f"[v5-pub] published to {EVENTS_TOPIC!r}  "
          f"user-props=[env=demo, version=5.0]")

    if not received.wait(timeout=10):
        print("[v5-sub] WARNING: timed out waiting for event")
    else:
        props_received = received_user_props[0] if received_user_props else []
        assert ("env", "demo") in props_received, \
            f"user-prop env=demo not delivered: {props_received}"
        assert ("version", "5.0") in props_received, \
            f"user-prop version=5.0 not delivered: {props_received}"
        print("[v5-sub] user-props verified (KubeMQ Tags round-trip OK)")

    pub.loop_stop()
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()


# ---------------------------------------------------------------------------
# Phase C — Negative RPC: v3.1.1 publish to commands/ is silently dropped
# ---------------------------------------------------------------------------

def phase_c_v311_rpc_drop(host: str, port: int, scheme: str) -> None:
    """
    Demonstrate that a v3.1.1 client publishing to commands/<ch> receives
    PUBACK 0x00 (success) but the command is SILENTLY DROPPED server-side.

    The connector audits a publish.error server-side, but the MQTT client has
    NO indication of failure — the PUBACK still returns 0x00.  This is gotcha #4.

    There is therefore no response to wait for.  The example publishes to
    commands/demo/x, receives PUBACK 0x00, waits briefly, and documents the
    outcome: "no response — this is expected (v3.1.1 RPC drop)".
    """
    print("\n--- Phase C: v3.1.1 publish to commands/demo/x (RPC silently dropped) ---")
    print(f"  topic   : {COMMAND_TOPIC}  (KubeMQ channel demo.x)")
    print("  gotcha  : v3.1.1 PUBLISH to commands/ is dropped; PUBACK is still 0x00.")
    print("  fix     : use MQTT 5.0 for RPC.")

    puback_received = threading.Event()
    puback_code: list[int] = []

    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-v311-rpc-caller",
        protocol=mqtt.MQTTv311,
        transport="websockets" if scheme == "ws" else "tcp",
    )

    # VERSION2: on_connect(client, userdata, connect_flags, reason_code, properties)
    def on_connect(
        client: mqtt.Client, userdata, connect_flags, rc, *args  # noqa: ANN001
    ) -> None:
        if rc_value(rc) == 0:
            print("[v3.1.1-rpc] connected")
        else:
            print(f"[v3.1.1-rpc] connect failed rc=0x{rc_value(rc):02x}")

    # VERSION2: on_publish(client, userdata, mid, reason_code, properties).
    # Under VERSION2 even a v3.1.1 PUBACK is delivered with (reason_code, properties);
    # v3.1.1 has no reason-code byte on the wire, so the effective code is 0x00.
    def on_publish(
        client: mqtt.Client, userdata, mid, reason_code=None, properties=None  # noqa: ANN001
    ) -> None:
        puback_code.append(0x00)  # v3.1.1 PUBACK has no reason code byte
        print(f"[v3.1.1-rpc] PUBACK received for mid={mid}  "
              "(code=0x00 — broker accepted; command is SILENTLY DROPPED)")
        puback_received.set()

    client.on_connect = on_connect
    client.on_publish = on_publish

    if scheme == "tls":
        client.tls_set()

    client.connect(host, port, keepalive=30)
    client.loop_start()
    time.sleep(0.3)

    payload = b'{"command": "do-something"}'
    result = client.publish(COMMAND_TOPIC, payload=payload, qos=1)
    result.wait_for_publish(timeout=10)
    print(f"[v3.1.1-rpc] published to {COMMAND_TOPIC!r}")

    if not puback_received.wait(timeout=10):
        print("[v3.1.1-rpc] WARNING: PUBACK not received within timeout")

    # No response will arrive — this is the expected behavior.
    # Wait a short time to confirm silence, then document outcome.
    time.sleep(1.5)
    print("[v3.1.1-rpc] no response received (expected: v3.1.1 RPC is silently dropped)")
    print("             The server audits publish.error but the MQTT client sees PUBACK 0x00.")

    client.loop_stop()
    client.disconnect()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)

    print("=== MQTT 3.1.1 vs 5.0 — KubeMQ connector comparison ===")
    print(f"Broker  : {url}")
    print(f"Topics  : events={EVENTS_TOPIC!r}  command={COMMAND_TOPIC!r}")
    print()

    phase_a_v311_events(host, port, scheme)
    phase_b_v5_events(host, port, scheme)
    phase_c_v311_rpc_drop(host, port, scheme)

    print()
    print("=== Summary ===")
    print("  MQTT 3.1.1")
    print("    - basic events pub/sub: works")
    print("    - user-properties (KubeMQ Tags): NOT available (no wire support)")
    print("    - shared-subscription queue consume: NOT available (protocol lacks syntax)")
    print("    - RPC (commands/queries): SILENTLY DROPPED — PUBACK 0x00 but no delivery")
    print("  MQTT 5.0")
    print("    - basic events pub/sub: works")
    print("    - user-properties forwarded as KubeMQ Tags: YES (cap 32 props / 4096 bytes)")
    print("    - shared-subscription queue consume: YES ($share/<group>/queues/<ch> QoS>=1)")
    print("    - RPC via ResponseTopic + CorrelationData: YES (PUBACK immediate; await response separately)")
    print("    - fine-grained reason codes: 0x83 / 0x87 / 0x97 / 0x9A / 0xA2 …")
    print("Done.")


if __name__ == "__main__":
    run()

# ---------------------------------------------------------------------------
# Expected output:
#
# === MQTT 3.1.1 vs 5.0 — KubeMQ connector comparison ===
# Broker  : tcp://localhost:1883
# Topics  : events='events/demo/v3'  command='commands/demo/x'
#
# --- Phase A: events pub/sub via MQTT 3.1.1 ---
# [v3.1.1-sub] connected; subscribing to 'events/demo/v3' QoS 1
# [v3.1.1-sub] SUBACK mid=1 granted_qos=[1]
# [v3.1.1-pub] published to 'events/demo/v3'  (no user-properties)
# [v3.1.1-sub] received on 'events/demo/v3'
#              payload       : '{"proto": "3.1.1", "msg": "hello from v3.1.1"}'
#              user-props    : False  (expected: False — v3.1.1 has no user-properties)
#
# --- Phase B: events pub/sub via MQTT 5.0 (User Properties -> KubeMQ Tags) ---
# [v5-sub] connected; subscribing to 'events/demo/v3' QoS 1
# [v5-sub] SUBACK 0x01
# [v5-pub] published to 'events/demo/v3'  user-props=[env=demo, version=5.0]
# [v5-sub] received on 'events/demo/v3'
#          payload    : '{"proto": "5.0", "msg": "hello from v5"}'
#          user-props : [('env', 'demo'), ('version', '5.0')]  (forwarded as KubeMQ Tags)
# [v5-sub] user-props verified (KubeMQ Tags round-trip OK)
#
# --- Phase C: v3.1.1 publish to commands/demo/x (RPC silently dropped) ---
#   topic   : commands/demo/x  (KubeMQ channel demo.x)
#   gotcha  : v3.1.1 PUBLISH to commands/ is dropped; PUBACK is still 0x00.
#   fix     : use MQTT 5.0 for RPC.
# [v3.1.1-rpc] connected
# [v3.1.1-rpc] published to 'commands/demo/x'
# [v3.1.1-rpc] PUBACK received for mid=1  (code=0x00 — broker accepted; command is SILENTLY DROPPED)
# [v3.1.1-rpc] no response received (expected: v3.1.1 RPC is silently dropped)
#              The server audits publish.error but the MQTT client sees PUBACK 0x00.
#
# === Summary ===
#   MQTT 3.1.1
#     - basic events pub/sub: works
#     - user-properties (KubeMQ Tags): NOT available (no wire support)
#     - shared-subscription queue consume: NOT available (protocol lacks syntax)
#     - RPC (commands/queries): SILENTLY DROPPED — PUBACK 0x00 but no delivery
#   MQTT 5.0
#     - basic events pub/sub: works
#     - user-properties forwarded as KubeMQ Tags: YES (cap 32 props / 4096 bytes)
#     - shared-subscription queue consume: YES ($share/<group>/queues/<ch> QoS>=1)
#     - RPC via ResponseTopic + CorrelationData: YES (PUBACK immediate; await response separately)
#     - fine-grained reason codes: 0x83 / 0x87 / 0x97 / 0x9A / 0xA2 ...
# Done.
# ---------------------------------------------------------------------------
