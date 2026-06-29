"""
Example: connectivity/auth_jwt

Demonstrates password-as-JWT authentication for the KubeMQ MQTT connector.

Auth mechanism
--------------
- CONNECT Password field = KubeMQ JWT token (PasswordFlag = true in the CONNECT
  packet).  The connector validates the token via the KubeMQ authentication
  service (hooks.go:authenticateConnect).
- Username is DISPLAY-ONLY — NOT used for auth.
- Empty or invalid password when auth is enabled → CONNACK 0x86 (bad auth).
- Auth is checked ONLY at connect time; there is no mid-connection recheck.
- Auth NOT configured on the broker → open access (any password, even empty,
  is accepted — this example still connects and demonstrates the pattern).

LIVE-TEST NOTE
--------------
Auth appears DISABLED on the live broker (tcp://127.0.0.1:1883).
Connecting with a JWT in the Password field succeeds, but JWT enforcement
is NOT active.  Full JWT rejection (CONNACK 0x86) requires an auth-enabled
broker.  Mark this example partial / N/A for live JWT rejection tests; do NOT
treat the absence of rejection as a failure.

Security note
-------------
The JWT travels in the CONNECT Password field in cleartext at the MQTT layer.
In production, ALWAYS combine this pattern with the TLS listener (port 8883)
or WebSocket-over-TLS so the token is encrypted in transit.
See: connectivity/tls/

Environment
-----------
  KUBEMQ_MQTT_URL   tcp://localhost:1883  (default)
  KUBEMQ_MQTT_TOKEN <your-jwt-token>      (required for auth-enabled brokers;
                                           harmless default empty string used
                                           when auth is disabled)

Run
---
  export KUBEMQ_MQTT_URL=tcp://localhost:1883
  export KUBEMQ_MQTT_TOKEN=<your-jwt-token>
  python connectivity/auth_jwt/main.py
"""

from __future__ import annotations

import os
import threading
import time

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


def rc_value(rc: ReasonCode | int) -> int:
    """Extract the integer reason code from a paho ReasonCode (or plain int)."""
    return rc.value if isinstance(rc, ReasonCode) else int(rc)


# ---------------------------------------------------------------------------
# Canonical topic / channel (per spec S6.1.10 / integration test #16)
# ---------------------------------------------------------------------------

TOPIC = "events/demo/auth"     # MQTT topic
CHANNEL = "demo.auth"          # KubeMQ channel (/ -> . translation)


# ---------------------------------------------------------------------------
# Example
# ---------------------------------------------------------------------------

def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)

    jwt_token: str = os.environ.get("KUBEMQ_MQTT_TOKEN", "")

    print(f"Broker : {url}  (scheme={scheme}, host={host}, port={port})")
    print(f"MQTT topic  : {TOPIC}")
    print(f"KubeMQ ch   : {CHANNEL}  (/ -> . translation)")

    if jwt_token:
        # Show only the first 20 chars so secrets are not fully exposed in logs.
        preview = jwt_token[:20] + ("..." if len(jwt_token) > 20 else "")
        print(f"JWT token   : {preview}  (Password field, PasswordFlag=true)")
    else:
        print(
            "[auth] KUBEMQ_MQTT_TOKEN is not set — connecting without credentials.\n"
            "       On an auth-enabled broker this will fail with CONNACK 0x86.\n"
            "       On this broker (auth disabled) the connection will succeed."
        )

    received = threading.Event()
    connect_failed = threading.Event()

    # Step 1 — Build the MQTT 5.0 client (CallbackAPIVersion.VERSION2 avoids
    # the paho 2.x deprecation warning for the callback signature).
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-auth-client",
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )

    # Step 2 — Set credentials.
    #   Username is display-only (informational) — the KubeMQ connector does not
    #   use it for authentication.  Password carries the JWT token.
    client.username_pw_set(
        username="kubemq-python-example",
        password=jwt_token if jwt_token else None,
    )

    if scheme == "tls":
        client.tls_set()

    # Step 3 — on_connect: handle CONNACK reason codes.
    # VERSION2 signature: (client, userdata, connect_flags, reason_code, properties)
    def on_connect(
        c: mqtt.Client,
        userdata: object,
        connect_flags: mqtt.ConnectFlags,
        reason_code: ReasonCode,
        properties: mqtt.Properties,
    ) -> None:
        rc = rc_value(reason_code)
        if rc == 0:
            print(f"[auth] CONNACK 0x00 — connected to {host}:{port}")
            # Step 4 — subscribe to events/demo/auth at QoS 1.
            c.subscribe(TOPIC, qos=1)
        elif rc == 0x86:
            print(
                "[auth] CONNACK 0x86 — bad username or password "
                "(JWT rejected by auth-enabled broker)"
            )
            connect_failed.set()
        elif rc == 0x80:
            print("[auth] CONNACK 0x80 — broker not ready; retry shortly")
            connect_failed.set()
        else:
            print(f"[auth] CONNACK 0x{rc:02x} — connection refused")
            connect_failed.set()

    # VERSION2 signature: (client, userdata, mid, reason_codes, properties)
    def on_subscribe(
        c: mqtt.Client,
        userdata: object,
        mid: int,
        reason_codes: list[ReasonCode],
        properties: mqtt.Properties,
    ) -> None:
        code = rc_value(reason_codes[0]) if reason_codes else None
        print(f"[auth] SUBACK mid={mid} reason_code={code}")

    # VERSION2 signature: (client, userdata, message)
    def on_message(
        c: mqtt.Client,
        userdata: object,
        msg: mqtt.MQTTMessage,
    ) -> None:
        print(f"[auth] Received on {msg.topic}: {msg.payload.decode()!r}")
        received.set()

    # VERSION2 on_publish: (client, userdata, mid, reason_code, properties)
    def on_publish(
        c: mqtt.Client,
        userdata: object,
        mid: int,
        reason_code: ReasonCode | None = None,
        properties: mqtt.Properties | None = None,
    ) -> None:
        rc = rc_value(reason_code) if reason_code is not None else 0
        print(f"[auth] PUBACK received  mid={mid}  reason_code=0x{rc:02x}")

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_publish = on_publish

    # Step 5 — connect and start the network loop.
    try:
        client.connect(host, port, keepalive=30)
    except Exception as exc:
        print(f"[auth] Could not connect to {host}:{port}: {exc}")
        return

    client.loop_start()

    # Wait long enough for CONNACK to arrive before deciding whether to proceed.
    time.sleep(0.5)

    if connect_failed.is_set():
        client.loop_stop()
        client.disconnect()
        print("Done (connection refused — no publish attempted).")
        return

    # Step 6 — publish one event to events/demo/auth at QoS 1.
    payload = b'{"auth": "jwt-demo"}'
    result = client.publish(TOPIC, payload=payload, qos=1)
    result.wait_for_publish(timeout=10)
    print(f"[auth] Published  topic={TOPIC}  payload={payload.decode()!r}  mid={result.mid}")

    # Step 7 — wait for the loopback event (subscriber on the same connection).
    if not received.wait(timeout=10):
        print(
            "[auth] Timed out waiting for loopback message.\n"
            "       This can happen when auth rejected the connection silently,\n"
            "       or when the broker is not ready."
        )
    else:
        print("[ok] Auth connect + pub/sub round-trip verified.")

    client.loop_stop()
    client.disconnect()
    print("Done.")


if __name__ == "__main__":
    run()

# ---------------------------------------------------------------------------
# Expected output — auth-enabled broker, valid JWT
# ---------------------------------------------------------------------------
# Broker : tcp://localhost:1883  (scheme=tcp, host=localhost, port=1883)
# MQTT topic  : events/demo/auth
# KubeMQ ch   : demo.auth  (/ -> . translation)
# JWT token   : eyJhbGciOiJSUzI1...  (Password field, PasswordFlag=true)
# [auth] CONNACK 0x00 — connected to localhost:1883
# [auth] SUBACK mid=1 reason_code=1
# [auth] Published  topic=events/demo/auth  payload='{"auth": "jwt-demo"}'  mid=1
# [auth] PUBACK received  mid=1  reason_code=0x00
# [auth] Received on events/demo/auth: '{"auth": "jwt-demo"}'
# [ok] Auth connect + pub/sub round-trip verified.
# Done.
#
# ---------------------------------------------------------------------------
# Expected output — auth-enabled broker, empty / invalid JWT
# ---------------------------------------------------------------------------
# [auth] KUBEMQ_MQTT_TOKEN is not set — connecting without credentials.
#        On an auth-enabled broker this will fail with CONNACK 0x86.
#        On this broker (auth disabled) the connection will succeed.
# [auth] CONNACK 0x86 — bad username or password (JWT rejected by auth-enabled broker)
# Done (connection refused — no publish attempted).
#
# ---------------------------------------------------------------------------
# Expected output — auth disabled (live broker default)
# ---------------------------------------------------------------------------
# Broker : tcp://localhost:1883  (scheme=tcp, host=localhost, port=1883)
# MQTT topic  : events/demo/auth
# KubeMQ ch   : demo.auth  (/ -> . translation)
# [auth] KUBEMQ_MQTT_TOKEN is not set — connecting without credentials.
#        On an auth-enabled broker this will fail with CONNACK 0x86.
#        On this broker (auth disabled) the connection will succeed.
# [auth] CONNACK 0x00 — connected to localhost:1883
# [auth] SUBACK mid=1 reason_code=1
# [auth] Published  topic=events/demo/auth  payload='{"auth": "jwt-demo"}'  mid=1
# [auth] PUBACK received  mid=1  reason_code=0x00
# [auth] Received on events/demo/auth: '{"auth": "jwt-demo"}'
# [ok] Auth connect + pub/sub round-trip verified.
# Done.
