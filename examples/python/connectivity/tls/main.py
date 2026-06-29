"""
Example: connectivity/tls

Demonstrates connecting to the KubeMQ MQTT connector over TLS (port 8883)
and performing a basic events pub/sub round-trip.

LIVE-TEST NOTE:
  The TLS listener on the live broker (127.0.0.1:8883) is CLOSED on this
  deployment.  This example compiles and is logically correct, but the live
  connection will fail until the broker is started with TLS enabled
  (CONNECTORSMQTT_TLS_PORT=8883 + a Security config that provides the server
  certificate and key).

  With InsecureSkipVerify=True the client skips server certificate validation,
  which is acceptable for development and self-signed certificates.
  IN PRODUCTION, always supply the proper CA bundle via KUBEMQ_MQTT_TLS_CA
  so the server certificate is fully verified and MITM attacks are prevented.

Topic: events/demo/tls  (maps to KubeMQ channel "demo.tls" via the Events bridge)
QoS  : 1

Environment variables:
  KUBEMQ_MQTT_URL        URL of the MQTT broker (default: tls://localhost:8883)
                         Scheme must be tls:// for this example.
  KUBEMQ_MQTT_TLS_CA     Optional path to a PEM CA certificate file.
                         When set, the server certificate is verified against it.
                         When not set, InsecureSkipVerify=True is used and the
                         system CA store is bypassed (development only).

Run:
  cd examples/python
  export KUBEMQ_MQTT_URL=tls://localhost:8883
  # Optional: provide CA cert for proper server verification
  # export KUBEMQ_MQTT_TLS_CA=/path/to/ca.crt
  python connectivity/tls/main.py
"""

from __future__ import annotations

import os
import ssl
import threading
import time

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.reasoncodes import ReasonCode

TOPIC = "events/demo/tls"


def broker_url() -> str:
    return os.environ.get("KUBEMQ_MQTT_URL", "tls://localhost:8883")


def parse_url(url: str) -> tuple[str, str, int]:
    """Parse 'scheme://host:port' into (scheme, host, port)."""
    scheme, rest = url.split("://", 1)
    host_port = rest.rstrip("/")
    if ":" in host_port:
        host, port_str = host_port.rsplit(":", 1)
        port = int(port_str)
    else:
        host = host_port
        port = {"tcp": 1883, "tls": 8883, "ws": 8083}.get(scheme, 8883)
    return scheme, host, port


def rc_value(rc: ReasonCode | int) -> int:
    """Extract the integer reason code from a paho ReasonCode (or plain int)."""
    return rc.value if isinstance(rc, ReasonCode) else int(rc)


def build_tls_context(ca_cert: str | None) -> ssl.SSLContext:
    """Build an SSLContext for the MQTT connection.

    If a CA certificate path is provided, the server certificate is verified
    against it (recommended for production).  Otherwise InsecureSkipVerify is
    used — acceptable for development / self-signed certs, but MUST NOT be
    used in production environments.
    """
    if ca_cert:
        ctx = ssl.create_default_context(cafile=ca_cert)
        print(f"[tls] Using CA certificate: {ca_cert}")
    else:
        ctx = ssl.create_default_context()
        # InsecureSkipVerify: skip server certificate validation.
        # PRODUCTION: replace with a proper CA bundle (set KUBEMQ_MQTT_TLS_CA).
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        print(
            "[tls] InsecureSkipVerify=True — server certificate not validated.\n"
            "  NOTE: In production, set KUBEMQ_MQTT_TLS_CA=/path/to/ca.crt\n"
            "        to enable full server certificate verification."
        )
    return ctx


def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)

    if scheme not in ("tls", "ssl"):
        print(f"[tls] WARNING: expected a tls:// URL, got {url!r}. Proceeding anyway.")

    ca_cert = os.environ.get("KUBEMQ_MQTT_TLS_CA")
    received = threading.Event()

    # --- Step 1: Create MQTT 5.0 client (TCP transport; TLS applied via tls_set) ---
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="python-tls-client",
        protocol=mqtt.MQTTv5,
        transport="tcp",
    )

    # --- Step 2: Configure TLS ---
    tls_ctx = build_tls_context(ca_cert)
    client.tls_set_context(tls_ctx)

    # --- Step 3: Register callbacks ---
    # VERSION2: on_connect(client, userdata, connect_flags, reason_code, properties)
    def on_connect(c: mqtt.Client, userdata, connect_flags, reason_code, props):  # noqa: ANN001
        rc = rc_value(reason_code)
        if rc == 0:
            print(f"[tls] Connected over TLS to {host}:{port}")
            # Step 4 (inside callback): subscribe to the events topic at QoS 1
            c.subscribe(TOPIC, qos=1)
            print(f"[tls] Subscribed to {TOPIC} (QoS 1)")
        elif rc == 0x86:
            print("[tls] CONNACK 0x86: bad username or password")
        elif rc == 0x87:
            print("[tls] CONNACK 0x87: not authorized (ACL)")
        elif rc == 0x8B:
            print("[tls] DISCONNECT 0x8B: server shutting down")
        else:
            print(f"[tls] Connection failed: reason code 0x{rc:02x}")

    # VERSION2: on_subscribe(client, userdata, mid, reason_code_list, properties)
    def on_subscribe(c: mqtt.Client, userdata, mid, reason_codes, props):  # noqa: ANN001
        granted = [rc_value(rc) for rc in reason_codes]
        print(f"[tls] SUBACK received — granted QoS: {granted}")

    def on_message(c: mqtt.Client, userdata, msg: mqtt.MQTTMessage):  # noqa: ANN001
        print(f"[tls] Message received on {msg.topic}: {msg.payload.decode()}")
        received.set()

    # VERSION2: on_disconnect(client, userdata, disconnect_flags, reason_code, properties)
    def on_disconnect(c: mqtt.Client, userdata, disconnect_flags, reason_code, properties=None):  # noqa: ANN001
        rc = rc_value(reason_code)
        if rc == 0x8B:
            print("[tls] Disconnected: server shutting down (0x8B)")
        elif rc != 0:
            print(f"[tls] Unexpected disconnect: reason code 0x{rc:02x}")

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # --- Step 5: Connect to the TLS listener ---
    print(f"[tls] Connecting to {host}:{port} over TLS (MQTT 5.0) ...")
    try:
        client.connect(host, port, keepalive=30)
    except OSError as exc:
        print(
            f"[tls] Could not connect to {host}:{port}: {exc}\n"
            "\n"
            "  NOTE: The TLS listener is disabled on the default KubeMQ deployment.\n"
            "  To enable it, start kubemq-server with:\n"
            "    CONNECTORSMQTT_TLS_PORT=8883\n"
            "  plus a Security configuration providing the server certificate and key.\n"
        )
        return

    client.loop_start()

    # Allow on_connect + subscribe to complete before publishing.
    time.sleep(0.5)

    # --- Step 6: Publish an event over TLS ---
    payload = b'{"transport": "tls", "channel": "demo.tls"}'
    result = client.publish(TOPIC, payload=payload, qos=1)
    try:
        result.wait_for_publish(timeout=10)
        print(f"[tls] Published to {TOPIC} (QoS 1)")
    except Exception as exc:  # noqa: BLE001
        print(f"[tls] Publish failed: {exc}")

    # --- Step 7: Wait for the loopback message ---
    if not received.wait(timeout=10):
        print("[tls] Timed out waiting for loopback message.")
    else:
        print("[tls] Round-trip complete.")

    # --- Step 8: Clean disconnect ---
    client.loop_stop()
    client.disconnect()
    print("Done.")


if __name__ == "__main__":
    run()

# ---------------------------------------------------------------------------
# Expected output (TLS-enabled broker, InsecureSkipVerify):
#
# [tls] InsecureSkipVerify=True — server certificate not validated.
#   NOTE: In production, set KUBEMQ_MQTT_TLS_CA=/path/to/ca.crt
#         to enable full server certificate verification.
# [tls] Connecting to localhost:8883 over TLS (MQTT 5.0) ...
# [tls] Connected over TLS to localhost:8883
# [tls] Subscribed to events/demo/tls (QoS 1)
# [tls] SUBACK received — granted QoS: [1]
# [tls] Published to events/demo/tls (QoS 1)
# [tls] Message received on events/demo/tls: {"transport": "tls", "channel": "demo.tls"}
# [tls] Round-trip complete.
# Done.
#
# ---------------------------------------------------------------------------
# Expected output (TLS listener disabled — live broker default):
#
# [tls] InsecureSkipVerify=True — server certificate not validated.
#   NOTE: In production, set KUBEMQ_MQTT_TLS_CA=/path/to/ca.crt
#         to enable full server certificate verification.
# [tls] Connecting to localhost:8883 over TLS (MQTT 5.0) ...
# [tls] Could not connect to localhost:8883: [Errno 61] Connection refused
#
#   NOTE: The TLS listener is disabled on the default KubeMQ deployment.
#   To enable it, start kubemq-server with:
#     CONNECTORSMQTT_TLS_PORT=8883
#   plus a Security configuration providing the server certificate and key.
