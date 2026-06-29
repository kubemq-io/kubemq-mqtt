"""
Example: rpc/command_round_trip

Demonstrates a KubeMQ Commands RPC round-trip via MQTT 5.0.

MQTT 5.0 ONLY — v3.1.1 publishes to ``commands/`` are silently dropped (gotcha #4).

Commands vs Queries:
  - Commands: NO response body; response user-props carry ``kubemq-executed``
    ("true"/"false") + optional ``kubemq-error``.
  - Queries:  response carries a body + user-props (kubemq-metadata + tags).

Flow:
  1. Subscribe to ``$reply/<clientID>/inbox`` (own namespace only, mochi-local).
  2. Publish to ``commands/demo/cmd`` with:
       Properties.ResponseTopic   = "$reply/<clientID>/inbox"
       Properties.CorrelationData = <unique bytes>
  3. PUBACK arrives IMMEDIATELY (NOT after the response — client MUST implement
     its own response-wait timeout).
  4. KubeMQ bridges the request to the gRPC-side command responder.
  5. Response arrives on the reply topic with ``kubemq-executed`` user-prop
     and NO body.

REQUIREMENT: a gRPC responder must be running on channel ``demo.cmd`` (type=command).
See kubemq-mqtt/.work/test-harness/responder/

Run:
  export KUBEMQ_MQTT_URL=tcp://localhost:1883
  # Start gRPC responder first:
  #   cd kubemq-mqtt/.work/test-harness/responder
  #   go run . -channel demo.cmd -type command
  # Then:
  python rpc/command_round_trip/main.py

Reason codes to handle:
  0x83 — $reply namespace not own clientID, or wrong ResponseTopic
  0x97 — RpcMaxPending quota exceeded
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


def rc_value(rc: ReasonCode | int) -> int:
    """Extract the integer reason code from a paho ReasonCode (or plain int)."""
    return rc.value if isinstance(rc, ReasonCode) else int(rc)


# MQTT 5.0 client ID — reply inbox MUST be own $reply/<clientID>/... namespace
CLIENT_ID = "python-command-client"
# Topic grammar: commands/<ch> -> KubeMQ channel demo.cmd (/ becomes .)
COMMAND_TOPIC = "commands/demo/cmd"
# Reply inbox: mochi-local, must be own clientID namespace (else PUBACK 0x83)
REPLY_TOPIC = f"$reply/{CLIENT_ID}/inbox"
# Unique correlation ID for this request
CORRELATION = uuid.uuid4().bytes


def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)
    print(f"[client] connecting to {url}")

    response_received = threading.Event()
    response_data: list[dict] = []

    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=CLIENT_ID,
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )

    # VERSION2: on_connect(client, userdata, connect_flags, reason_code, properties)
    def on_connect(c: mqtt.Client, userdata, connect_flags, rc, props):  # noqa: ANN001
        if rc_value(rc) == 0:
            print("[client] connected (MQTT 5.0)")
            # Step 1: subscribe to own reply inbox BEFORE publishing the command.
            # Must subscribe FIRST so the broker registers the route; any message
            # that arrives before subscribe completes would be lost.
            c.subscribe(REPLY_TOPIC, qos=1)
        else:
            print(f"[client] connect failed rc=0x{rc_value(rc):02x}")

    def on_subscribe(c: mqtt.Client, userdata, mid, reason_codes, props):  # noqa: ANN001
        for rc in (reason_codes if hasattr(reason_codes, "__iter__") else [reason_codes]):
            code = rc_value(rc)
            if code > 1:
                print(
                    f"[client] WARNING: subscribe reason code 0x{code:02x} — "
                    "foreign $reply namespace? MQTT clients cannot be RPC responders "
                    "(subscribing to commands/ returns SUBACK 0x83)."
                )
            else:
                print(f"[client] subscribed to reply topic {REPLY_TOPIC!r} (QoS {code})")

    def on_message(c: mqtt.Client, userdata, msg: mqtt.MQTTMessage):  # noqa: ANN001
        """Receive command response on $reply/<clientID>/inbox.

        Command response (per rpc_bridge.go:137-169):
          - NO body (empty payload)
          - user-props: kubemq-executed="true"/"false"
          - user-props: kubemq-error (only on failure)
          - CorrelationData echoed from the request
        """
        props_dict: dict = {}
        if msg.properties and hasattr(msg.properties, "UserProperty"):
            for k, v in (msg.properties.UserProperty or []):
                props_dict[k] = v
        response_data.append({
            "topic": msg.topic,
            "payload": msg.payload,
            "correlation": getattr(msg.properties, "CorrelationData", None),
            "user_props": props_dict,
        })
        response_received.set()

    def on_publish(c: mqtt.Client, userdata, mid, reason_codes=None, props=None):  # noqa: ANN001
        """PUBACK callback — fires IMMEDIATELY on broker receipt, NOT after execution."""
        code = None
        if reason_codes is not None:
            rcs = reason_codes if hasattr(reason_codes, "__iter__") else [reason_codes]
            for rc in rcs:
                code = rc_value(rc)
        if code is not None and code not in (0, None):
            print(f"[client] PUBACK reason code 0x{code:02x} — "
                  "0x83=wrong ResponseTopic/namespace; 0x97=RpcMaxPending exceeded")

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_publish = on_publish

    if scheme == "tls":
        client.tls_set()
    client.connect(host, port, keepalive=30)
    client.loop_start()

    # Wait for subscribe to establish before publishing.
    time.sleep(0.5)

    # Step 2: publish command with ResponseTopic + CorrelationData.
    pub_props = Properties(PacketTypes.PUBLISH)
    # ResponseTopic MUST be own $reply/<clientID>/... namespace (else PUBACK 0x83).
    pub_props.ResponseTopic = REPLY_TOPIC
    pub_props.CorrelationData = CORRELATION
    # Optional user-props become KubeMQ tags on the command message (v5 only, cap 32/4096B).
    pub_props.UserProperty = [("action", "do"), ("target", "demo-server")]

    # Command payload carries the action arguments (may be empty).
    payload = b'{"action": "do", "target": "demo-server"}'
    result = client.publish(
        COMMAND_TOPIC,
        payload=payload,
        qos=1,
        properties=pub_props,
    )
    result.wait_for_publish(timeout=10)
    # NOTE: PUBACK is sent IMMEDIATELY on broker receipt — it does NOT mean the
    # command has been executed. The client must implement its own response timeout.
    print(
        f"[client] command published to {COMMAND_TOPIC!r} "
        f"(PUBACK=immediate, correlation={CORRELATION.hex()[:8]}..., awaiting response...)"
    )

    # Step 3: wait for the command response on the reply inbox.
    # KubeMQ connector bridges the command to the gRPC responder; no response
    # arrives if no responder is registered (connector audits rpc.timeout after
    # RpcTimeoutSeconds, default 30 s).
    rpc_timeout = int(os.environ.get("KUBEMQ_RPC_TIMEOUT", "30"))
    if not response_received.wait(timeout=rpc_timeout + 2):
        print(
            f"[client] ERROR: no response within {rpc_timeout + 2}s.  "
            "Ensure a gRPC command responder is running on channel 'demo.cmd':\n"
            "  cd kubemq-mqtt/.work/test-harness/responder\n"
            "  go run . -channel demo.cmd -type command"
        )
    else:
        resp = response_data[0]
        # Verify CorrelationData echoed (rpc_bridge.go:164)
        corr_match = resp["correlation"] == CORRELATION
        executed = resp["user_props"].get("kubemq-executed", "unknown")
        error_msg = resp["user_props"].get("kubemq-error", "")

        print(f"[client] command response received on {resp['topic']!r}")
        print(f"  kubemq-executed:      {executed}")
        if error_msg:
            print(f"  kubemq-error:         {error_msg}")
        print(f"  payload (empty):      {resp['payload']!r}")
        print(f"  correlation echoed:   {corr_match} ({resp['correlation'].hex()[:8]}...)")
        print(f"  user_props:           {resp['user_props']}")

        if executed == "true" and not error_msg:
            print("[client] command executed successfully")
        elif executed == "false":
            print(f"[client] command execution FAILED: {error_msg or '(no error detail)'}")

    client.loop_stop()
    client.disconnect()
    print("[client] disconnected")


if __name__ == "__main__":
    run()

# Expected output (with gRPC responder running on channel demo.cmd):
# [client] connecting to tcp://localhost:1883
# [client] connected (MQTT 5.0)
# [client] subscribed to reply topic '$reply/python-command-client/inbox' (QoS 1)
# [client] command published to 'commands/demo/cmd' (PUBACK=immediate, correlation=..., awaiting response...)
# [client] command response received on '$reply/python-command-client/inbox'
#   kubemq-executed:      true
#   payload (empty):      b''
#   correlation echoed:   True (...)
#   user_props:           {'kubemq-executed': 'true', ...}
# [client] command executed successfully
# [client] disconnected
