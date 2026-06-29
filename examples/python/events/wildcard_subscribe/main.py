"""
Example: events/wildcard_subscribe

Demonstrates MQTT wildcard subscriptions forwarded to KubeMQ Events.  Mirrors
integration test #25 (TestMqttIntegration_QueueNoWildcardLeak) which asserts
that THREE overlapping event filters each create an independent bridge registry
entry, so ONE publish to events/leak/<id>/x delivers EXACTLY THREE copies.

Wildcard translation (Events pattern only):
  MQTT '+' -> KubeMQ '*'   (single-level wildcard)
  MQTT '#' -> KubeMQ '>'   (multi-level wildcard; must be final segment)

Wildcard subscriptions are ONLY permitted on the Events pattern.  Attempting a
wildcard subscribe on any other pattern (e.g. store/#, queues/#) returns
SUBACK 0xA2 (wildcard-subscriptions-not-supported).

Overlapping wildcard filters — gotcha #6:
  Each distinct subscribe filter creates an independent bridge registry entry.
  A publish that matches N entries is injected N times through the bridge; a
  subscriber whose N active filters all match receives N copies with NO
  cross-entry deduplication (MQTT 5.0 §3.3.5 permits one copy per matching
  subscription; the connector leverages this).

Per-run unique channel (CI determinism):
  Rather than the literal fixed "events/leak/x"/"events/#" (which over-count on
  a busy/shared broker), this example mints a per-run UNIQUE id and scopes its
  overlapping filters to "events/leak/<id>/...".  ONE subscriber registers THREE
  overlapping filters that all match the unique publish topic:
      events/leak/<id>/x   (exact)        -> KubeMQ leak.<id>.x
      events/leak/<id>/#   (multi-level)  -> KubeMQ leak.<id>.>   ('#' -> '>')
      events/leak/<id>/+   (single-level) -> KubeMQ leak.<id>.*   ('+' -> '*')
  A single publish to events/leak/<id>/x matches all three, so the subscriber
  receives one copy per registered overlapping bridge entry (exactly three on a
  quiet broker; ">= 3" deterministically — a foreign broad-wildcard subscriber
  on the global '>' channel may add extra copies, which IS gotcha #6, not an
  error).  A bare '#' is ALSO subscribed, but treated as INFORMATIONAL ONLY
  (it counts ambient traffic and so varies; it is never asserted on).

Run:
  export KUBEMQ_MQTT_URL=tcp://localhost:1883
  python events/wildcard_subscribe/main.py
"""

from __future__ import annotations

import os
import queue
import threading
import time
import uuid

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.reasoncodes import ReasonCode

# ---------------------------------------------------------------------------
# Broker helpers
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


def make_client(client_id: str, scheme: str) -> mqtt.Client:
    """Create an MQTTv5 client with the modern (VERSION2) callback API."""
    return mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
        transport="websockets" if scheme == "ws" else "tcp",
    )


def rc_value(rc: ReasonCode | int) -> int:
    """Extract the integer reason code from a paho ReasonCode (or plain int)."""
    return rc.value if isinstance(rc, ReasonCode) else int(rc)


# ---------------------------------------------------------------------------
# Example constants  — mirror test #25
# ---------------------------------------------------------------------------

# Expected copies: one per matching bridge registry entry (no cross-entry dedup).
WANT_COPIES = 3


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run() -> None:
    url = broker_url()
    scheme, host, port = parse_url(url)

    uid = uuid.uuid4().hex[:6]

    # Per-run unique channel so this example's three overlapping filters target a
    # freshly minted KubeMQ channel that no prior/other run reuses (CI
    # determinism on a busy/shared broker — see the Go reference).
    publish_topic = f"events/leak/{uid}/x"      # unique exact     -> leak.<id>.x
    unique_hash_filter = f"events/leak/{uid}/#"  # unique subtree   -> leak.<id>.>
    unique_plus_filter = f"events/leak/{uid}/+"  # unique 1-level   -> leak.<id>.*

    # Three overlapping filters that all match publish_topic; each registers a
    # separate bridge entry, so ONE publish routes through all three -> three
    # injections -> three copies (no cross-entry dedup).  The bare '#' is added
    # too but is INFORMATIONAL only (counts ambient traffic; never asserted on).
    asserted_filters = [publish_topic, unique_hash_filter, unique_plus_filter]
    filters = asserted_filters + ["#"]

    msg_queue: queue.Queue[mqtt.MQTTMessage] = queue.Queue()
    copy_counter = 0
    copy_lock = threading.Lock()

    # -----------------------------------------------------------------------
    # Step 1 — connect a single subscriber and register ALL the filters
    # -----------------------------------------------------------------------
    sub = make_client(f"py-wc-sub-{uid}", scheme)
    if scheme == "tls":
        sub.tls_set()

    subscribed_count = 0
    total_filters = len(filters)
    all_subs_done: queue.Queue[bool] = queue.Queue()

    # VERSION2: on_connect(client, userdata, connect_flags, reason_code, properties)
    def on_sub_connect(
        client: mqtt.Client,
        userdata: object,
        connect_flags: object,
        reason_code: ReasonCode,
        properties: object,
    ) -> None:
        if rc_value(reason_code) == 0:
            for f in filters:
                rc, mid = client.subscribe(f, qos=1)
                print(f"[subscriber] subscribe('{f}', qos=1)  mid={mid}  rc={rc}")

    # VERSION2: on_subscribe(client, userdata, mid, reason_codes, properties)
    def on_subscribe(
        client: mqtt.Client,
        userdata: object,
        mid: int,
        reason_codes: list[ReasonCode],
        properties: object,
    ) -> None:
        nonlocal subscribed_count
        for rc in reason_codes:
            code = rc_value(rc)
            if code in (0x00, 0x01, 0x02):  # Granted QoS 0/1/2
                subscribed_count += 1
                print(
                    f"[subscriber] SUBACK mid={mid}  code=0x{code:02X} (QoS granted)"
                    f"  ({subscribed_count}/{total_filters} registered)"
                )
                if subscribed_count >= total_filters:
                    all_subs_done.put(True)
            elif code == 0xA2:
                print(
                    "[subscriber] SUBACK 0xA2 — wildcard NOT allowed on this "
                    "pattern (non-events wildcard rejected)"
                )
                all_subs_done.put(False)
            else:
                print(f"[subscriber] SUBACK mid={mid}  unexpected code=0x{code:02X}")

    def on_message(
        client: mqtt.Client,
        userdata: object,
        msg: mqtt.MQTTMessage,
    ) -> None:
        nonlocal copy_counter
        # The bare '#' filter (KubeMQ channel '>') matches EVERYTHING on the
        # broker, including KubeMQ's internal cluster-events firehose (e.g.
        # events/kubemq/cluster/internal/events/snapshots).  Only count copies
        # of OUR per-run-unique test publish so the per-entry dedup assertion is
        # meaningful on a live/busy broker.
        if msg.topic != publish_topic:
            return
        msg_queue.put(msg)
        with copy_lock:
            copy_counter += 1
            n = copy_counter
        print(
            f"[subscriber] copy #{n} received on '{msg.topic}': "
            f"{msg.payload.decode()}"
        )

    sub.on_connect = on_sub_connect
    sub.on_subscribe = on_subscribe
    sub.on_message = on_message

    sub.connect(host, port, keepalive=30)
    sub.loop_start()

    # Wait until all three subscriptions are confirmed.
    try:
        ok = all_subs_done.get(timeout=10)
    except queue.Empty:
        raise TimeoutError("Timed out waiting for subscription acknowledgements")
    if not ok:
        raise RuntimeError("One or more subscriptions were rejected")

    print(
        f"[subscriber] all {total_filters} filters registered "
        f"({len(asserted_filters)} unique + bare '#' informational) — "
        "each creates an independent bridge registry entry"
    )

    # -----------------------------------------------------------------------
    # Step 2 — demonstrate the non-events wildcard rejection (0xA2 contrast)
    # -----------------------------------------------------------------------
    print()
    print(
        "[contrast] Attempting wildcard subscribe on 'store/#' (Events-Store) ..."
    )
    contrast = make_client(f"py-wc-contrast-{uid}", scheme)
    if scheme == "tls":
        contrast.tls_set()

    contrast_result: queue.Queue[int] = queue.Queue()

    def on_contrast_connect(
        client: mqtt.Client,
        userdata: object,
        connect_flags: object,
        reason_code: ReasonCode,
        properties: object,
    ) -> None:
        if rc_value(reason_code) == 0:
            client.subscribe("store/#", qos=1)

    def on_contrast_subscribe(
        client: mqtt.Client,
        userdata: object,
        mid: int,
        reason_codes: list[ReasonCode],
        properties: object,
    ) -> None:
        for rc in reason_codes:
            contrast_result.put(rc_value(rc))

    contrast.on_connect = on_contrast_connect
    contrast.on_subscribe = on_contrast_subscribe
    contrast.connect(host, port, keepalive=30)
    contrast.loop_start()

    try:
        rc = contrast_result.get(timeout=5)
        if rc == 0xA2:
            print(
                "[contrast] SUBACK 0xA2 — wildcard on non-events (Events-Store) "
                "correctly rejected (as expected)"
            )
        else:
            print(f"[contrast] SUBACK 0x{rc:02X} (unexpected; may be broker config)")
    except queue.Empty:
        print("[contrast] no SUBACK received within 5 s (skip)")

    contrast.loop_stop()
    contrast.disconnect()
    print()

    # -----------------------------------------------------------------------
    # Step 3 — publish ONE message and observe WANT_COPIES deliveries
    # -----------------------------------------------------------------------
    # Allow a brief settle so all bridge entries are fully registered.
    time.sleep(0.5)

    pub = make_client(f"py-wc-pub-{uid}", scheme)
    if scheme == "tls":
        pub.tls_set()
    pub.connect(host, port, keepalive=30)
    pub.loop_start()

    payload = b'{"event": "overlap-test"}'
    result = pub.publish(publish_topic, payload=payload, qos=1)
    result.wait_for_publish(timeout=10)
    print(
        f"[publisher] published ONE message to '{publish_topic}'  mid={result.mid}"
    )
    print(
        f"[publisher] expecting >= {WANT_COPIES} deliveries "
        "(one per matching unique bridge registry entry; the bare '#' may add more)"
    )

    # Collect exactly WANT_COPIES messages.
    copies: list[mqtt.MQTTMessage] = []
    deadline = time.monotonic() + 10.0
    while len(copies) < WANT_COPIES:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            copies.append(msg_queue.get(timeout=remaining))
        except queue.Empty:
            break

    if len(copies) < WANT_COPIES:
        raise TimeoutError(
            f"Expected {WANT_COPIES} copies, received only {len(copies)}"
        )

    # Brief window to surface any extra copies (e.g. ambient '>' entries from a
    # foreign broad-wildcard subscriber, or our own bare '#').
    time.sleep(0.7)
    total = len(copies) + msg_queue.qsize()

    print()
    print(
        f"[result] received {total} copies of '{publish_topic}' (>= {WANT_COPIES}) "
        "— one per matching bridge registry entry (no cross-entry dedup)"
    )
    if total > WANT_COPIES:
        print(
            f"[result] {total - WANT_COPIES} extra copy(ies) — the bare '#' (or a foreign "
            "broad-wildcard subscriber on the global '>' channel) added matching "
            "bridge entries; that is gotcha #6, not an error."
        )

    # -----------------------------------------------------------------------
    # Cleanup
    # -----------------------------------------------------------------------
    pub.loop_stop()
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()
    print()
    print("Done.")


if __name__ == "__main__":
    run()

# ---------------------------------------------------------------------------
# Expected output (placeholder — regenerated from a real run; <id> is the
# per-run unique id, e.g. "a1b2c3"; order of received lines may vary):
#
# [subscriber] subscribe('events/leak/<id>/x', qos=1)  mid=1  rc=0
# [subscriber] subscribe('events/leak/<id>/#', qos=1)  mid=2  rc=0
# [subscriber] subscribe('events/leak/<id>/+', qos=1)  mid=3  rc=0
# [subscriber] subscribe('#', qos=1)  mid=4  rc=0
# [subscriber] SUBACK mid=1  code=0x01 (QoS granted)  (1/4 registered)
# [subscriber] ... (4/4 registered)
# [subscriber] all 4 filters registered (3 unique + bare '#' informational) — each creates an independent bridge registry entry
#
# [contrast] Attempting wildcard subscribe on 'store/#' (Events-Store) ...
# [contrast] SUBACK 0xA2 — wildcard on non-events (Events-Store) correctly rejected (as expected)
#
# [publisher] published ONE message to 'events/leak/<id>/x'  mid=1
# [publisher] expecting >= 3 deliveries (one per matching unique bridge registry entry; the bare '#' may add more)
# [subscriber] copy #1 received on 'events/leak/<id>/x': {"event": "overlap-test"}
# [subscriber] copy #2 received on 'events/leak/<id>/x': {"event": "overlap-test"}
# [subscriber] copy #3 received on 'events/leak/<id>/x': {"event": "overlap-test"}
#
# [result] received N copies of 'events/leak/<id>/x' (>= 3) — one per matching bridge registry entry (no cross-entry dedup)
#
# Done.
# ---------------------------------------------------------------------------
