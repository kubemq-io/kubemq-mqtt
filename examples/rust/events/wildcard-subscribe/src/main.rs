//! Example: events/wildcard-subscribe
//!
//! Mirrors integration test #25 (TestMqttIntegration_QueueNoWildcardLeak).
//!
//! Wire contract:
//!   Per-run UNIQUE channel "events/leak/<id>/..." so a busy/shared broker cannot
//!   over-count this example's copies. Three overlapping event filters all match
//!   the single publish "events/leak/<id>/x":
//!     - sub1 subscribes exact   `events/leak/<id>/x`  -> KubeMQ exact channel `leak.<id>.x`
//!     - sub2 subscribes subtree `events/leak/<id>/#`  -> KubeMQ channel filter `leak.<id>.>`
//!     - wild subscribes bare    `#`                   -> KubeMQ channel filter `>` (INFORMATIONAL)
//!   pub  publishes to `events/leak/<id>/x` ONCE       -> KubeMQ channel `leak.<id>.x`
//!
//!   Each distinct filter creates an independent bridge registry entry. The single
//!   publish matches all three entries, so mochi fans the injected message out once
//!   per entry — three matching entries deliver EXACTLY 3 copies to each subscriber
//!   that matches (no cross-entry dedup in V1; gotcha #6 / test #25).
//!
//!   The two UNIQUE filters (sub1 exact + sub2 subtree) are scoped to the per-run
//!   id, so each asserts EXACTLY 3 copies. The bare `#` filter is INFORMATIONAL: it
//!   also catches ambient Events traffic on a shared broker, so it asserts >= 3.
//!
//!   QoS  : 1 (AtLeastOnce)
//!   MQTT : 5.0
//!
//!   Non-events wildcard contrast:
//!     Subscribing to `store/#` (non-events pattern) returns SUBACK reason code
//!     0xA2 (Wildcard Subscriptions Not Supported).
//!
//! Run: cargo run -p wildcard-subscribe

use rumqttc::v5::mqttbytes::v5::{Packet, SubscribeReasonCode};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use std::env;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;
use uuid::Uuid;

fn broker_url() -> String {
    env::var("KUBEMQ_MQTT_URL").unwrap_or_else(|_| "tcp://localhost:1883".to_string())
}

fn parse_host_port(url: &str) -> (String, u16) {
    // Strip scheme: tcp:// tls:// ws://
    let stripped = url
        .trim_start_matches("tcp://")
        .trim_start_matches("tls://")
        .trim_start_matches("ws://");
    // Remove path component (e.g. ws://host:8083/)
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    let mut parts = host_port.splitn(2, ':');
    let host = parts.next().unwrap_or("localhost").to_string();
    let port: u16 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(1883);
    (host, port)
}

/// Payload this example publishes; collectors count only copies of THIS message
/// so unrelated event traffic on a shared broker (matched by the bare `#`
/// filter) does not skew the per-entry copy count.
const DEMO_PAYLOAD: &str = "wildcard-overlap-demo";

/// Spawn a background event-loop task that collects incoming Publish packets.
/// Returns a handle and a shared counter of received messages. `filter` is a
/// run-time string (per-run unique filters are built from a UUID).
fn spawn_collector(
    label: &'static str,
    filter: String,
    mut eventloop: rumqttc::v5::EventLoop,
) -> (tokio::task::JoinHandle<()>, Arc<Mutex<Vec<String>>>) {
    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let topic = String::from_utf8_lossy(&p.topic).to_string();
                    let payload = String::from_utf8_lossy(&p.payload).to_string();
                    // Ignore unrelated events that a bare `#` filter catches on a
                    // shared broker — only count this example's payload.
                    if payload != DEMO_PAYLOAD {
                        continue;
                    }
                    println!(
                        "[{}] filter='{}' received on '{}': {}",
                        label, filter, topic, payload
                    );
                    received_clone.lock().await.push(payload);
                }
                Ok(Event::Incoming(Packet::SubAck(ack))) => {
                    for (i, code) in ack.return_codes.iter().enumerate() {
                        match code {
                            SubscribeReasonCode::Success(qos) => {
                                println!("[{}] SUBACK[{}] = granted QoS {:?}", label, i, qos);
                            }
                            SubscribeReasonCode::WildcardSubscriptionsNotSupported => {
                                println!(
                                    "[{}] SUBACK[{}] = 0xA2 WildcardSubscriptionsNotSupported \
                                     (expected for non-events wildcard)",
                                    label, i
                                );
                            }
                            other => {
                                println!("[{}] SUBACK[{}] = error {:?}", label, i, other);
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[{}] event loop error: {}", label, e);
                    return;
                }
            }
        }
    });
    (handle, received)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    let (host, port) = parse_host_port(&url);
    let run_id = Uuid::new_v4().to_string()[..8].to_string();

    // Per-run UNIQUE channel so this example's three overlapping filters target a
    // freshly minted KubeMQ channel that no prior/other run reuses.
    let publish_topic = format!("events/leak/{run_id}/x"); // unique exact
    let subtree_filter = format!("events/leak/{run_id}/#"); // unique subtree ('#' -> '>')

    println!("=== KubeMQ MQTT — Events: Wildcard Subscribe ===");
    println!("Broker: {}", url);
    println!();
    println!("Wire contract:");
    println!("  '+' maps to '*' server-side  (single-level wildcard)");
    println!("  '#' maps to '>' server-side  (multi-level wildcard)");
    println!("  Wildcards allowed ONLY on events/... subscriptions.");
    println!("  Non-events wildcard (e.g. store/#) -> SUBACK 0xA2.");
    println!();
    println!("Per-run unique channel: {publish_topic}");
    println!();

    // -----------------------------------------------------------------------
    // 1.  Create three subscriber clients — each registers one filter.
    //     Three distinct filters -> three independent bridge registry entries.
    //     A single publish matching all three entries => exactly 3 delivered
    //     copies (one per entry; no cross-entry dedup in V1).
    // -----------------------------------------------------------------------

    // The broker advertises MaxPacket = 4 MB. rumqttc's default incoming-packet
    // limit is only 10 KB, so a bare `#` filter that catches a large unrelated
    // event would otherwise abort the connection mid-deserialize. Raise the limit
    // to the broker maximum on every wildcard client.
    const MAX_PACKET: u32 = 4 * 1024 * 1024;

    // --- Client A: bare '#' (channel filter '>') — INFORMATIONAL --------------
    let id_wild = format!("mqtt-rust-wc-wild-{}", run_id);
    let mut opts_wild = MqttOptions::new(&id_wild, &host, port);
    opts_wild.set_keep_alive(Duration::from_secs(30));
    opts_wild.set_max_packet_size(Some(MAX_PACKET));
    let (client_wild, eventloop_wild) = AsyncClient::new(opts_wild, 32);
    let (_, received_wild) = spawn_collector("wild ", "#".to_string(), eventloop_wild);

    // --- Client B: exact 'events/leak/<id>/x' (UNIQUE) -----------------------
    let id_sub1 = format!("mqtt-rust-wc-sub1-{}", run_id);
    let mut opts_sub1 = MqttOptions::new(&id_sub1, &host, port);
    opts_sub1.set_keep_alive(Duration::from_secs(30));
    opts_sub1.set_max_packet_size(Some(MAX_PACKET));
    let (client_sub1, eventloop_sub1) = AsyncClient::new(opts_sub1, 32);
    let (_, received_sub1) = spawn_collector("sub1 ", publish_topic.clone(), eventloop_sub1);

    // --- Client C: 'events/leak/<id>/#' (UNIQUE subtree) ----------------------
    let id_sub2 = format!("mqtt-rust-wc-sub2-{}", run_id);
    let mut opts_sub2 = MqttOptions::new(&id_sub2, &host, port);
    opts_sub2.set_keep_alive(Duration::from_secs(30));
    opts_sub2.set_max_packet_size(Some(MAX_PACKET));
    let (client_sub2, eventloop_sub2) = AsyncClient::new(opts_sub2, 32);
    let (_, received_sub2) = spawn_collector("sub2 ", subtree_filter.clone(), eventloop_sub2);

    // -----------------------------------------------------------------------
    // 2.  Register subscriptions.
    //     '#' alone (no prefix) => DefaultPattern events (bridge treats it as
    //     channel filter '>'). This is the INFORMATIONAL filter.
    // -----------------------------------------------------------------------
    println!(
        "[wild ] subscribing '#'                  -> KubeMQ channel filter '>' (informational)"
    );
    client_wild.subscribe("#", QoS::AtLeastOnce).await?;

    println!("[sub1 ] subscribing '{publish_topic}' -> KubeMQ exact channel 'leak.{run_id}.x'");
    client_sub1
        .subscribe(&publish_topic, QoS::AtLeastOnce)
        .await?;

    println!("[sub2 ] subscribing '{subtree_filter}' -> KubeMQ channel filter 'leak.{run_id}.>'");
    client_sub2
        .subscribe(&subtree_filter, QoS::AtLeastOnce)
        .await?;

    // -----------------------------------------------------------------------
    // 2b. Demonstrate non-events wildcard rejection.
    //     'store/#' -> SUBACK 0xA2 (Wildcard Subscriptions Not Supported).
    //     We use a throwaway client so its rejection does not affect the main
    //     subscribers above.
    // -----------------------------------------------------------------------
    println!();
    println!("[rej  ] subscribing 'store/#'            -> expect SUBACK 0xA2");
    let id_rej = format!("mqtt-rust-wc-rej-{}", run_id);
    let mut opts_rej = MqttOptions::new(&id_rej, &host, port);
    opts_rej.set_keep_alive(Duration::from_secs(30));
    let (client_rej, eventloop_rej) = AsyncClient::new(opts_rej, 8);
    let (_rej_handle, _) = spawn_collector("rej  ", "store/#".to_string(), eventloop_rej);
    client_rej.subscribe("store/#", QoS::AtLeastOnce).await?;

    // Allow subscriptions and the rejection SUBACK to propagate.
    println!();
    println!("Waiting 700 ms for subscriptions to propagate...");
    tokio::time::sleep(Duration::from_millis(700)).await;

    // -----------------------------------------------------------------------
    // 3.  Publisher client — separate connection.
    // -----------------------------------------------------------------------
    let id_pub = format!("mqtt-rust-wc-pub-{}", run_id);
    let mut opts_pub = MqttOptions::new(&id_pub, &host, port);
    opts_pub.set_keep_alive(Duration::from_secs(30));
    let (client_pub, mut eventloop_pub) = AsyncClient::new(opts_pub, 8);

    // Drive the publisher event-loop just enough to deliver the PUBACK.
    tokio::spawn(async move {
        loop {
            match eventloop_pub.poll().await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[pub  ] event loop error: {}", e);
                    return;
                }
            }
        }
    });

    // -----------------------------------------------------------------------
    // 4.  Publish to events/leak/<id>/x ONCE.
    //     Three bridge entries match -> exactly 3 copies delivered (no dedup).
    // -----------------------------------------------------------------------
    println!("Publishing to '{publish_topic}' ONCE (QoS 1)...");
    println!("  Three matching bridge entries -> each subscriber gets 3 copies.");
    client_pub
        .publish(
            &publish_topic,
            QoS::AtLeastOnce,
            false,
            DEMO_PAYLOAD.as_bytes(),
        )
        .await?;
    println!("Published.");
    println!();

    // -----------------------------------------------------------------------
    // 5.  Wait up to 10 s for all three subscribers to collect 3 copies each.
    //     sub1 + sub2 are the UNIQUE filters (asserted == 3); wild is the
    //     INFORMATIONAL filter (asserted >= 3, may catch ambient traffic).
    // -----------------------------------------------------------------------
    const WANT_COPIES: usize = 3;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let w = received_wild.lock().await.len();
        let s1 = received_sub1.lock().await.len();
        let s2 = received_sub2.lock().await.len();

        if w >= WANT_COPIES && s1 >= WANT_COPIES && s2 >= WANT_COPIES {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            eprintln!(
                "Timeout waiting for {} copies on all three subscribers \
                 (wild={}, sub1={}, sub2={}). Broker may need a moment to \
                 register all three bridge entries before the publish.",
                WANT_COPIES, w, s1, s2
            );
            std::process::exit(1);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Brief window to surface any extra copies the bare '#' filter may catch.
    tokio::time::sleep(Duration::from_millis(700)).await;

    let w = received_wild.lock().await.len();
    let s1 = received_sub1.lock().await.len();
    let s2 = received_sub2.lock().await.len();

    println!("Results:");
    println!(
        "  [sub1 ] filter '{publish_topic}' received {} copies (want exactly {})",
        s1, WANT_COPIES
    );
    println!(
        "  [sub2 ] filter '{subtree_filter}' received {} copies (want exactly {})",
        s2, WANT_COPIES
    );
    println!(
        "  [wild ] filter '#' (informational)          received {} copies (want >= {})",
        w, WANT_COPIES
    );
    println!();

    // The two UNIQUE filters must receive EXACTLY 3 copies (one per matching
    // bridge entry; no cross-entry dedup). The bare '#' filter is informational
    // and may catch ambient Events traffic on a shared broker, so it asserts >= 3.
    if s1 == WANT_COPIES && s2 == WANT_COPIES && w >= WANT_COPIES {
        println!("THREE_COPIES_CONFIRMED — one copy per matching bridge registry entry");
        if w > WANT_COPIES {
            println!(
                "  (bare '#' caught {} extra copy(ies) of ambient Events traffic — informational, not an error)",
                w - WANT_COPIES
            );
        }
    } else {
        eprintln!(
            "FAIL: expected sub1==3, sub2==3, wild>=3; got wild={}, sub1={}, sub2={}",
            w, s1, s2
        );
        std::process::exit(1);
    }

    Ok(())
}

// Expected output (order of received lines may vary):
// === KubeMQ MQTT — Events: Wildcard Subscribe ===
// Broker: tcp://localhost:1883
//
// Wire contract:
//   '+' maps to '*' server-side  (single-level wildcard)
//   '#' maps to '>' server-side  (multi-level wildcard)
//   Wildcards allowed ONLY on events/... subscriptions.
//   Non-events wildcard (e.g. store/#) -> SUBACK 0xA2.
//
// Per-run unique channel: events/leak/<id>/x
//
// [wild ] subscribing '#'                  -> KubeMQ channel filter '>' (informational)
// [sub1 ] subscribing 'events/leak/<id>/x' -> KubeMQ exact channel 'leak.<id>.x'
// [sub2 ] subscribing 'events/leak/<id>/#' -> KubeMQ channel filter 'leak.<id>.>'
//
// [rej  ] subscribing 'store/#'            -> expect SUBACK 0xA2
//
// Waiting 700 ms for subscriptions to propagate...
// [wild ] SUBACK[0] = granted QoS AtLeastOnce
// [sub1 ] SUBACK[0] = granted QoS AtLeastOnce
// [sub2 ] SUBACK[0] = granted QoS AtLeastOnce
// [rej  ] SUBACK[0] = 0xA2 WildcardSubscriptionsNotSupported (expected for non-events wildcard)
// Publishing to 'events/leak/<id>/x' ONCE (QoS 1)...
//   Three matching bridge entries -> each subscriber gets 3 copies.
// Published.
//
// [sub1 ] filter='events/leak/<id>/x' received on 'events/leak/<id>/x': wildcard-overlap-demo
// [sub2 ] filter='events/leak/<id>/#' received on 'events/leak/<id>/x': wildcard-overlap-demo
// [wild ] filter='#' received on 'events/leak/<id>/x': wildcard-overlap-demo
// ... (3 copies each) ...
// Results:
//   [sub1 ] filter 'events/leak/<id>/x' received 3 copies (want exactly 3)
//   [sub2 ] filter 'events/leak/<id>/#' received 3 copies (want exactly 3)
//   [wild ] filter '#' (informational)          received 3 copies (want >= 3)
//
// THREE_COPIES_CONFIRMED — one copy per matching bridge registry entry
