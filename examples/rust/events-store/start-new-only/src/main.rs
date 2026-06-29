//! Example: events-store/start-new-only
//!
//! Demonstrates the StartNewOnly behavior of KubeMQ Events-Store over MQTT.
//!
//! Wire contract:
//!   topic prefix : store/<ch>  -> KubeMQ Events-Store pattern
//!   channel      : store/demo/s  (KubeMQ channel: demo.s)
//!   QoS          : 1 (AtLeastOnce)
//!   MQTT version : 5.0
//!
//! KEY TEACHING POINT — Events-Store over MQTT is ALWAYS StartNewOnly.
//! There is NO historical replay via MQTT.
//!
//! Flow:
//!   1. Publish 3 "pre-subscribe" messages to store/demo/s  (NOT delivered to any subscriber)
//!   2. Subscribe to store/demo/s
//!   3. Publish 1 "post-subscribe" message to store/demo/s  (DELIVERED)
//!   4. Assert (a) NONE of this run's pre-subscribe messages were replayed and
//!      (b) this run's post-subscribe message arrived.
//!
//! Note on the shared channel:
//!   store/demo/s is a canonical demo channel that may carry concurrent traffic
//!   from other clients (e.g. sibling examples in other languages running at the
//!   same time).  To stay correct under that traffic, every message this run
//!   publishes is tagged with a unique per-run id plus a phase marker
//!   ("pre"/"post").  On receive, each message is classified by run id into
//!   own-pre / own-post / foreign; foreign messages (including non-matching or
//!   non-UTF-8 payloads) are ignored.  The assertions only consider THIS run's
//!   tagged messages, so the StartNewOnly guarantee stays provable without a
//!   brittle exact-count assertion against the shared channel.
//!
//! Run: cargo run -p start-new-only

use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use std::env;
use std::sync::{Arc, Mutex};
use tokio::time::Duration;
use uuid::Uuid;

const CHANNEL: &str = "store/demo/s";

/// Per-run tagged payload: `run_id|phase|body`.
///
/// `run_id` lets the subscriber tell THIS run's traffic apart from concurrent
/// foreign traffic on the shared channel; `phase` ("pre"/"post") distinguishes
/// pre-subscribe messages (which must NOT be replayed) from the post-subscribe
/// message (which must arrive).
fn tag_payload(run_id: &str, phase: &str, body: &str) -> String {
    format!("{run_id}|{phase}|{body}")
}

/// Classification of a received payload relative to THIS run.
#[derive(PartialEq)]
enum Kind {
    OwnPre,
    OwnPost,
    Foreign,
}

/// Classify a received payload by run id and phase.  Anything that does not
/// carry this run's id (including malformed payloads) is `Foreign` and ignored.
fn classify(run_id: &str, payload: &str) -> Kind {
    let mut parts = payload.splitn(3, '|');
    match (parts.next(), parts.next()) {
        (Some(id), Some("pre")) if id == run_id => Kind::OwnPre,
        (Some(id), Some("post")) if id == run_id => Kind::OwnPost,
        _ => Kind::Foreign,
    }
}

fn broker_url() -> String {
    env::var("KUBEMQ_MQTT_URL").unwrap_or_else(|_| "tcp://localhost:1883".to_string())
}

fn parse_host_port(url: &str) -> (String, u16) {
    // Strip scheme (tcp:// tls:// ws://)
    let stripped = url
        .trim_start_matches("tcp://")
        .trim_start_matches("tls://")
        .trim_start_matches("ws://");
    // Remove any path component (e.g. ws://host:8083/)
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    let mut parts = host_port.splitn(2, ':');
    let host = parts.next().unwrap_or("localhost").to_string();
    let port: u16 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(1883);
    (host, port)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    let (host, port) = parse_host_port(&url);
    // Unique per-run id: reused for the client id AND embedded in every payload
    // so the subscriber can ignore concurrent foreign traffic on the shared
    // channel.
    let run_id = Uuid::new_v4().to_string();
    let client_id = format!("mqtt-rust-es-sno-{}", &run_id[..8]);

    println!("=== Events-Store: Start-New-Only Demo ===");
    println!("Broker : {url}");
    println!("Channel: {CHANNEL}  (KubeMQ channel: demo.s)");
    println!("Run ID : {run_id}");
    println!();

    let mut mqttoptions = MqttOptions::new(&client_id, &host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // Collect THIS run's received messages, split by phase, so we can verify
    // StartNewOnly behavior under concurrent foreign traffic on the shared
    // channel.  Foreign messages are ignored by the assertions.
    let own_pre: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let own_post: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let foreign_count = Arc::new(Mutex::new(0usize));
    let own_pre_clone = Arc::clone(&own_pre);
    let own_post_clone = Arc::clone(&own_post);
    let foreign_clone = Arc::clone(&foreign_count);
    let run_id_ev = run_id.clone();

    // Drive the event loop in a background task.
    // It classifies each incoming publish by run id and signals when THIS run's
    // post-subscribe message arrives.
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let ev_handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let topic = String::from_utf8_lossy(&p.topic).to_string();
                    let payload = String::from_utf8_lossy(&p.payload).to_string();
                    match classify(&run_id_ev, &payload) {
                        Kind::OwnPre => {
                            println!("[received] OWN-PRE  topic='{topic}' payload='{payload}'");
                            own_pre_clone.lock().unwrap().push(payload);
                        }
                        Kind::OwnPost => {
                            println!("[received] OWN-POST topic='{topic}' payload='{payload}'");
                            own_post_clone.lock().unwrap().push(payload);
                            // Signal on this run's post-subscribe delivery.
                            if let Some(sender) = tx.lock().unwrap().take() {
                                let _ = sender.send(());
                            }
                        }
                        Kind::Foreign => {
                            println!(
                                "[received] foreign (ignored) topic='{topic}' payload='{payload}'"
                            );
                            *foreign_clone.lock().unwrap() += 1;
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Event loop error: {e}");
                    break;
                }
            }
        }
    });

    // -----------------------------------------------------------------------
    // STEP 1: Publish BEFORE subscribing — these messages are stored by KubeMQ
    //         but will NOT be delivered because Events-Store over MQTT is
    //         ALWAYS StartNewOnly. No historical replay is ever sent.
    // -----------------------------------------------------------------------
    println!("--- Step 1: publish BEFORE subscribing (StartNewOnly: not delivered) ---");
    for i in 1..=3 {
        let body = format!("pre-subscribe message #{i}");
        let msg = tag_payload(&run_id, "pre", &body);
        println!("[published-pre] #{i}: '{body}' -> {CHANNEL}  (will NOT be delivered)");
        client
            .publish(CHANNEL, QoS::AtLeastOnce, false, msg.into_bytes())
            .await?;
    }

    // Give the broker time to process the pre-subscribe publishes.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // -----------------------------------------------------------------------
    // STEP 2: Subscribe to the Events-Store channel.
    //         From this point forward, new events will be delivered.
    //         Previously stored events are NOT replayed.
    // -----------------------------------------------------------------------
    println!();
    println!("--- Step 2: subscribe to {CHANNEL} (StartNewOnly active) ---");
    client.subscribe(CHANNEL, QoS::AtLeastOnce).await?;
    println!("[subscribed] {CHANNEL}  QoS=1");

    // Allow the subscription to register on the broker.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // -----------------------------------------------------------------------
    // STEP 3: Publish AFTER subscribing — only this message arrives.
    // -----------------------------------------------------------------------
    println!();
    println!("--- Step 3: publish AFTER subscribing (this message IS delivered) ---");
    let post_body = "post-subscribe message -- the only one you get";
    let post_msg = tag_payload(&run_id, "post", post_body);
    client
        .publish(
            CHANNEL,
            QoS::AtLeastOnce,
            false,
            post_msg.clone().into_bytes(),
        )
        .await?;
    println!("[published-post] '{post_body}' -> {CHANNEL}");

    // -----------------------------------------------------------------------
    // STEP 4: Wait for the post-subscribe event to arrive (up to 10 s).
    // -----------------------------------------------------------------------
    println!();
    println!("--- Step 4: waiting for delivery ---");
    tokio::time::timeout(Duration::from_secs(10), rx)
        .await
        .map_err(|_| "Timed out waiting for post-subscribe event")??;

    // Brief window to catch any unexpected (pre-subscribe) replay before we
    // assert.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // -----------------------------------------------------------------------
    // Summary: assert (a) NONE of this run's pre-subscribe messages were
    // replayed and (b) this run's post-subscribe message arrived.  Foreign
    // messages on the shared channel are tolerated, not asserted against.
    // -----------------------------------------------------------------------
    let own_pre = own_pre.lock().unwrap();
    let own_post = own_post.lock().unwrap();
    let foreign = *foreign_count.lock().unwrap();
    println!();
    println!("=== Result ===");
    println!("Pre-subscribe published        : 3");
    println!(
        "This run's pre-subscribe replayed : {} (StartNewOnly — expect 0)",
        own_pre.len()
    );
    println!(
        "This run's post-subscribe received: {} (expect >= 1)",
        own_post.len()
    );
    println!("Foreign messages ignored          : {foreign}");
    for (i, m) in own_post.iter().enumerate() {
        println!("  post[{}] '{}'", i + 1, m);
    }

    // (a) The StartNewOnly guarantee: NONE of this run's pre-subscribe messages
    //     are replayed to a subscription that started after they were published.
    assert!(
        own_pre.is_empty(),
        "StartNewOnly violation: {} pre-subscribe message(s) were replayed: {:?}",
        own_pre.len(),
        *own_pre
    );
    // (b) This run's uniquely-tagged post-subscribe message IS delivered.
    assert!(
        own_post.contains(&post_msg),
        "Expected this run's post-subscribe message but got: {:?}",
        *own_post
    );

    println!();
    println!("StartNewOnly confirmed: this run's 3 pre-subscribe publishes were NOT replayed.");
    println!("This run's post-subscribe publish arrived.");
    println!("EVENTS_STORE_START_NEW_ONLY_OK");

    drop(own_pre);
    drop(own_post);
    ev_handle.abort();
    Ok(())
}

// Expected output (run id and foreign count vary per run):
// === Events-Store: Start-New-Only Demo ===
// Broker : tcp://localhost:1883
// Channel: store/demo/s  (KubeMQ channel: demo.s)
// Run ID : 3f2a1c9e-...-...
//
// --- Step 1: publish BEFORE subscribing (StartNewOnly: not delivered) ---
// [published-pre] #1: 'pre-subscribe message #1' -> store/demo/s  (will NOT be delivered)
// [published-pre] #2: 'pre-subscribe message #2' -> store/demo/s  (will NOT be delivered)
// [published-pre] #3: 'pre-subscribe message #3' -> store/demo/s  (will NOT be delivered)
//
// --- Step 2: subscribe to store/demo/s (StartNewOnly active) ---
// [subscribed] store/demo/s  QoS=1
//
// --- Step 3: publish AFTER subscribing (this message IS delivered) ---
// [published-post] 'post-subscribe message -- the only one you get' -> store/demo/s
//
// --- Step 4: waiting for delivery ---
// [received] OWN-POST topic='store/demo/s' payload='3f2a1c9e-...|post|post-subscribe message -- the only one you get'
//
// === Result ===
// Pre-subscribe published        : 3
// This run's pre-subscribe replayed : 0 (StartNewOnly — expect 0)
// This run's post-subscribe received: 1 (expect >= 1)
// Foreign messages ignored          : 0
//   post[1] '3f2a1c9e-...|post|post-subscribe message -- the only one you get'
//
// StartNewOnly confirmed: this run's 3 pre-subscribe publishes were NOT replayed.
// This run's post-subscribe publish arrived.
// EVENTS_STORE_START_NEW_ONLY_OK
