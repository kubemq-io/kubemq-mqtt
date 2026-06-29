//! Example: queues/shared-consume
//!
//! Demonstrates MQTT 5.0 shared subscription for consuming from a KubeMQ Queue.
//!
//! Wire contract:
//!   produce topic : queues/demo/q                          -> KubeMQ Queues channel demo.q
//!   consume topic : $share/g1/queues/demo/q  at QoS >= 1  -> KubeMQ Queues (consume)
//!   QoS           : 1 (QoS 0 -> SUBACK 0x83; plain queues/... subscribe -> SUBACK 0x83)
//!   MQTT version  : 5.0  (shared subscriptions require MQTT 5.0)
//!
//! GOTCHA — $share group name is audit/metrics-only:
//!   ALL groups compete in ONE shared KubeMQ queue pool (NOT per-group copies).
//!   "g1" and "g2" are NOT separate queues; both pull from the same pool.
//!
//! GOTCHA — QoS 0 rejected:
//!   Subscribing to $share/.../queues/... at QoS 0 returns SUBACK 0x83.
//!   Plain queues/... subscribe (without $share prefix) also returns SUBACK 0x83.
//!
//! Ack model: PUBACK = ack. No PUBACK within QueueAckTimeoutSeconds (30 s default)
//! -> message is redelivered. Disconnect with unacked messages -> immediate requeue.
//!
//! Run: cargo run -p shared-consume

use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use std::env;
use tokio::time::Duration;
use uuid::Uuid;

const PRODUCE_TOPIC: &str = "queues/demo/q";
const CONSUME_TOPIC: &str = "$share/g1/queues/demo/q";

/// Read broker URL from environment; default tcp://localhost:1883.
fn broker_url() -> String {
    env::var("KUBEMQ_MQTT_URL").unwrap_or_else(|_| "tcp://localhost:1883".to_string())
}

/// Parse "tcp://host:port", "tls://host:port", or "ws://host:port/path"
/// into (host, port).
fn parse_host_port(url: &str) -> (String, u16) {
    let stripped = url
        .trim_start_matches("tcp://")
        .trim_start_matches("tls://")
        .trim_start_matches("ws://");
    // Drop any path component (e.g. "/mqtt" in ws URLs).
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    let mut parts = host_port.splitn(2, ':');
    let host = parts.next().unwrap_or("localhost").to_string();
    let port: u16 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(1883);
    (host, port)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    println!("Connecting to broker: {}", url);
    let (host, port) = parse_host_port(&url);
    let sfx = &Uuid::new_v4().to_string()[..8];

    // -----------------------------------------------------------------------
    // Step 1: Consumer subscribes via shared subscription at QoS 1.
    //
    // Two separate MQTT clients are used (consumer + producer) so their
    // event loops do not share the same connection and buffer.
    // -----------------------------------------------------------------------
    println!(
        "[consumer] Connecting and subscribing to {} at QoS 1...",
        CONSUME_TOPIC
    );

    // MQTT 5.0 is required for shared subscriptions ($share/...).
    let mut con_opts = MqttOptions::new(format!("rust-q-con-{sfx}"), &host, port);
    con_opts.set_keep_alive(Duration::from_secs(30));
    // clean_start=false: on reconnect the same node restores subscriptions
    // (sessions are in-memory node-local per KubeMQ broker).
    con_opts.set_clean_start(false);

    let (consumer, mut con_eventloop) = AsyncClient::new(con_opts, 10);

    // Spawn the consumer event loop.
    // rumqttc sends PUBACK automatically for QoS 1 — this acks the message
    // and removes it from the KubeMQ queue.
    let (tx, rx) = tokio::sync::oneshot::channel::<String>();
    let tx = std::sync::Arc::new(tokio::sync::Mutex::new(Some(tx)));

    let ev_handle = tokio::spawn(async move {
        loop {
            match con_eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let payload = String::from_utf8_lossy(&p.payload).to_string();
                    println!("[consumer] Received on '{}': {}", CONSUME_TOPIC, payload);
                    // rumqttc sends PUBACK automatically for QoS 1 — this acks
                    // the message and removes it from the KubeMQ queue.
                    println!("[consumer] PUBACK sent automatically -> message acked");
                    if let Some(sender) = tx.lock().await.take() {
                        let _ = sender.send(payload);
                    }
                    println!("QUEUE_CONSUMED");
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[consumer] Event loop error: {e}");
                    return;
                }
            }
        }
    });

    // Subscribe BEFORE producing so no messages are missed.
    //
    // Required topic form: $share/<group>/queues/<channel>  at QoS 1.
    //
    // WRONG forms (both return SUBACK 0x83):
    //   - QoS 0:           $share/g1/queues/demo/q  QoS 0
    //   - Plain subscribe: queues/demo/q             QoS 1
    consumer.subscribe(CONSUME_TOPIC, QoS::AtLeastOnce).await?;

    // Wait for the SUBSCRIBE/SUBACK round-trip to complete in the broker.
    tokio::time::sleep(Duration::from_millis(400)).await;
    println!("[consumer] Subscription active");

    // -----------------------------------------------------------------------
    // Step 2: Producer publishes a queue message on the plain produce topic.
    // -----------------------------------------------------------------------
    println!("[producer] Connecting...");
    let mut prod_opts = MqttOptions::new(format!("rust-q-prod-{sfx}"), &host, port);
    prod_opts.set_keep_alive(Duration::from_secs(30));
    let (producer, mut prod_eventloop) = AsyncClient::new(prod_opts, 10);

    // Drain the producer event loop in the background (required so PUBACK flows).
    tokio::spawn(async move {
        loop {
            match prod_eventloop.poll().await {
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });

    let order_payload = r#"{"order_id":"ORD-001","item":"widget","qty":3}"#;
    println!("[producer] Publishing to {} (QoS 1)...", PRODUCE_TOPIC);
    producer
        .publish(
            PRODUCE_TOPIC,
            QoS::AtLeastOnce,
            false, // retain MUST be false; retain flag is silently dropped by the broker
            order_payload.as_bytes(),
        )
        .await?;
    println!("[producer] Published: {}", order_payload);

    // -----------------------------------------------------------------------
    // Step 3: Wait for the consumer to receive and ack the message.
    // -----------------------------------------------------------------------
    let received = tokio::time::timeout(Duration::from_secs(15), rx)
        .await
        .map_err(|_| "Timed out waiting for consumer to receive the message")?
        .map_err(|_| "Consumer channel dropped unexpectedly")?;

    let _ = tokio::time::timeout(Duration::from_secs(5), ev_handle).await;

    consumer.disconnect().await.ok();
    producer.disconnect().await.ok();

    println!("Round-trip complete. Received: {}", received);
    Ok(())
}

// Expected output:
// Connecting to broker: tcp://localhost:1883
// [consumer] Connecting and subscribing to $share/g1/queues/demo/q at QoS 1...
// [consumer] Subscription active
// [producer] Connecting...
// [producer] Publishing to queues/demo/q (QoS 1)...
// [producer] Published: {"order_id":"ORD-001","item":"widget","qty":3}
// [consumer] Received on '$share/g1/queues/demo/q': {"order_id":"ORD-001","item":"widget","qty":3}
// [consumer] PUBACK sent automatically -> message acked
// QUEUE_CONSUMED
// Round-trip complete. Received: {"order_id":"ORD-001","item":"widget","qty":3}
