//! Example: queues/ack-and-redelivery
//!
//! Demonstrates KubeMQ Queue ack-on-PUBACK behaviour and the two redelivery
//! paths using MQTT 5.0 + rumqttc:
//!
//!  1. **Fast path (demonstrated)**: Consumer 1 receives the message but
//!     disconnects WITHOUT calling `client.ack()` (no PUBACK sent).  KubeMQ
//!     detects the TCP close and immediately requeues the in-flight message,
//!     which Consumer 2 then receives and properly acks.
//!
//!  2. **Slow path (documented, not run)**: No PUBACK within
//!     `QueueAckTimeoutSeconds` (default 30 s) → KubeMQ NAcks the delivery
//!     and redelivers to any subscriber on the same `$share` group.
//!
//! Wire contract (MQTT 5.0 only):
//!   produce topic  : queues/demo/rq              (plain publish, QoS 1)
//!   consume topic  : $share/g1/queues/demo/rq    (QoS 1)
//!   QoS 0 on shared sub       → SUBACK 0x83 (rejected)
//!   Plain queues/… subscribe  → SUBACK 0x83 (rejected)
//!   $share group name         : audit/metrics only — all groups compete in ONE
//!                               shared KubeMQ queue pool (NOT per-group copies)
//!
//! Manual-ack in rumqttc v5:
//!   Set `mqttoptions.set_manual_acks(true)`.  When set, the event loop will
//!   NOT send PUBACK automatically.  Call `client.ack(&publish).await` to send
//!   PUBACK and remove the message from the queue.  Dropping the client without
//!   calling `ack` leaves the message in-flight; KubeMQ requeues it on TCP close.
//!
//! Run: cargo run -p ack-and-redelivery

use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use std::env;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio::time::Duration;
use uuid::Uuid;

fn broker_url() -> String {
    env::var("KUBEMQ_MQTT_URL").unwrap_or_else(|_| "tcp://localhost:1883".to_string())
}

fn parse_host_port(url: &str) -> (String, u16) {
    let stripped = url
        .trim_start_matches("tcp://")
        .trim_start_matches("tls://")
        .trim_start_matches("ws://");
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    let mut parts = host_port.splitn(2, ':');
    let host = parts.next().unwrap_or("localhost").to_string();
    let port: u16 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(1883);
    (host, port)
}

const PRODUCE_TOPIC: &str = "queues/demo/rq";
const CONSUME_TOPIC: &str = "$share/g1/queues/demo/rq";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    let (host, port) = parse_host_port(&url);
    let sfx = &Uuid::new_v4().to_string()[..8];

    // -----------------------------------------------------------------------
    // Step 1: Consumer 1 — manual-ack mode
    // -----------------------------------------------------------------------
    println!("=== Step 1: Consumer 1 subscribes (manual-ack mode — will NOT send PUBACK) ===");

    let mut opts1 = MqttOptions::new(format!("rust-q-con1-{sfx}"), &host, port);
    opts1.set_keep_alive(Duration::from_secs(30));
    // manual_acks=true: the event loop will NOT send PUBACK automatically.
    // We must call client1.ack(&publish).await to ack, or simply drop / disconnect
    // to leave the message in-flight and trigger KubeMQ's immediate requeue.
    opts1.set_manual_acks(true);

    let (client1, mut eventloop1) = AsyncClient::new(opts1, 10);

    // Channel for con1 to hand the received Publish to the main task.
    let (con1_tx, con1_rx) = oneshot::channel::<rumqttc::v5::mqttbytes::v5::Publish>();
    let con1_tx = Arc::new(Mutex::new(Some(con1_tx)));
    let con1_tx_clone = con1_tx.clone();

    // Spawn the event loop for consumer 1. We stop polling as soon as we have
    // received the publish (the main task will then disconnect con1).
    let ev1_handle = tokio::spawn(async move {
        loop {
            match eventloop1.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let payload = String::from_utf8_lossy(&p.payload).to_string();
                    println!(
                        "[Consumer 1] Received message (PUBACK NOT sent): {}",
                        payload
                    );
                    // Hand the publish to the main task for potential ack — but
                    // in this example we will NOT ack it.
                    if let Some(tx) = con1_tx_clone.lock().await.take() {
                        let _ = tx.send(p);
                    }
                    // Keep polling so the event loop stays alive until the main
                    // task calls client1.disconnect().
                }
                Ok(_) => {}
                Err(e) => {
                    // Connection closed by main task's disconnect — expected.
                    eprintln!("[Consumer 1] Event loop ended: {e}");
                    return;
                }
            }
        }
    });

    client1.subscribe(CONSUME_TOPIC, QoS::AtLeastOnce).await?;
    println!("[Consumer 1] Subscribed to {CONSUME_TOPIC:?} (QoS 1, manual-ack)");

    // Small pause to let the subscription register in the MQTT bridge.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // -----------------------------------------------------------------------
    // Step 2: Producer publishes one message
    // -----------------------------------------------------------------------
    println!("\n=== Step 2: Producer publishes one message ===");

    let mut opts_prod = MqttOptions::new(format!("rust-q-prod-{sfx}"), &host, port);
    opts_prod.set_keep_alive(Duration::from_secs(30));
    let (client_prod, mut eventloop_prod) = AsyncClient::new(opts_prod, 10);

    // Drain the producer event loop in the background.
    tokio::spawn(async move {
        loop {
            match eventloop_prod.poll().await {
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });

    let payload = format!(
        r#"{{"job":1,"note":"will be requeued on disconnect","ts":"{}"}}"#,
        chrono_now_rfc3339()
    );
    client_prod
        .publish(PRODUCE_TOPIC, QoS::AtLeastOnce, false, payload.into_bytes())
        .await?;
    println!("[Producer] Published to {PRODUCE_TOPIC:?} (QoS 1)");

    // -----------------------------------------------------------------------
    // Step 3: Wait for Consumer 1 to receive the message
    // -----------------------------------------------------------------------
    println!("\n=== Step 3: Consumer 1 receives the message (no PUBACK sent) ===");

    let _in_flight = tokio::time::timeout(Duration::from_secs(15), con1_rx)
        .await
        .map_err(|_| "Timed out waiting for Consumer 1 to receive the message")?
        .map_err(|_| "Consumer 1 channel dropped unexpectedly")?;

    // -----------------------------------------------------------------------
    // Step 4: Connect Consumer 2 BEFORE disconnecting Consumer 1
    // -----------------------------------------------------------------------
    println!("\n=== Step 4: Consumer 2 connects and subscribes (auto-ack) ===");
    println!("  (Ready to pick up the requeued message the moment it is re-enqueued.)");

    let mut opts2 = MqttOptions::new(format!("rust-q-con2-{sfx}"), &host, port);
    opts2.set_keep_alive(Duration::from_secs(30));
    // manual_acks=false (default): rumqttc sends PUBACK automatically when it
    // delivers the Publish event — this fully acks the message on KubeMQ.

    let (client2, mut eventloop2) = AsyncClient::new(opts2, 10);

    let (con2_tx, con2_rx) = oneshot::channel::<String>();
    let con2_tx = Arc::new(Mutex::new(Some(con2_tx)));

    let ev2_handle = tokio::spawn(async move {
        loop {
            match eventloop2.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let payload = String::from_utf8_lossy(&p.payload).to_string();
                    println!("[Consumer 2] Received redelivered message (auto-PUBACK): {payload}");
                    if let Some(tx) = con2_tx.lock().await.take() {
                        let _ = tx.send(payload);
                    }
                    return; // Done — event loop will be dropped.
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[Consumer 2] Event loop ended: {e}");
                    return;
                }
            }
        }
    });

    client2.subscribe(CONSUME_TOPIC, QoS::AtLeastOnce).await?;
    println!("[Consumer 2] Subscribed to {CONSUME_TOPIC:?} (QoS 1, auto-ack)");

    // Let the subscription settle.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // -----------------------------------------------------------------------
    // Step 5: Consumer 1 disconnects WITHOUT acking
    // -----------------------------------------------------------------------
    println!("\n=== Step 5: Consumer 1 disconnects WITHOUT acking ===");
    println!("  KubeMQ detects the TCP close and immediately requeues the in-flight message.");

    // Disconnect triggers a clean MQTT DISCONNECT (reason 0x00).  Because the
    // QoS 1 PUBACK was never sent, KubeMQ treats this as a NAck on the pending
    // delivery and requeues the message immediately (fast-path redelivery).
    client1.disconnect().await?;
    // Wait for the event loop task to finish.
    let _ = tokio::time::timeout(Duration::from_secs(5), ev1_handle).await;
    println!("[Consumer 1] Disconnected — in-flight message has been requeued by KubeMQ.");

    // -----------------------------------------------------------------------
    // Step 6: Consumer 2 receives the requeued message
    // -----------------------------------------------------------------------
    println!("\n=== Step 6: Consumer 2 receives the requeued message ===");

    let received = tokio::time::timeout(Duration::from_secs(15), con2_rx)
        .await
        .map_err(|_| "Timed out waiting for Consumer 2 to receive the redelivered message")?
        .map_err(|_| "Consumer 2 channel dropped unexpectedly")?;

    println!("\nAt-least-once delivery confirmed — message survived the disconnect.");
    println!("  Redelivered payload: {received}");

    client2.disconnect().await?;
    client_prod.disconnect().await?;
    let _ = tokio::time::timeout(Duration::from_secs(5), ev2_handle).await;

    // -----------------------------------------------------------------------
    // Summary
    // -----------------------------------------------------------------------
    println!("\n--- Ack & Redelivery summary ---");
    println!("Fast path (demonstrated):   disconnect with pending PUBACK");
    println!("                            → KubeMQ immediately requeues the message.");
    println!("Slow path (not run here):   no PUBACK within QueueAckTimeoutSeconds");
    println!("                            (default 30 s) → NAck → redeliver.");
    println!("$share group is one pool:   all $share/<g>/queues/<ch> groups compete");
    println!("                            in ONE KubeMQ queue — NOT per-group copies.");
    println!("Done.");

    Ok(())
}

/// Minimal RFC-3339 timestamp without pulling in chrono.
fn chrono_now_rfc3339() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Format as YYYY-MM-DDTHH:MM:SSZ (UTC approximation — good enough for demo).
    let s = secs;
    let sec = s % 60;
    let min = (s / 60) % 60;
    let hour = (s / 3600) % 24;
    let days = s / 86400; // days since epoch
                          // Approximate calendar from epoch (correct for ~100 years, demo use only).
    let year = 1970 + days / 365;
    let day_of_year = days % 365;
    let month = day_of_year / 30 + 1;
    let day = day_of_year % 30 + 1;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}
