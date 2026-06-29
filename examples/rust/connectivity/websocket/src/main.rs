//! Example: connectivity/websocket
//!
//! Demonstrates MQTT 5.0 over WebSocket connecting to KubeMQ.
//!
//! Wire contract:
//!   transport    : WebSocket (ws://host:8083/)
//!   MQTT version : 5.0
//!   sub topic    : events/demo/ws
//!   pub topic    : events/demo/ws
//!   QoS          : 1
//!   KubeMQ pattern: Events (topic prefix events/ -> KubeMQ Events)
//!
//! rumqttc WebSocket transport:
//!   When Transport::Ws is set, the `host` parameter in MqttOptions::new MUST be the
//!   full ws:// URL (including scheme, host, port, path). The port parameter in
//!   MqttOptions::new is ignored by rumqttc when a ws:// URL is detected in the host field.
//!
//! Env:
//!   KUBEMQ_MQTT_URL  — ws://host:port/path (default ws://localhost:8083/)
//!                      tcp:// and tls:// are accepted as fallback (plain TCP, no WS).
//!
//! Run: cargo run -p connectivity-websocket

use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use rumqttc::Transport;
use std::env;
use tokio::time::Duration;
use uuid::Uuid;

/// Read KUBEMQ_MQTT_URL; default to WebSocket URL for this example.
fn broker_url() -> String {
    env::var("KUBEMQ_MQTT_URL").unwrap_or_else(|_| "ws://localhost:8083/".to_string())
}

/// Build MqttOptions appropriate for the URL scheme.
///
/// rumqttc WebSocket quirk: pass the FULL ws:// URL as the `host` argument.
/// The port argument is unused when a ws:// URL is in the host field.
fn build_options(url: &str, client_id: &str) -> MqttOptions {
    if url.starts_with("ws://") || url.starts_with("wss://") {
        // Pass the full URL as `host`; port is ignored by rumqttc for WS.
        let mut opts = MqttOptions::new(client_id, url, 8083);
        opts.set_transport(Transport::Ws);
        opts.set_keep_alive(Duration::from_secs(30));
        println!("Transport : WebSocket");
        println!("URL       : {url}");
        opts
    } else {
        // Fallback: plain TCP — allows testing with tcp:// KUBEMQ_MQTT_URL.
        println!("KUBEMQ_MQTT_URL scheme is not ws:// — falling back to plain TCP.");
        println!("(WebSocket demo requires ws://host:8083/)");
        let stripped = url
            .trim_start_matches("tcp://")
            .trim_start_matches("tls://");
        let host_port = stripped.split('/').next().unwrap_or(stripped);
        let mut parts = host_port.splitn(2, ':');
        let host = parts.next().unwrap_or("localhost");
        let port: u16 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(1883);
        let mut opts = MqttOptions::new(client_id, host, port);
        opts.set_keep_alive(Duration::from_secs(30));
        opts
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    let client_id = format!("mqtt-rust-ws-{}", &Uuid::new_v4().to_string()[..8]);

    println!("=== KubeMQ MQTT 5.0 over WebSocket ===");
    println!("Client ID : {client_id}");

    let mqttoptions = build_options(&url, &client_id);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // Drain the event loop in a background task.
    // Returns once a published message is received or an unrecoverable error occurs.
    let ev_handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    println!("[event-loop] Connected (CONNACK received)");
                }
                Ok(Event::Incoming(Packet::SubAck(sa))) => {
                    println!("[event-loop] Subscribed (SUBACK pkid={})", sa.pkid);
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let topic = String::from_utf8_lossy(&p.topic);
                    let payload = String::from_utf8_lossy(&p.payload);
                    println!("[event-loop] Received on '{topic}': {payload}");
                    println!("WS_EVENT_RECEIVED");
                    return Ok::<(), String>(());
                }
                Ok(_) => {} // PubAck, PingResp, etc.
                Err(e) => {
                    // MQTT 5 reason codes relevant to WebSocket / events:
                    //   0x80 broker-not-ready, 0x83 impl-specific, 0x8F topic-filter-invalid
                    // Connection errors are printed here for diagnostics.
                    eprintln!("[event-loop] Error: {e}");
                    return Err(e.to_string());
                }
            }
        }
    });

    // Step 1: Subscribe to the events topic BEFORE publishing.
    // Topic prefix 'events/' maps to KubeMQ Events pattern.
    let sub_topic = "events/demo/ws";
    client.subscribe(sub_topic, QoS::AtLeastOnce).await?;
    println!("Subscribed to '{sub_topic}' (QoS 1)");

    // Brief pause to let the broker process the SUBSCRIBE before we publish.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Step 2: Publish a message on the same topic.
    // retain=false — KubeMQ MQTT forces RetainAvailable=0; a retained publish would be
    // silently dropped with PUBACK 0x00, so we never set retain=true here.
    let pub_topic = "events/demo/ws";
    let payload = b"Hello over WebSocket from Rust!";
    client
        .publish(pub_topic, QoS::AtLeastOnce, false, payload.as_ref())
        .await?;
    println!(
        "Published  to '{pub_topic}' (QoS 1, {} bytes)",
        payload.len()
    );

    // Step 3: Wait for the loopback delivery (up to 10 s).
    match tokio::time::timeout(Duration::from_secs(10), ev_handle).await {
        Ok(Ok(Ok(()))) => {
            println!("=== Success: message round-tripped over WebSocket ===");
        }
        Ok(Ok(Err(e))) => {
            eprintln!("Event-loop returned error: {e}");
            std::process::exit(1);
        }
        Ok(Err(e)) => {
            eprintln!("Task panicked: {e}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("Timeout: no message received within 10 s");
            std::process::exit(1);
        }
    }

    Ok(())
}

// Expected output:
// === KubeMQ MQTT 5.0 over WebSocket ===
// Client ID : mqtt-rust-ws-<8 hex chars>
// Transport : WebSocket
// URL       : ws://127.0.0.1:8083/
// Subscribed to 'events/demo/ws' (QoS 1)
// [event-loop] Connected (CONNACK received)
// [event-loop] Subscribed (SUBACK pkid=1)
// Published  to 'events/demo/ws' (QoS 1, 31 bytes)
// [event-loop] Received on 'events/demo/ws': Hello over WebSocket from Rust!
// WS_EVENT_RECEIVED
// === Success: message round-tripped over WebSocket ===
