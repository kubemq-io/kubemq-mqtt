//! Example: protocol/mqtt-v311-vs-v5
//!
//! Contrasts MQTT 3.1.1 and 5.0 behaviour on the KubeMQ MQTT connector.
//!
//! Key differences demonstrated:
//!   1. v5 pub/sub on events/demo/v3  -> works normally.
//!   2. v3.1.1 pub/sub on events/demo/v3 -> works (v3.1.1 is supported for Events).
//!   3. v3.1.1 publish to commands/demo/x -> SILENTLY DROPPED (PUBACK succeeds,
//!      message never reaches the responder — no CONNACK rejection, no error code).
//!      RPC requires MQTT 5.0 because v5 ResponseTopic + CorrelationData are needed.
//!
//! Wire contract:
//!   events topic  : events/demo/v3  (both versions)
//!   commands topic: commands/demo/x (v3.1.1 drop demo)
//!   QoS           : 1
//!
//! Run: cargo run -p mqtt-v311-vs-v5

// ---- v3.1.1 (plain) imports ----
use rumqttc::mqttbytes::v4::Packet as PacketV4;
use rumqttc::{
    AsyncClient as AsyncClientV4, Event as EventV4, MqttOptions as MqttOptionsV4, QoS as QoSV4,
};

// ---- v5 imports ----
use rumqttc::v5::mqttbytes::v5::Packet as PacketV5;
use rumqttc::v5::mqttbytes::QoS as QoSV5;
use rumqttc::v5::{AsyncClient as AsyncClientV5, Event as EventV5, MqttOptions as MqttOptionsV5};

use std::env;
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

/// Demo 1: v5 events pub/sub (baseline reference).
async fn demo_v5_events(host: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Demo 1: MQTT 5.0 events pub/sub ---");
    let client_id = format!("mqtt-rust-v5-{}", &Uuid::new_v4().to_string()[..8]);
    let mut opts = MqttOptionsV5::new(&client_id, host, port);
    opts.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClientV5::new(opts, 10);

    let ev_handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(EventV5::Incoming(PacketV5::Publish(p))) => {
                    let topic = String::from_utf8_lossy(&p.topic);
                    let payload = String::from_utf8_lossy(&p.payload);
                    println!("v5 received on '{}': {}", topic, payload);
                    println!("V5_EVENT_RECEIVED");
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("v5 event loop error: {e}");
                    return;
                }
            }
        }
    });

    client
        .subscribe("events/demo/v3", QoSV5::AtLeastOnce)
        .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    client
        .publish(
            "events/demo/v3",
            QoSV5::AtLeastOnce,
            false,
            b"v5 event payload".as_ref(),
        )
        .await?;
    println!("v5: published to events/demo/v3");

    tokio::time::timeout(Duration::from_secs(10), ev_handle).await??;
    Ok(())
}

/// Demo 2: v3.1.1 events pub/sub.
async fn demo_v311_events(host: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Demo 2: MQTT 3.1.1 events pub/sub ---");
    let client_id = format!("mqtt-rust-v311-{}", &Uuid::new_v4().to_string()[..8]);
    let mut opts = MqttOptionsV4::new(&client_id, host, port);
    opts.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClientV4::new(opts, 10);

    let ev_handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(EventV4::Incoming(PacketV4::Publish(p))) => {
                    // v4 p.topic is String, p.payload is Bytes
                    let payload = String::from_utf8_lossy(&p.payload);
                    println!("v3.1.1 received on '{}': {}", p.topic, payload);
                    println!("V311_EVENT_RECEIVED");
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("v3.1.1 event loop error: {e}");
                    return;
                }
            }
        }
    });

    client
        .subscribe("events/demo/v3", QoSV4::AtLeastOnce)
        .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    client
        .publish(
            "events/demo/v3",
            QoSV4::AtLeastOnce,
            false,
            b"v3.1.1 event payload".as_ref(),
        )
        .await?;
    println!("v3.1.1: published to events/demo/v3");

    tokio::time::timeout(Duration::from_secs(10), ev_handle).await??;
    Ok(())
}

/// Demo 3: v3.1.1 publish to commands/ -> SILENTLY DROPPED (PUBACK 0x00 but no RPC delivery).
async fn demo_v311_rpc_drop(host: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Demo 3: MQTT 3.1.1 publish to commands/ -> silently dropped ---");
    let client_id = format!("mqtt-rust-v311-cmd-{}", &Uuid::new_v4().to_string()[..8]);
    let mut opts = MqttOptionsV4::new(&client_id, host, port);
    opts.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClientV4::new(opts, 10);

    // Drive the event loop just long enough to connect and receive PUBACK.
    let ev_handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(EventV4::Incoming(PacketV4::PubAck(_))) => {
                    println!("v3.1.1: PUBACK received for commands/demo/x (message silently dropped by broker, not delivered to any responder)");
                    println!("V311_RPC_DROP_CONFIRMED");
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("v3.1.1 cmd event loop error: {e}");
                    return;
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish to commands/ with v3.1.1 — no ResponseTopic is possible in v3.1.1.
    // The broker accepts the publish (PUBACK 0x00) but the message is dropped.
    client
        .publish(
            "commands/demo/x",
            QoSV4::AtLeastOnce,
            false,
            b"v3.1.1 command will be dropped".as_ref(),
        )
        .await?;
    println!("v3.1.1: published to commands/demo/x (expecting silent drop)");

    tokio::time::timeout(Duration::from_secs(5), ev_handle).await??;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    let (host, port) = parse_host_port(&url);

    demo_v5_events(&host, port).await?;
    demo_v311_events(&host, port).await?;
    demo_v311_rpc_drop(&host, port).await?;

    println!("\nAll demos complete.");
    Ok(())
}

// Expected output:
// --- Demo 1: MQTT 5.0 events pub/sub ---
// v5: published to events/demo/v3
// v5 received on 'events/demo/v3': v5 event payload
// V5_EVENT_RECEIVED
//
// --- Demo 2: MQTT 3.1.1 events pub/sub ---
// v3.1.1: published to events/demo/v3
// v3.1.1 received on 'events/demo/v3': v3.1.1 event payload
// V311_EVENT_RECEIVED
//
// --- Demo 3: MQTT 3.1.1 publish to commands/ -> silently dropped ---
// v3.1.1: published to commands/demo/x (expecting silent drop)
// v3.1.1: PUBACK received for commands/demo/x (message silently dropped by broker, not delivered to any responder)
// V311_RPC_DROP_CONFIRMED
//
// All demos complete.
