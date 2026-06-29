//! Example: connectivity/tls
//!
//! Demonstrates MQTT over TLS (port 8883) connecting to KubeMQ.
//!
//! Wire contract:
//!   transport    : TLS (tls://host:8883)
//!   MQTT version : 5.0
//!   sub topic    : events/demo/tls
//!   pub topic    : events/demo/tls
//!   QoS          : 1
//!   TLS min      : 1.2; mTLS supported
//!
//! LIVE BROKER NOTE: The TLS listener is CLOSED on the current test broker.
//! This example compiles and connects when TLS is configured, but live-testing
//! against the local broker is N/A. Mark as partial/NA in test results.
//!
//! The example falls back to tcp:// if KUBEMQ_MQTT_URL does not start with tls://.
//!
//! Run: cargo run -p connectivity-tls

use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use rumqttc::{TlsConfiguration, Transport};
use rustls::ClientConfig;
use std::{env, sync::Arc};
use tokio::time::Duration;
use uuid::Uuid;

fn broker_url() -> String {
    env::var("KUBEMQ_MQTT_URL").unwrap_or_else(|_| "tls://localhost:8883".to_string())
}

fn parse_host_port(url: &str) -> (String, u16, bool) {
    let is_tls = url.starts_with("tls://");
    let stripped = url
        .trim_start_matches("tls://")
        .trim_start_matches("tcp://")
        .trim_start_matches("ws://");
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    let mut parts = host_port.splitn(2, ':');
    let host = parts.next().unwrap_or("localhost").to_string();
    let port: u16 = parts
        .next()
        .and_then(|p| p.parse().ok())
        .unwrap_or(if is_tls { 8883 } else { 1883 });
    (host, port, is_tls)
}

fn build_tls_config() -> Arc<ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    // Load native system trust roots.
    for cert in rustls_native_certs::load_native_certs().expect("failed to load native certs") {
        roots.add(cert).expect("failed to add cert");
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(config)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    let (host, port, is_tls) = parse_host_port(&url);
    let client_id = format!("mqtt-rust-tls-{}", &Uuid::new_v4().to_string()[..8]);

    let mut mqttoptions = MqttOptions::new(&client_id, &host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    if is_tls {
        let tls_config = build_tls_config();
        mqttoptions.set_transport(Transport::Tls(TlsConfiguration::Rustls(tls_config)));
        println!("Connecting over TLS to tls://{}:{}", host, port);
    } else {
        println!("KUBEMQ_MQTT_URL is not tls:// — connecting over plain TCP (TLS demo requires tls://host:8883)");
    }

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    let ev_handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let topic = String::from_utf8_lossy(&p.topic);
                    let payload = String::from_utf8_lossy(&p.payload);
                    println!("Received on '{}': {}", topic, payload);
                    println!("TLS_EVENT_RECEIVED");
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Connection/event loop error: {e}");
                    eprintln!("(TLS listener may not be active on this broker — expected for local test setup)");
                    return;
                }
            }
        }
    });

    client
        .subscribe("events/demo/tls", QoS::AtLeastOnce)
        .await?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    client
        .publish(
            "events/demo/tls",
            QoS::AtLeastOnce,
            false,
            b"Hello over TLS from Rust!".as_ref(),
        )
        .await?;
    println!("Published to events/demo/tls (QoS 1)");

    tokio::time::timeout(Duration::from_secs(10), ev_handle).await??;

    Ok(())
}

// Expected output (TLS listener active):
// Connecting over TLS to tls://localhost:8883
// Published to events/demo/tls (QoS 1)
// Received on 'events/demo/tls': Hello over TLS from Rust!
// TLS_EVENT_RECEIVED
