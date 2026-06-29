//! Example: connectivity/auth-jwt
//!
//! Demonstrates MQTT CONNECT with a JWT token in the Password field.
//!
//! Wire contract:
//!   MQTT version  : 5.0
//!   CONNECT field : Password = JWT token string (PasswordFlag must be set)
//!                   Username = display-only (not used for auth)
//!   sub topic     : events/demo/auth
//!   pub topic     : events/demo/auth
//!   QoS           : 1
//!
//! Auth is checked at CONNECT time only (no mid-connection recheck).
//! Auth not configured -> open (this broker has auth disabled).
//! Empty password when auth enabled -> CONNACK refused (0x86).
//! ACL deny -> PUBACK 0x87.
//!
//! LIVE BROKER NOTE: Auth appears DISABLED on the local broker.
//! This example connects successfully but full JWT enforcement is not tested.
//! Mark as partial/NA for the JWT-rejection path.
//!
//! The JWT is read from the KUBEMQ_MQTT_JWT env var.
//!
//! Run: cargo run -p connectivity-auth-jwt

use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use std::env;
use tokio::time::Duration;
use uuid::Uuid;

fn broker_url() -> String {
    env::var("KUBEMQ_MQTT_URL").unwrap_or_else(|_| "tcp://localhost:1883".to_string())
}

fn jwt_token() -> String {
    env::var("KUBEMQ_MQTT_JWT").unwrap_or_else(|_| "placeholder-jwt-token".to_string())
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    let (host, port) = parse_host_port(&url);
    let jwt = jwt_token();
    let client_id = format!("mqtt-rust-auth-{}", &Uuid::new_v4().to_string()[..8]);

    let mut mqttoptions = MqttOptions::new(&client_id, &host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));
    // Username is display-only (not used for auth in KubeMQ).
    // Password carries the JWT; PasswordFlag is set implicitly when credentials are provided.
    mqttoptions.set_credentials("kubemq-client", &jwt);

    println!("Connecting with JWT in Password field (Username is display-only)");

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    let ev_handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let topic = String::from_utf8_lossy(&p.topic);
                    let payload = String::from_utf8_lossy(&p.payload);
                    println!("Received on '{}': {}", topic, payload);
                    println!("AUTH_EVENT_RECEIVED");
                    return;
                }
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    println!("CONNACK: {:?}", ack.code);
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Connection error: {e}");
                    eprintln!("(Expected on auth-enabled broker with invalid JWT — CONNACK 0x86)");
                    return;
                }
            }
        }
    });

    client
        .subscribe("events/demo/auth", QoS::AtLeastOnce)
        .await?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    client
        .publish(
            "events/demo/auth",
            QoS::AtLeastOnce,
            false,
            b"Hello from auth-jwt example!".as_ref(),
        )
        .await?;
    println!("Published to events/demo/auth (QoS 1)");

    tokio::time::timeout(Duration::from_secs(10), ev_handle).await??;

    Ok(())
}

// Expected output (auth-disabled broker):
// Connecting with JWT in Password field (Username is display-only)
// CONNACK: Success
// Published to events/demo/auth (QoS 1)
// Received on 'events/demo/auth': Hello from auth-jwt example!
// AUTH_EVENT_RECEIVED
//
// Expected output (auth-enabled broker, invalid JWT):
// Connecting with JWT in Password field (Username is display-only)
// Connection error: ... (CONNACK 0x86 Bad Credentials)
