//! Example: events/basic-pubsub
//!
//! Demonstrates fire-and-forget event pub/sub using MQTT 5.0 over the KubeMQ MQTT connector.
//!
//! Wire contract:
//!   topic prefix  : events/<ch>  -> KubeMQ Events pattern
//!   sub topic     : events/demo/+     (single-level wildcard match)
//!   pub topic     : events/demo/x
//!   channel       : demo.x  (KubeMQ maps '/' -> '.' in the channel name)
//!   QoS           : 1 (AtLeastOnce)
//!   v5 user-prop  : k1=v1  (becomes KubeMQ Tag k1=v1,
//!                           echoed back to the subscriber)
//!   MQTT version  : 5.0
//!
//! Two separate client connections are used (subscriber + publisher) as a clarity/best-practice
//! choice that keeps the publish and subscribe roles cleanly separated. This is NOT required
//! because of any loopback restriction: the KubeMQ Events connector DOES deliver a published
//! event back to the same connection when that connection is subscribed to the matching topic
//! (verified against the live connector). A single connection could both publish and receive.
//!
//! Run:
//!   cd examples/rust
//!   KUBEMQ_MQTT_URL=tcp://localhost:1883 cargo run -p basic-pubsub

use rumqttc::v5::mqttbytes::v5::{Packet, PublishProperties};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use std::env;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
use uuid::Uuid;

/// Read the broker URL from the environment variable KUBEMQ_MQTT_URL.
/// Defaults to tcp://localhost:1883.
fn broker_url() -> String {
    env::var("KUBEMQ_MQTT_URL").unwrap_or_else(|_| "tcp://localhost:1883".to_string())
}

/// Parse host and port from a URL with scheme tcp://, tls://, or ws://.
fn parse_host_port(url: &str) -> (String, u16) {
    // Strip scheme prefix.
    let stripped = url
        .trim_start_matches("tcp://")
        .trim_start_matches("tls://")
        .trim_start_matches("ws://");
    // Remove any path component (e.g. ws://host:8083/mqtt).
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    let mut parts = host_port.splitn(2, ':');
    let host = parts.next().unwrap_or("localhost").to_string();
    let port: u16 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(1883);
    (host, port)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    println!("[1] Broker URL: {url}");

    let (host, port) = parse_host_port(&url);

    // The topics we subscribe and publish on.
    // KubeMQ maps the '/' path separators to '.', so:
    //   MQTT publish topic   events/demo/x   ('events/' prefix consumed, 'demo/x' -> 'demo.x')
    //   KubeMQ channel       demo.x
    // The subscriber uses a single-level '+' wildcard (events/demo/+) which maps to the
    // KubeMQ channel filter demo.* and matches the published events/demo/x topic.
    let sub_topic = "events/demo/+";
    let pub_topic = "events/demo/x";

    // -------------------------------------------------------------------------
    // Subscriber connection
    // Use a unique client ID to avoid session conflicts across runs.
    // -------------------------------------------------------------------------
    let sub_id = format!("mqtt-rust-pubsub-sub-{}", &Uuid::new_v4().to_string()[..8]);
    println!("[2] Subscriber client ID: {sub_id}");
    let mut sub_opts = MqttOptions::new(&sub_id, &host, port);
    sub_opts.set_keep_alive(Duration::from_secs(30));
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 10);

    // -------------------------------------------------------------------------
    // Publisher connection
    // -------------------------------------------------------------------------
    let pub_id = format!("mqtt-rust-pubsub-pub-{}", &Uuid::new_v4().to_string()[..8]);
    println!("[3] Publisher client ID: {pub_id}");
    let mut pub_opts = MqttOptions::new(&pub_id, &host, port);
    pub_opts.set_keep_alive(Duration::from_secs(30));
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_opts, 10);

    // Drive the publisher's event loop in the background (needed for PUBACK processing).
    tokio::spawn(async move {
        loop {
            match pub_eventloop.poll().await {
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });

    // -------------------------------------------------------------------------
    // Set up subscriber: subscribe, then signal readiness via a oneshot channel.
    // -------------------------------------------------------------------------
    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let (msg_tx, msg_rx) = oneshot::channel::<(String, String, Vec<(String, String)>)>();

    tokio::spawn(async move {
        let mut ready_tx_opt = Some(ready_tx);
        loop {
            match sub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    if let Some(tx) = ready_tx_opt.take() {
                        let _ = tx.send(());
                    }
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let recv_topic = String::from_utf8_lossy(&p.topic).to_string();
                    let payload = String::from_utf8_lossy(&p.payload).to_string();
                    let user_props = p
                        .properties
                        .map(|pr| pr.user_properties)
                        .unwrap_or_default();
                    let _ = msg_tx.send((recv_topic, payload, user_props));
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Subscriber event loop error: {e}");
                    return;
                }
            }
        }
    });

    // Subscribe to the single-level wildcard filter at QoS 1.
    println!("[4] Subscribing to '{sub_topic}' at QoS 1 ...");
    sub_client.subscribe(sub_topic, QoS::AtLeastOnce).await?;

    // Wait until the broker has confirmed the subscription (SUBACK received).
    timeout(Duration::from_secs(5), ready_rx).await??;
    println!("    SUBACK received — subscription active");

    // -------------------------------------------------------------------------
    // Publish from the separate publisher connection.
    // -------------------------------------------------------------------------
    // The MQTT 5.0 User Property 'k1=v1' is forwarded by the KubeMQ
    // connector as a KubeMQ Tag and echoed back to the subscriber in the same message.
    let payload = b"hello";
    let pub_props = PublishProperties {
        user_properties: vec![("k1".to_string(), "v1".to_string())],
        ..Default::default()
    };

    println!("[5] Publishing to '{pub_topic}' — payload='hello', user-prop k1=v1 ...");
    pub_client
        .publish_with_properties(
            pub_topic,
            QoS::AtLeastOnce,
            false,
            payload.as_ref(),
            pub_props,
        )
        .await?;
    println!("    Publish sent (PUBACK expected 0x00)");

    // -------------------------------------------------------------------------
    // Wait for the message to arrive on the subscriber.
    // -------------------------------------------------------------------------
    let (recv_topic, recv_payload, user_props) = timeout(Duration::from_secs(10), msg_rx).await??;

    println!("[6] Received on '{recv_topic}': {recv_payload}");
    for (k, v) in &user_props {
        println!("    v5 user-prop (KubeMQ Tag): {k}={v}");
    }
    println!("[7] EVENT_RECEIVED — round-trip successful");

    println!("[8] Done.");
    Ok(())
}

// Expected output:
// [1] Broker URL: tcp://localhost:1883
// [2] Subscriber client ID: mqtt-rust-pubsub-sub-<random>
// [3] Publisher client ID: mqtt-rust-pubsub-pub-<random>
// [4] Subscribing to 'events/demo/+' at QoS 1 ...
//     SUBACK received — subscription active
// [5] Publishing to 'events/demo/x' — payload='hello', user-prop k1=v1 ...
//     Publish sent (PUBACK expected 0x00)
// [6] Received on 'events/demo/x': hello
//     v5 user-prop (KubeMQ Tag): k1=v1
// [7] EVENT_RECEIVED — round-trip successful
// [8] Done.
