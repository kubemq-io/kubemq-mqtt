//! Example: rpc/command-round-trip
//!
//! Demonstrates MQTT 5.0 command RPC round-trip over the KubeMQ MQTT connector.
//!
//! Wire contract:
//!   MQTT version     : 5.0  (v3.1.1 publishes to commands/ are silently dropped)
//!   command topic    : commands/demo/cmd  (KubeMQ channel demo.cmd; '/' → '.')
//!   reply topic      : $reply/<clientID>/inbox  (mochi-local, own namespace only)
//!   QoS              : 1
//!   v5 properties    : ResponseTopic = $reply/<clientID>/inbox
//!                      CorrelationData = <request-id bytes>
//!   response body    : NONE (command responses have NO body)
//!   response props   : kubemq-executed ("true"/"false") + optional kubemq-error
//!                      CorrelationData echoed
//!
//! Flow:
//!   1. Connect to the broker with MQTT 5.0.
//!   2. Subscribe to own $reply/<clientID>/inbox BEFORE publishing.
//!   3. Publish to commands/demo/cmd with ResponseTopic + CorrelationData.
//!   4. PUBACK arrives IMMEDIATELY (NOT after command execution) — implement own timeout.
//!   5. Response arrives on $reply/<clientID>/inbox with kubemq-executed user-prop.
//!      No body. CorrelationData echoed.
//!
//! NOTE: MQTT clients CANNOT be command responders (SUBACK 0x83).
//!       A gRPC-side responder must be running on channel demo.cmd:
//!         go run .work/test-harness/responder/ -channel demo.cmd -type command
//!
//! Run: cargo run -p command-round-trip

use bytes::Bytes;
use rumqttc::v5::mqttbytes::v5::{Packet, PublishProperties};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use std::env;
use tokio::sync::oneshot;
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    let (host, port) = parse_host_port(&url);

    // Unique client ID so the $reply namespace is unambiguous per run.
    let client_id = format!("mqtt-rust-cmd-{}", &Uuid::new_v4().to_string()[..8]);
    // Reply topic MUST be in the client's own $reply/<clientID>/... namespace.
    // The broker enforces this: a ResponseTopic in a foreign namespace → PUBACK 0x83.
    let reply_topic = format!("$reply/{}/inbox", client_id);

    // commands/demo/cmd → KubeMQ channel demo.cmd (topic '/' becomes '.' in the channel name).
    let command_topic = "commands/demo/cmd";

    println!(
        "Step 1: Connecting to broker {}:{} as {}",
        host, port, client_id
    );

    let mut mqttoptions = MqttOptions::new(&client_id, &host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // Correlation ID for matching the response.
    let correlation_id = Uuid::new_v4().to_string();
    let corr_bytes = Bytes::from(correlation_id.clone().into_bytes());

    // Channel to deliver the command response (executed flag + optional error message).
    let (tx, rx) = oneshot::channel::<(bool, Option<String>)>();

    let reply_topic_clone = reply_topic.clone();
    let corr_check = corr_bytes.clone();
    let ev_handle = tokio::spawn(async move {
        let mut tx_opt = Some(tx);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let topic = String::from_utf8_lossy(&p.topic).to_string();
                    if topic == reply_topic_clone {
                        // Verify CorrelationData is echoed — use it to match concurrent in-flight requests.
                        let cd = p
                            .properties
                            .as_ref()
                            .and_then(|props| props.correlation_data.clone());
                        if cd.as_deref() == Some(corr_check.as_ref()) {
                            // Command response: NO body.
                            // User-properties carry kubemq-executed + optional kubemq-error.
                            let executed = p
                                .properties
                                .as_ref()
                                .and_then(|props| {
                                    props
                                        .user_properties
                                        .iter()
                                        .find(|(k, _)| k == "kubemq-executed")
                                        .map(|(_, v)| v == "true")
                                })
                                .unwrap_or(false);
                            let error_msg = p.properties.as_ref().and_then(|props| {
                                props
                                    .user_properties
                                    .iter()
                                    .find(|(k, _)| k == "kubemq-error")
                                    .map(|(_, v)| v.clone())
                            });
                            // Log all user-properties for visibility.
                            if let Some(props) = p.properties.as_ref() {
                                if !props.user_properties.is_empty() {
                                    println!("  all user-properties:");
                                    for (k, v) in &props.user_properties {
                                        println!("    {} = {}", k, v);
                                    }
                                }
                            }
                            if let Some(tx) = tx_opt.take() {
                                let _ = tx.send((executed, error_msg));
                                return;
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Event loop error: {e}");
                    return;
                }
            }
        }
    });

    // Step 2: Subscribe to own $reply topic BEFORE publishing.
    // Subscribing after the publish risks losing the response.
    println!("Step 2: Subscribing to reply topic {:?}", reply_topic);
    client.subscribe(&reply_topic, QoS::AtLeastOnce).await?;

    // Small delay to ensure the SUBACK has been processed before publishing.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Step 3: Publish to commands/demo/cmd with ResponseTopic + CorrelationData.
    // ResponseTopic tells the broker where to deliver the response.
    // CorrelationData is echoed verbatim on the response.
    let cmd_payload =
        r#"{"action":"restart","target":"service-a","requestedBy":"mqtt-rust-client"}"#;
    println!(
        "Step 3: Publishing command to {:?}\n  payload: {}\n  correlationData: {}",
        command_topic, cmd_payload, correlation_id
    );

    let props = PublishProperties {
        response_topic: Some(reply_topic.clone()),
        correlation_data: Some(corr_bytes),
        ..Default::default()
    };

    client
        .publish_with_properties(
            command_topic,
            QoS::AtLeastOnce,
            false,
            cmd_payload.as_bytes(),
            props,
        )
        .await?;
    // PUBACK arrives IMMEDIATELY — the broker does NOT wait for the gRPC
    // responder to execute the command before acknowledging the publish.
    // The actual response comes later as a separate PUBLISH to $reply/...
    println!("  PUBACK received immediately (execution outcome pending on $reply/...)");

    // Step 4: Wait for the command response on our reply topic.
    // Command response semantics (differs from query):
    //   - Payload: empty (NO body)
    //   - User-Property kubemq-executed: "true" if the command succeeded, "false" otherwise
    //   - User-Property kubemq-error: present only when executed=false
    //   - CorrelationData: echoed verbatim from the original publish
    println!(
        "Step 4: Waiting for command response on {:?} ...",
        reply_topic
    );
    let (executed, error_msg) = tokio::time::timeout(Duration::from_secs(30), rx)
        .await
        .map_err(|_| {
            "Timed out waiting for command response — is the gRPC test-responder running on channel demo.cmd?\n  Start it with: go run .work/test-harness/responder/ -channel demo.cmd -type command"
        })?
        .expect("Response channel closed unexpectedly");

    println!("Step 5: Command response received:");
    println!("  payload (should be empty): \"\"");
    println!("  kubemq-executed:           {}", executed);
    if let Some(ref err) = error_msg {
        println!("  kubemq-error:              {}", err);
    }
    println!(
        "  correlationData echoed:    {} (match=true)",
        correlation_id
    );

    if executed {
        println!("Command executed successfully.");
    } else {
        eprintln!("Command NOT executed. Error: {:?}", error_msg);
    }

    println!("COMMAND_RESPONSE_RECEIVED");

    // Give the event loop a moment to process any remaining packets, then shut down.
    tokio::time::timeout(Duration::from_secs(2), ev_handle)
        .await
        .ok();

    Ok(())
}

// Expected output (requires gRPC responder on channel demo.cmd type=command):
//
// Step 1: Connecting to broker 127.0.0.1:1883 as mqtt-rust-cmd-a1b2c3d4
// Step 2: Subscribing to reply topic "$reply/mqtt-rust-cmd-a1b2c3d4/inbox"
// Step 3: Publishing command to "commands/demo/cmd"
//   payload: {"action":"restart","target":"service-a","requestedBy":"mqtt-rust-client"}
//   correlationData: <uuid>
//   PUBACK received immediately (execution outcome pending on $reply/...)
// Step 4: Waiting for command response on "$reply/mqtt-rust-cmd-a1b2c3d4/inbox" ...
//   all user-properties:
//     kubemq-executed = true
// Step 5: Command response received:
//   payload (should be empty): ""
//   kubemq-executed:           true
//   correlationData echoed:    <uuid> (match=true)
// Command executed successfully.
// COMMAND_RESPONSE_RECEIVED
