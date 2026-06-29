//! Example: rpc/query-round-trip
//!
//! Demonstrates MQTT 5.0 query RPC round-trip over the KubeMQ MQTT connector.
//!
//! Wire contract:
//!   MQTT version     : 5.0  (v3.1.1 publishes to queries/ are silently dropped)
//!   query topic      : queries/demo/rpc  -> KubeMQ channel demo.rpc
//!   reply topic      : $reply/<clientID>/inbox  (mochi-local, own namespace only)
//!   QoS              : 1
//!   v5 properties    : ResponseTopic = $reply/<clientID>/inbox
//!                      CorrelationData = <request-id bytes>
//!   response body    : present (query responses include body)
//!   response props   : kubemq-metadata + tags as user-properties; CorrelationData echoed
//!
//! Flow:
//!   1. Subscribe to own $reply/<clientID>/inbox BEFORE publishing.
//!   2. Publish to queries/demo/rpc with ResponseTopic + CorrelationData.
//!   3. PUBACK arrives IMMEDIATELY (NOT after the response) — implement own timeout.
//!   4. Response arrives on $reply/<clientID>/inbox with CorrelationData echoed.
//!
//! Reason codes to handle:
//!   0x83 (SUBACK) — ImplementationSpecific: $reply topic not in own namespace
//!   0xA2 (SUBACK) — WildcardSubscriptionsNotSupported: only for non-events topics
//!   0x83 (PUBACK) — ImplementationSpecificError: ResponseTopic not in own namespace
//!   0x97 (PUBACK) — QuotaExceeded: RpcMaxPending quota exceeded
//!
//! NOTE: MQTT clients CANNOT be query responders — a gRPC responder must be running.
//!       Start it with:
//!         go run .work/test-harness/responder/main.go -channel demo.rpc -type query
//!
//! Run: cargo run -p query-round-trip

use bytes::Bytes;
use rumqttc::v5::mqttbytes::v5::{Packet, PubAckReason, PublishProperties, SubscribeReasonCode};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
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
    // Strip any path component (e.g. ws://host:8083/mqtt).
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    let mut parts = host_port.splitn(2, ':');
    let host = parts.next().unwrap_or("localhost").to_string();
    let port: u16 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(1883);
    (host, port)
}

/// Fully parsed query response delivered by the event loop to the main task.
struct QueryResponse {
    topic: String,
    body: Vec<u8>,
    correlation_data: Option<Bytes>,
    user_properties: Vec<(String, String)>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = broker_url();
    let (host, port) = parse_host_port(&url);

    // Unique client ID guarantees the $reply/<clientID>/... namespace is unambiguous.
    let client_id = format!("mqtt-rust-query-{}", &Uuid::new_v4().to_string()[..8]);

    // RULE: reply topic MUST be in own $reply/<clientID>/... namespace.
    // Using a foreign namespace returns SUBACK/PUBACK 0x83 (ImplementationSpecific).
    let reply_topic = format!("$reply/{}/inbox", client_id);

    // queries/demo/rpc -> KubeMQ channel demo.rpc
    // (prefix 'queries/' is consumed by the connector; '/' separators become '.' in the channel name).
    let query_topic = "queries/demo/rpc";

    // Correlation ID for matching the response — echoed verbatim by the broker.
    let correlation_id = Uuid::new_v4().to_string();
    let corr_bytes = Bytes::from(correlation_id.clone().into_bytes());

    println!("=== MQTT 5.0 Query Round-Trip ===");
    println!("[1] Broker URL  : {url}");
    println!("[1] Client ID   : {client_id}");
    println!("[1] Query topic : {query_topic} -> KubeMQ channel demo.rpc");
    println!("[1] Reply topic : {reply_topic}");
    println!("[1] Correlation : {correlation_id}");

    let mut mqttoptions = MqttOptions::new(&client_id, &host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    println!("[1] Connected to {host}:{port} (MQTT 5.0)");

    // One-shot channel delivers the fully parsed query response to the main task.
    let (tx, rx) = oneshot::channel::<QueryResponse>();

    let reply_topic_clone = reply_topic.clone();
    let corr_check = corr_bytes.clone();

    // Spawn the event loop driver.
    // Handles SUBACK, PUBACK, and the incoming query response.
    let ev_handle = tokio::spawn(async move {
        let mut tx_opt = Some(tx);
        loop {
            match eventloop.poll().await {
                // -----------------------------------------------------------------
                // SUBACK: verify the reply-inbox subscription was accepted.
                // 0x83 ImplementationSpecific -> $reply topic not in own namespace.
                // 0xA2 WildcardSubscriptionsNotSupported -> wildcard on non-events.
                // -----------------------------------------------------------------
                Ok(Event::Incoming(Packet::SubAck(sa))) => {
                    for (i, rc) in sa.return_codes.iter().enumerate() {
                        match rc {
                            SubscribeReasonCode::Success(qos) => {
                                println!(
                                    "    SUBACK[{i}] = 0x{:02X} (granted QoS {:?})",
                                    *qos as u8, qos
                                );
                            }
                            SubscribeReasonCode::ImplementationSpecific => {
                                eprintln!(
                                    "ERROR: SUBACK[{i}] = 0x83 — reply topic rejected; \
                                     must be $reply/<own-clientID>/..."
                                );
                                return;
                            }
                            SubscribeReasonCode::WildcardSubscriptionsNotSupported => {
                                eprintln!(
                                    "ERROR: SUBACK[{i}] = 0xA2 — wildcard not allowed on this topic"
                                );
                                return;
                            }
                            other => {
                                eprintln!("ERROR: SUBACK[{i}] unexpected reason code {other:?}");
                                return;
                            }
                        }
                    }
                }
                // -----------------------------------------------------------------
                // PUBACK: check for quota / namespace errors.
                // PUBACK is IMMEDIATE — it arrives before the responder replies.
                // 0x83 ImplementationSpecificError -> ResponseTopic not in own namespace.
                // 0x97 QuotaExceeded -> RpcMaxPending exceeded.
                // -----------------------------------------------------------------
                Ok(Event::Incoming(Packet::PubAck(pa))) => match pa.reason {
                    PubAckReason::Success => {
                        println!(
                            "    PUBACK = 0x00 (success — PUBACK is IMMEDIATE, \
                                 response arrives separately)"
                        );
                    }
                    PubAckReason::ImplementationSpecificError => {
                        eprintln!(
                            "ERROR: PUBACK 0x83 — ResponseTopic not in own \
                                 $reply/<clientID>/... namespace"
                        );
                        return;
                    }
                    PubAckReason::QuotaExceeded => {
                        eprintln!(
                            "ERROR: PUBACK 0x97 — RpcMaxPending quota exceeded; \
                                 retry later or increase RpcMaxPending in broker config"
                        );
                        return;
                    }
                    other => {
                        eprintln!("WARN: PUBACK unexpected reason {other:?}");
                    }
                },
                // -----------------------------------------------------------------
                // PUBLISH: incoming query response on the reply inbox.
                // Filter by topic and CorrelationData before forwarding.
                // -----------------------------------------------------------------
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let topic = String::from_utf8_lossy(&p.topic).to_string();
                    if topic != reply_topic_clone {
                        continue;
                    }
                    let cd = p
                        .properties
                        .as_ref()
                        .and_then(|pr| pr.correlation_data.clone());
                    // Discard stale responses not matching our CorrelationData.
                    if cd.as_deref() != Some(corr_check.as_ref()) {
                        continue;
                    }
                    let user_properties = p
                        .properties
                        .as_ref()
                        .map(|pr| pr.user_properties.clone())
                        .unwrap_or_default();
                    let resp = QueryResponse {
                        topic,
                        body: p.payload.to_vec(),
                        correlation_data: cd,
                        user_properties,
                    };
                    if let Some(sender) = tx_opt.take() {
                        let _ = sender.send(resp);
                        return;
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

    // -------------------------------------------------------------------------
    // Step 2: SUBSCRIBE to the reply inbox BEFORE publishing.
    //
    // The broker enforces that $reply topics are in the own client namespace.
    // A subscribe outside the own namespace returns SUBACK 0x83.
    // -------------------------------------------------------------------------
    println!("[2] Subscribing to reply topic {reply_topic:?} (QoS 1) ...");
    client.subscribe(&reply_topic, QoS::AtLeastOnce).await?;
    println!("    Subscribe sent; waiting for SUBACK ...");

    // Allow SUBACK to arrive before publishing.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // -------------------------------------------------------------------------
    // Step 3: PUBLISH the query with ResponseTopic + CorrelationData.
    //
    // IMPORTANT: PUBACK arrives IMMEDIATELY after the broker receives the publish
    // — NOT after the gRPC responder sends its reply.  Wait for the response
    // separately (Step 4 below).
    // -------------------------------------------------------------------------
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let query_payload = format!(r#"{{"action":"echo","data":"hello from rust-mqtt","ts":{ts}}}"#);

    let props = PublishProperties {
        response_topic: Some(reply_topic.clone()),
        correlation_data: Some(corr_bytes),
        ..Default::default()
    };

    println!("[3] Publishing query to {query_topic:?} ...");
    println!("    payload         : {query_payload}");
    println!("    ResponseTopic   : {reply_topic}");
    println!("    CorrelationData : {correlation_id}");

    client
        .publish_with_properties(
            query_topic,
            QoS::AtLeastOnce,
            false,
            Bytes::from(query_payload.into_bytes()),
            props,
        )
        .await?;
    println!("    Publish sent (awaiting PUBACK; PUBACK is IMMEDIATE — not a sign the response is ready)");
    println!("    Waiting for gRPC responder on channel demo.rpc to reply ...");

    // -------------------------------------------------------------------------
    // Step 4: Wait for the query response on the reply inbox (timeout = 30 s).
    //
    // The response carries:
    //   - Payload         : the responder's reply body (e.g. "pong:<original-body>")
    //   - User-Properties : kubemq-metadata + any tags set by the responder
    //   - CorrelationData : echoed verbatim from the publish
    // -------------------------------------------------------------------------
    let resp = tokio::time::timeout(Duration::from_secs(30), rx)
        .await
        .map_err(|_| {
            "Timed out waiting for query response — is the gRPC test-responder running?\n\
             Start it with: go run .work/test-harness/responder/main.go -channel demo.rpc -type query"
        })?
        .map_err(|_| "Response channel closed unexpectedly")?;

    println!("\n[4] Response received on {:?}", resp.topic);
    println!(
        "    body            : {}",
        String::from_utf8_lossy(&resp.body)
    );
    if let Some(cd) = &resp.correlation_data {
        let echo = String::from_utf8_lossy(cd);
        let matched = echo == correlation_id.as_str();
        println!("    correlation     : {echo}");
        println!("    correlation match : {matched}");
        if !matched {
            eprintln!("WARN: correlation mismatch — response may not belong to this request");
        }
    }
    if !resp.user_properties.is_empty() {
        println!("    user-properties (kubemq-metadata / tags):");
        for (k, v) in &resp.user_properties {
            println!("      {k} = {v}");
        }
    }

    println!("\nQUERY_RESPONSE_RECEIVED");

    // -------------------------------------------------------------------------
    // Step 5: Graceful shutdown.
    // -------------------------------------------------------------------------
    println!("[5] Disconnecting ...");
    tokio::time::timeout(Duration::from_secs(2), ev_handle)
        .await
        .ok();
    println!("[5] Done.");
    println!("=== Query round-trip complete ===");

    Ok(())
}

// Expected output (requires a gRPC responder on channel demo.rpc):
//
// === MQTT 5.0 Query Round-Trip ===
// [1] Broker URL  : tcp://localhost:1883
// [1] Client ID   : mqtt-rust-query-<random>
// [1] Query topic : queries/demo/rpc -> KubeMQ channel demo.rpc
// [1] Reply topic : $reply/mqtt-rust-query-<random>/inbox
// [1] Correlation : <uuid>
// [1] Connected to localhost:1883 (MQTT 5.0)
// [2] Subscribing to reply topic "$reply/mqtt-rust-query-<random>/inbox" (QoS 1) ...
//     Subscribe sent; waiting for SUBACK ...
//     SUBACK[0] = 0x01 (granted QoS AtLeastOnce)
// [3] Publishing query to "queries/demo/rpc" ...
//     payload         : {"action":"echo","data":"hello from rust-mqtt","ts":...}
//     ResponseTopic   : $reply/mqtt-rust-query-<random>/inbox
//     CorrelationData : <uuid>
//     Publish sent (awaiting PUBACK; PUBACK is IMMEDIATE — not a sign the response is ready)
//     Waiting for gRPC responder on channel demo.rpc to reply ...
//     PUBACK = 0x00 (success — PUBACK is IMMEDIATE, response arrives separately)
//
// [4] Response received on "$reply/mqtt-rust-query-<random>/inbox"
//     body            : pong:{"action":"echo","data":"hello from rust-mqtt","ts":...}
//     correlation     : <uuid>
//     correlation match : true
//     user-properties (kubemq-metadata / tags):
//       kubemq-metadata = responder/query
//       responder = mqtt-test
//       rpc-type = query
//
// QUERY_RESPONSE_RECEIVED
// [5] Disconnecting ...
// [5] Done.
// === Query round-trip complete ===
