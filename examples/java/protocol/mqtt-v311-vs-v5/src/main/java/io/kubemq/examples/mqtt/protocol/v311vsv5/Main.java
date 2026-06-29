/**
 * Example: protocol/mqtt-v311-vs-v5
 *
 * Side-by-side comparison of MQTT 3.1.1 (level 4) and MQTT 5.0 (level 5)
 * against the KubeMQ MQTT connector.
 *
 * What is demonstrated:
 *
 *  SECTION A — MQTT 3.1.1 (v3 Paho client)
 *    A1. Events pub/sub on events/demo/v3: works in v3.1.1 (fire-and-forget).
 *    A2. RPC attempt:    publish to commands/demo/x — silently dropped
 *        (no PUBACK error, but the command never reaches the responder).
 *        v3.1.1 has no ResponseTopic/CorrelationData => connector drops it.
 *
 *  SECTION B — MQTT 5.0 (v5 Paho async client)
 *    B1. Events pub/sub on events/demo/v3 with User Properties -> KubeMQ Tags (v5-only).
 *    B2. Queues shared subscription: $share/g1/queues/demo/proto-q (v5-only).
 *        Plain queues/... subscribe -> SUBACK 0x83 (rejected).
 *        QoS 0 shared subscribe    -> SUBACK 0x83 (rejected).
 *    B3. RPC command round-trip via commands/demo/cmd:
 *        - Subscribe reply inbox $reply/<clientId>/rq1 BEFORE publishing.
 *        - Publish with ResponseTopic=$reply/<clientId>/rq1 + CorrelationData.
 *        - PUBACK arrives IMMEDIATELY on receipt (NOT after response).
 *        - Response arrives on $reply/... with CorrelationData echoed.
 *        - Requires gRPC-side responder on channel demo.cmd type command (start via
 *          .work/test-harness/responder/).
 *
 * Wire contract notes:
 *   - "events/" prefix -> KubeMQ Events pattern (fire-and-forget).
 *   - "commands/" prefix -> KubeMQ Commands (RPC, v5 only).
 *   - "queues/" prefix -> KubeMQ Queues (produce).
 *   - "$share/<group>/queues/<ch>" -> KubeMQ Queues (consume, v5 only, QoS>=1).
 *   - MQTT "/" -> KubeMQ "." in channel name (e.g. demo/cmd -> demo.cmd).
 *   - User Properties map to KubeMQ Tags (v5 only; max 32 props / 4096 B total).
 *   - Retain is NOT supported: silently dropped, PUBACK 0x00 returned.
 *   - $share group name is audit/metrics only — ALL groups share ONE pool.
 *   - MQTT 3.1 (level 3) is rejected at CONNACK by KubeMQ.
 *
 * KUBEMQ_MQTT_URL (default tcp://localhost:1883) selects broker and transport.
 *
 * Run: mvn compile exec:java
 */
package io.kubemq.examples.mqtt.protocol.v311vsv5;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Main {

    // -------------------------------------------------------------------------
    // Broker URL — read from KUBEMQ_MQTT_URL; default tcp://localhost:1883.
    // Scheme used as-is by both Paho v3 and v5 clients.
    // -------------------------------------------------------------------------
    static String brokerUrl() {
        String url = System.getenv("KUBEMQ_MQTT_URL");
        return (url != null && !url.isEmpty()) ? url : "tcp://localhost:1883";
    }

    public static void main(String[] args) throws Exception {
        String broker = brokerUrl();

        System.out.println("=== protocol/mqtt-v311-vs-v5 ===");
        System.out.println("Broker: " + broker);
        System.out.println();

        // =====================================================================
        // SECTION A — MQTT 3.1.1
        // =====================================================================
        runV311Section(broker);

        System.out.println();

        // =====================================================================
        // SECTION B — MQTT 5.0
        // =====================================================================
        runV5Section(broker);

        System.out.println();
        System.out.println("=== Feature comparison summary ===");
        System.out.println("""
                Feature                          v3.1.1        v5.0
                -------                          ------        ----
                Events pub/sub                   YES           YES
                User Properties (KubeMQ Tags)    NO            YES (32 props / 4096 B cap)
                RPC (commands/queries)           DROPPED       YES (ResponseTopic + CorrelationData)
                Shared queue consume             NO            YES ($share/<g>/queues/<ch>, QoS>=1)
                Detailed SUBACK/PUBACK codes     NO (0/0x80)   YES (0x83 / 0x97 / 0xA2 etc.)
                Will-Retain                      YES           Rejected (0x9A) - broker-side
                MQTT 3.1 (level 3)               N/A           N/A (rejected by KubeMQ)
                """);
    }

    // =========================================================================
    // SECTION A: MQTT 3.1.1
    // =========================================================================

    private static void runV311Section(String broker) throws Exception {
        System.out.println("--- SECTION A: MQTT 3.1.1 (org.eclipse.paho.client.mqttv3) ---");
        System.out.println();

        // A1 — Events pub/sub
        System.out.println("[A1] Events pub/sub over MQTT 3.1.1");
        runV311EventsPubSub(broker);

        System.out.println();

        // A2 — RPC attempt (silently dropped)
        System.out.println("[A2] RPC attempt over MQTT 3.1.1 (should be silently dropped)");
        runV311RpcAttempt(broker);
    }

    /**
     * A1: Demonstrate basic events pub/sub using the Paho v3 client (MQTT 3.1.1).
     * KubeMQ channel: demo.v3 (events/demo/v3 -> demo.v3).
     */
    private static void runV311EventsPubSub(String broker) throws Exception {
        String topic = "events/demo/v3";
        String payload = "hello-from-v311";

        System.out.println("    Topic   : " + topic);
        System.out.println("    Channel : demo.v3  (MQTT '/' -> KubeMQ '.')");

        CountDownLatch received = new CountDownLatch(1);

        // Subscribe client
        String subId = "v311-sub-" + UUID.randomUUID().toString().substring(0, 6);
        MqttClient subscriber = new MqttClient(broker, subId, new MemoryPersistence());

        subscriber.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String t, MqttMessage msg) {
                System.out.println("    [A1] Message received:");
                System.out.println("         topic   = " + t);
                System.out.println("         payload = " + new String(msg.getPayload(), StandardCharsets.UTF_8));
                System.out.println("         QoS     = " + msg.getQos());
                // NOTE: MQTT 3.1.1 has NO User Properties — tags are not available here.
                System.out.println("         User Properties: N/A (MQTT 3.1.1 has no user properties)");
                received.countDown();
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) { /* publish-only */ }

            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("    [A1] subscriber connectionLost: " + cause.getMessage());
            }
        });

        MqttConnectOptions subOpts = v311Options();
        System.out.println("    Connecting v3.1.1 subscriber ...");
        subscriber.connect(subOpts);
        System.out.println("    v3.1.1 subscriber connected (protocol level 4)");

        subscriber.subscribe(topic, 1);
        System.out.println("    Subscribed to '" + topic + "' at QoS 1");

        // Publisher client
        String pubId = "v311-pub-" + UUID.randomUUID().toString().substring(0, 6);
        MqttClient publisher = new MqttClient(broker, pubId, new MemoryPersistence());

        publisher.setCallback(new MqttCallback() {
            @Override public void messageArrived(String t, MqttMessage m) { }
            @Override public void deliveryComplete(IMqttDeliveryToken t) { }
            @Override public void connectionLost(Throwable cause) {
                System.err.println("    [A1] publisher connectionLost: " + cause.getMessage());
            }
        });

        System.out.println("    Connecting v3.1.1 publisher ...");
        publisher.connect(v311Options());
        System.out.println("    v3.1.1 publisher connected");

        MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(1);
        // NOTE: Do NOT set msg.setRetained(true) — retain is silently dropped by KubeMQ
        //       (PUBACK 0x00 returned, message not delivered).
        System.out.println("    Publishing '" + payload + "' to '" + topic + "' QoS=1 ...");
        publisher.publish(topic, msg);
        System.out.println("    Published OK (no User Properties — v3.1.1 limitation)");

        boolean ok = received.await(8, TimeUnit.SECONDS);
        if (!ok) {
            System.err.println("    [A1] WARN: timed out waiting for v3.1.1 event delivery");
        } else {
            System.out.println("    [A1] Events pub/sub PASSED (v3.1.1)");
        }

        disconnectV3(publisher, "v311-pub");
        disconnectV3(subscriber, "v311-sub");
    }

    /**
     * A2: Attempt an RPC publish over MQTT 3.1.1.
     * Publishing to commands/ with v3.1.1 is SILENTLY DROPPED by the connector
     * because there is no ResponseTopic or CorrelationData in the v3.1.1 protocol.
     * PUBACK arrives with reason code 0 (success), but no command is executed.
     */
    private static void runV311RpcAttempt(String broker) throws Exception {
        String cmdTopic = "commands/demo/x";
        // There is no $reply mechanism in v3.1.1 — we subscribe to a plain topic
        // just to illustrate the attempt; no response will ever arrive.
        String fakeReplyTopic = "reply/v311/attempt";

        System.out.println("    Command topic : " + cmdTopic);
        System.out.println("    NOTE: v3.1.1 has no ResponseTopic/CorrelationData properties.");
        System.out.println("          The connector SILENTLY DROPS commands/queries published over v3.1.1.");

        String clientId = "v311-rpc-" + UUID.randomUUID().toString().substring(0, 6);
        MqttClient client = new MqttClient(broker, clientId, new MemoryPersistence());
        CountDownLatch replyLatch = new CountDownLatch(1);

        client.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String t, MqttMessage m) {
                // This will never be called — the connector silently drops the publish.
                System.out.println("    [A2] Unexpected reply received on: " + t);
                replyLatch.countDown();
            }
            @Override public void deliveryComplete(IMqttDeliveryToken t) { }
            @Override public void connectionLost(Throwable cause) {
                System.err.println("    [A2] connectionLost: " + cause.getMessage());
            }
        });

        client.connect(v311Options());
        // Subscribe to a fake reply topic — nothing will arrive here over v3.1.1
        client.subscribe(fakeReplyTopic, 1);

        MqttMessage cmdMsg = new MqttMessage("run-action".getBytes(StandardCharsets.UTF_8));
        cmdMsg.setQos(1);
        System.out.println("    Publishing command to '" + cmdTopic + "' via v3.1.1 ...");
        client.publish(cmdTopic, cmdMsg);
        System.out.println("    PUBACK received (code 0x00 = success at wire level)");
        System.out.println("    Waiting 3 s for a reply that will NOT arrive ...");

        boolean replyArrived = replyLatch.await(3, TimeUnit.SECONDS);
        if (replyArrived) {
            System.out.println("    [A2] Unexpected: reply did arrive (check broker config)");
        } else {
            System.out.println("    [A2] No reply received (EXPECTED — v3.1.1 RPC is silently dropped)");
            System.out.println("         To use RPC, upgrade the client to MQTT 5.0 (see Section B).");
        }

        disconnectV3(client, "v311-rpc");
    }

    // =========================================================================
    // SECTION B: MQTT 5.0
    // =========================================================================

    private static void runV5Section(String broker) throws Exception {
        System.out.println("--- SECTION B: MQTT 5.0 (org.eclipse.paho.mqttv5.client) ---");
        System.out.println();

        // B1 — Events with User Properties
        System.out.println("[B1] Events pub/sub with User Properties (KubeMQ Tags) over MQTT 5.0");
        runV5EventsWithUserProps(broker);

        System.out.println();

        // B2 — Queues shared subscription
        System.out.println("[B2] Queues shared subscription over MQTT 5.0");
        runV5SharedQueueConsume(broker);

        System.out.println();

        // B3 — RPC (commands) via MQTT 5.0
        System.out.println("[B3] RPC command round-trip over MQTT 5.0");
        runV5RpcRoundTrip(broker);
    }

    /**
     * B1: Events pub/sub using MQTT 5.0 with User Properties.
     * User Properties are forwarded by the connector as KubeMQ message Tags.
     * Cap: 32 properties / 4096 bytes total — exceeding causes PUBACK 0x97.
     * v3.1.1 has no user properties, so this is a v5-exclusive feature.
     */
    private static void runV5EventsWithUserProps(String broker) throws Exception {
        String topic = "events/demo/v3";
        String payload = "hello-from-v5";

        System.out.println("    Topic   : " + topic);
        System.out.println("    Channel : demo.v3");
        System.out.println("    User Properties -> KubeMQ Tags (v5 only; cap 32 props / 4096 B)");

        CountDownLatch received = new CountDownLatch(1);

        // Subscriber
        String subId = "v5-events-sub-" + UUID.randomUUID().toString().substring(0, 6);
        MqttAsyncClient subscriber = new MqttAsyncClient(broker, subId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        subscriber.setCallback(new org.eclipse.paho.mqttv5.client.MqttCallback() {
            @Override
            public void messageArrived(String t, org.eclipse.paho.mqttv5.common.MqttMessage msg) {
                System.out.println("    [B1] Message received:");
                System.out.println("         topic   = " + t);
                System.out.println("         payload = " + new String(msg.getPayload(), StandardCharsets.UTF_8));
                System.out.println("         QoS     = " + msg.getQos());
                // User Properties from the publisher are delivered back as User Properties
                // on the PUBLISH to the subscriber — these map to/from KubeMQ Tags.
                MqttProperties props = msg.getProperties();
                if (props != null && props.getUserProperties() != null
                        && !props.getUserProperties().isEmpty()) {
                    System.out.println("         User Properties (KubeMQ Tags):");
                    for (UserProperty up : props.getUserProperties()) {
                        System.out.println("           " + up.getKey() + " = " + up.getValue());
                    }
                } else {
                    System.out.println("         User Properties: (none on delivery)");
                }
                received.countDown();
            }

            @Override
            public void disconnected(MqttDisconnectResponse r) {
                System.err.println("    [B1] subscriber disconnected: " + r.getReturnCode());
            }

            @Override
            public void mqttErrorOccurred(MqttException e) {
                System.err.println("    [B1] subscriber error: " + e.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttToken token) { }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("    v5 subscriber connected to " + serverURI);
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) { }
        });

        subscriber.connect(v5Options()).waitForCompletion(10_000);

        IMqttToken subToken = subscriber.subscribe(topic, 1);
        subToken.waitForCompletion(10_000);
        int subCode = subToken.getGrantedQos() != null ? subToken.getGrantedQos()[0] : -1;
        if (subCode >= 0x80) {
            System.err.printf("    SUBACK rejected: 0x%02X%n", subCode);
            subscriber.disconnect();
            subscriber.close();
            return;
        }
        System.out.println("    Subscribed; SUBACK granted QoS=" + subCode);

        // Publisher
        String pubId = "v5-events-pub-" + UUID.randomUUID().toString().substring(0, 6);
        MqttAsyncClient publisher = new MqttAsyncClient(broker, pubId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());
        publisher.setCallback(minimalV5Callback("v5-events-pub"));
        publisher.connect(v5Options()).waitForCompletion(10_000);

        // Build message with MQTT 5.0 User Properties (-> KubeMQ Tags)
        org.eclipse.paho.mqttv5.common.MqttMessage msg =
                new org.eclipse.paho.mqttv5.common.MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(1);
        // Retain MUST NOT be set — silently dropped (PUBACK 0x00, message not delivered).

        MqttProperties pubProps = new MqttProperties();
        pubProps.setUserProperties(List.of(
                new UserProperty("protocol",  "v5"),
                new UserProperty("source",    "java-example"),
                new UserProperty("variant",   "protocol/mqtt-v311-vs-v5")
        ));
        msg.setProperties(pubProps);

        System.out.println("    Publishing with User Properties: protocol=v5, source=java-example ...");
        IMqttToken pubToken = publisher.publish(topic, msg);
        pubToken.waitForCompletion(10_000);

        int pubCode = pubToken.getReasonCodes() != null ? pubToken.getReasonCodes()[0] : -1;
        if (pubCode != 0x00) {
            System.err.printf("    PUBACK rejected: 0x%02X (0x97=quota exceeded / 0x90=invalid topic)%n", pubCode);
        } else {
            System.out.println("    PUBACK 0x00 (success)");
        }

        boolean ok = received.await(8, TimeUnit.SECONDS);
        if (!ok) {
            System.err.println("    [B1] WARN: timed out waiting for event delivery");
        } else {
            System.out.println("    [B1] Events pub/sub with User Properties PASSED (v5)");
        }

        disconnectV5(publisher, "v5-events-pub");
        disconnectV5(subscriber, "v5-events-sub");
    }

    /**
     * B2: Demonstrate queues shared subscription (MQTT 5.0 only).
     *
     * Producer publishes to:  queues/demo/proto-q
     * Consumers subscribe to: $share/g1/queues/demo/proto-q (QoS 1)
     *
     * Wire contract:
     *   - Plain queues/... subscribe -> SUBACK 0x83 (rejected).
     *   - QoS 0 shared subscribe    -> SUBACK 0x83 (rejected).
     *   - $share group name is audit/metrics ONLY — all groups compete in one pool.
     *     This differs from standard MQTT 5.0 semantics (no per-group message copies).
     *   - Ack-on-PUBACK: Paho auto-sends PUBACK after callback returns.
     *   - No PUBACK within 30 s (QueueAckTimeoutSeconds) -> redelivery.
     *   - Disconnect with unacked messages -> immediate requeue.
     */
    private static void runV5SharedQueueConsume(String broker) throws Exception {
        String producerTopic = "queues/demo/proto-q";
        String consumerTopic = "$share/g1/queues/demo/proto-q";
        int messageCount = 2;

        System.out.println("    Producer topic : " + producerTopic);
        System.out.println("    Consumer topic : " + consumerTopic + " (shared, QoS 1)");
        System.out.println("    NOTE: $share group name is audit-only — ALL groups share ONE pool.");

        CountDownLatch received = new CountDownLatch(messageCount);

        // Consumer 1
        String c1Id = "v5-q-consumer-" + UUID.randomUUID().toString().substring(0, 6);
        MqttAsyncClient consumer1 = new MqttAsyncClient(broker, c1Id,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        consumer1.setCallback(new org.eclipse.paho.mqttv5.client.MqttCallback() {
            @Override
            public void messageArrived(String t, org.eclipse.paho.mqttv5.common.MqttMessage msg) {
                System.out.println("    [B2] consumer-1 received: "
                        + new String(msg.getPayload(), StandardCharsets.UTF_8)
                        + " (Paho auto-acks on PUBACK after this callback)");
                received.countDown();
            }
            @Override public void disconnected(MqttDisconnectResponse r) { }
            @Override public void mqttErrorOccurred(MqttException e) {
                System.err.println("    [B2] consumer-1 error: " + e.getMessage());
            }
            @Override public void deliveryComplete(IMqttToken t) { }
            @Override public void connectComplete(boolean r, String u) {
                System.out.println("    v5 consumer-1 connected");
            }
            @Override public void authPacketArrived(int c, MqttProperties p) { }
        });

        consumer1.connect(v5Options()).waitForCompletion(10_000);

        // Attempt a plain queues/... subscribe — must be rejected (0x83).
        //
        // IMPORTANT: do this on a SEPARATE throwaway client, NOT on the real
        // consumer. A plain `queues/<ch>` subscribe and a `$share/<g>/queues/<ch>`
        // subscribe map to the SAME mochi filter key (`$share/<g>/` is stripped
        // off before the filter is stored). If both are issued on one client, the
        // rejected non-shared filter collides with the granted shared filter and
        // the broker stops delivering queued messages to that client. Always keep
        // the rejection demo on its own short-lived client that disconnects before
        // the real consumer subscribes.
        System.out.println("    Testing plain 'queues/demo/proto-q' subscribe (expect SUBACK 0x83) ...");
        String rejId = "v5-q-reject-" + UUID.randomUUID().toString().substring(0, 6);
        MqttAsyncClient rejectDemo = new MqttAsyncClient(broker, rejId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());
        rejectDemo.setCallback(minimalV5Callback("v5-q-reject"));
        rejectDemo.connect(v5Options()).waitForCompletion(10_000);
        IMqttToken plainSubToken = rejectDemo.subscribe(producerTopic, 1);
        plainSubToken.waitForCompletion(10_000);
        int plainCode = plainSubToken.getGrantedQos() != null ? plainSubToken.getGrantedQos()[0] : -1;
        if (plainCode == 0x83) {
            System.out.printf("    SUBACK 0x83 received for plain queues/... (EXPECTED — use $share prefix)%n");
        } else {
            System.out.printf("    plain queues/... SUBACK code: 0x%02X (expected 0x83)%n", plainCode);
        }
        // Disconnect the throwaway client so its rejected filter cannot interfere
        // with the real consumer's shared subscription below.
        disconnectV5(rejectDemo, "v5-q-reject");

        // Now subscribe correctly with $share prefix at QoS 1 on the real consumer.
        IMqttToken sharedSubToken = consumer1.subscribe(consumerTopic, 1);
        sharedSubToken.waitForCompletion(10_000);
        int sharedCode = sharedSubToken.getGrantedQos() != null ? sharedSubToken.getGrantedQos()[0] : -1;
        if (sharedCode >= 0x80) {
            System.err.printf("    SUBACK rejected for shared subscribe: 0x%02X%n", sharedCode);
            disconnectV5(consumer1, "consumer-1");
            return;
        }
        System.out.println("    $share consumer-1 subscribed; granted QoS=" + sharedCode);

        Thread.sleep(400); // let subscription register

        // Producer
        String prodId = "v5-q-producer-" + UUID.randomUUID().toString().substring(0, 6);
        MqttAsyncClient producer = new MqttAsyncClient(broker, prodId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());
        producer.setCallback(minimalV5Callback("v5-q-producer"));
        producer.connect(v5Options()).waitForCompletion(10_000);

        for (int i = 1; i <= messageCount; i++) {
            String item = "task-" + i;
            org.eclipse.paho.mqttv5.common.MqttMessage qMsg =
                    new org.eclipse.paho.mqttv5.common.MqttMessage(item.getBytes(StandardCharsets.UTF_8));
            qMsg.setQos(1);
            qMsg.setRetained(false); // retain=true -> silently dropped
            IMqttToken t = producer.publish(producerTopic, qMsg);
            t.waitForCompletion(5_000);
            System.out.println("    [B2] produced: " + item);
        }

        boolean allReceived = received.await(15, TimeUnit.SECONDS);
        if (allReceived) {
            System.out.println("    [B2] Shared queue consume PASSED (v5)");
        } else {
            System.err.println("    [B2] WARN: not all messages arrived within timeout");
        }

        disconnectV5(producer, "v5-q-producer");
        disconnectV5(consumer1, "consumer-1");
    }

    /**
     * B3: RPC command round-trip via MQTT 5.0.
     *
     * Protocol flow:
     *  1. Subscribe to OWN reply topic: $reply/<clientId>/rq1   (before publishing)
     *  2. Publish to commands/demo/cmd with:
     *       Properties.ResponseTopic = $reply/<own-clientId>/rq1
     *       Properties.CorrelationData = <bytes>
     *  3. PUBACK arrives IMMEDIATELY on receipt (NOT after response) — use own timeout.
     *  4. gRPC-side responder processes the command and the response arrives on $reply/...
     *  5. Command response: NO body + user-prop kubemq-executed("true"/"false")
     *                                          + optional kubemq-error.
     *     CorrelationData is echoed on the response.
     *
     * Requires: gRPC-side responder running on channel demo.cmd (type command)
     *           (start via .work/test-harness/responder/).
     *           If no responder is running, the reply will time out.
     *
     * Security: ResponseTopic MUST be in own $reply/<clientId>/ namespace.
     *           Foreign namespace -> PUBACK 0x83.
     */
    private static void runV5RpcRoundTrip(String broker) throws Exception {
        String clientId  = "v5-rpc-" + UUID.randomUUID().toString().substring(0, 6);
        String cmdTopic  = "commands/demo/cmd";
        // Reply topic MUST be in own $reply/<clientId>/ namespace
        String replyTopic = "$reply/" + clientId + "/rq1";
        byte[] correlationId = ("corr-" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);

        System.out.println("    Command topic  : " + cmdTopic + "  -> KubeMQ channel demo.cmd");
        System.out.println("    Reply inbox    : " + replyTopic);
        System.out.println("    CorrelationData: " + new String(correlationId, StandardCharsets.UTF_8));
        System.out.println("    NOTE: PUBACK is immediate (on receipt); response arrives separately.");
        System.out.println("          Command response: no body + kubemq-executed user-prop.");
        System.out.println("    Requires gRPC responder on channel demo.cmd (type command).");

        CountDownLatch replyLatch = new CountDownLatch(1);
        AtomicReference<String> executedProp = new AtomicReference<>("<not received>");
        AtomicReference<byte[]> echoedCorr   = new AtomicReference<>(new byte[0]);

        MqttAsyncClient client = new MqttAsyncClient(broker, clientId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        client.setCallback(new org.eclipse.paho.mqttv5.client.MqttCallback() {
            @Override
            public void messageArrived(String t, org.eclipse.paho.mqttv5.common.MqttMessage msg) {
                // This fires for the response on $reply/...
                System.out.println("    [B3] Response received on: " + t);
                String body = new String(msg.getPayload(), StandardCharsets.UTF_8);
                System.out.println("         body    : '" + body + "' (command response has no body)");

                MqttProperties props = msg.getProperties();
                if (props != null && props.getUserProperties() != null) {
                    for (UserProperty up : props.getUserProperties()) {
                        System.out.println("         user-prop: " + up.getKey() + " = " + up.getValue());
                        if ("kubemq-executed".equals(up.getKey())) {
                            executedProp.set(up.getValue());
                        }
                    }
                }

                // Verify CorrelationData echo
                if (props != null && props.getCorrelationData() != null) {
                    echoedCorr.set(props.getCorrelationData());
                    boolean match = Arrays.equals(correlationId, props.getCorrelationData());
                    System.out.println("         CorrelationData echoed: "
                            + new String(props.getCorrelationData(), StandardCharsets.UTF_8)
                            + (match ? " (matches)" : " (MISMATCH — unexpected)"));
                }

                replyLatch.countDown();
            }

            @Override
            public void disconnected(MqttDisconnectResponse r) {
                System.err.println("    [B3] disconnected: " + r.getReturnCode());
            }

            @Override
            public void mqttErrorOccurred(MqttException e) {
                System.err.println("    [B3] MQTT error: " + e.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttToken token) { }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("    v5 RPC client connected to " + serverURI);
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) { }
        });

        client.connect(v5Options()).waitForCompletion(10_000);

        // Step 1: Subscribe to OWN reply inbox BEFORE publishing the command.
        // This is critical — if you publish first, the response might arrive before
        // the subscription is set up and be lost (mochi-local; no persistent delivery).
        System.out.println("    [B3] Step 1: subscribing reply inbox BEFORE publishing command ...");
        IMqttToken replySubToken = client.subscribe(replyTopic, 1);
        replySubToken.waitForCompletion(10_000);
        int replySubCode = replySubToken.getGrantedQos() != null ? replySubToken.getGrantedQos()[0] : -1;
        if (replySubCode >= 0x80) {
            System.err.printf("    Reply inbox subscribe rejected: SUBACK 0x%02X%n", replySubCode);
            disconnectV5(client, "v5-rpc");
            return;
        }
        System.out.println("    Reply inbox subscribed; SUBACK granted QoS=" + replySubCode);

        // Step 2: Publish the command with ResponseTopic and CorrelationData.
        MqttProperties cmdProps = new MqttProperties();
        // ResponseTopic MUST be in own $reply/<clientId>/ namespace.
        // Using a foreign namespace -> PUBACK 0x83 (not authorized).
        cmdProps.setResponseTopic(replyTopic);
        cmdProps.setCorrelationData(correlationId);
        // Optional metadata tags as User Properties
        cmdProps.setUserProperties(List.of(
                new UserProperty("action", "demo-command"),
                new UserProperty("source", "mqtt-v311-vs-v5-example")
        ));

        org.eclipse.paho.mqttv5.common.MqttMessage cmdMsg =
                new org.eclipse.paho.mqttv5.common.MqttMessage("run-demo".getBytes(StandardCharsets.UTF_8));
        cmdMsg.setQos(1);
        cmdMsg.setProperties(cmdProps);

        System.out.println("    [B3] Step 2: publishing command to '" + cmdTopic + "' ...");
        IMqttToken pubToken = client.publish(cmdTopic, cmdMsg);
        pubToken.waitForCompletion(10_000);

        // Step 3: PUBACK arrives immediately on receipt — NOT after the response.
        // The client must implement its own response-wait timeout.
        int pubCode = pubToken.getReasonCodes() != null ? pubToken.getReasonCodes()[0] : -1;
        System.out.printf("    [B3] Step 3: PUBACK received: 0x%02X%s%n", pubCode,
                pubCode == 0x00 ? " (immediate, success — response comes separately)"
                : pubCode == 0x83 ? " (impl-specific: foreign ResponseTopic or QoS0 issue)"
                : pubCode == 0x97 ? " (quota exceeded: RpcMaxPending or user-prop cap)"
                : " (check broker logs)");

        // Step 4: Wait for the response on $reply/... (up to 10 s).
        // If no gRPC responder is running, this will time out.
        System.out.println("    [B3] Step 4: waiting for response on reply inbox (up to 10 s) ...");
        boolean responseArrived = replyLatch.await(10, TimeUnit.SECONDS);

        if (responseArrived) {
            System.out.println("    [B3] kubemq-executed = " + executedProp.get());
            System.out.println("    [B3] RPC command round-trip PASSED (v5)");
        } else {
            System.out.println("    [B3] No response within 10 s.");
            System.out.println("         If no gRPC responder is running on channel demo.cmd, this is expected.");
            System.out.println("         Start .work/test-harness/responder/ and re-run to see a full RPC round-trip.");
            System.out.println("         The PUBACK 0x00 confirms the command was accepted at the broker level.");
        }

        disconnectV5(client, "v5-rpc");
    }

    // =========================================================================
    // Shared helpers
    // =========================================================================

    /** Build MQTT 3.1.1 connect options (Paho v3 client). */
    private static MqttConnectOptions v311Options() {
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1); // level 4
        opts.setCleanSession(true);
        opts.setKeepAliveInterval(30);
        opts.setConnectionTimeout(10);
        opts.setAutomaticReconnect(false);
        return opts;
    }

    /** Build MQTT 5.0 connect options (Paho v5 async client). */
    private static MqttConnectionOptions v5Options() {
        MqttConnectionOptions opts = new MqttConnectionOptions();
        // Paho mqttv5 client defaults to MQTT 5.0; no explicit setMqttVersion needed.
        opts.setCleanStart(true);
        opts.setKeepAliveInterval(30);
        opts.setConnectionTimeout(10);
        opts.setAutomaticReconnect(false);
        return opts;
    }

    /** A minimal do-nothing callback for publish-only v5 clients. */
    private static org.eclipse.paho.mqttv5.client.MqttCallback minimalV5Callback(String label) {
        return new org.eclipse.paho.mqttv5.client.MqttCallback() {
            @Override
            public void messageArrived(String t, org.eclipse.paho.mqttv5.common.MqttMessage m) { }

            @Override
            public void disconnected(MqttDisconnectResponse r) {
                System.err.println("[" + label + "] disconnected: " + r.getReturnCode());
            }

            @Override
            public void mqttErrorOccurred(MqttException e) {
                System.err.println("[" + label + "] MQTT error: " + e.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttToken token) { }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("    " + label + " connected to " + serverURI);
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) { }
        };
    }

    private static void disconnectV3(MqttClient client, String label) {
        try {
            if (client.isConnected()) client.disconnect();
            client.close();
        } catch (Exception e) {
            System.err.println("[" + label + "] disconnect error: " + e.getMessage());
        }
    }

    private static void disconnectV5(MqttAsyncClient client, String label) {
        try {
            if (client.isConnected()) client.disconnect().waitForCompletion(3_000);
            client.close();
        } catch (Exception e) {
            System.err.println("[" + label + "] disconnect error: " + e.getMessage());
        }
    }
}

// Expected output (with gRPC responder running on channel demo.cmd type command):
//
// === protocol/mqtt-v311-vs-v5 ===
// Broker: tcp://127.0.0.1:1883
//
// --- SECTION A: MQTT 3.1.1 (org.eclipse.paho.client.mqttv3) ---
//
// [A1] Events pub/sub over MQTT 3.1.1
//     Topic   : events/demo/v3
//     Channel : demo.v3  (MQTT '/' -> KubeMQ '.')
//     Connecting v3.1.1 subscriber ...
//     v3.1.1 subscriber connected (protocol level 4)
//     Subscribed to 'events/demo/v3' at QoS 1
//     Connecting v3.1.1 publisher ...
//     v3.1.1 publisher connected
//     Publishing 'hello-from-v311' to 'events/demo/v3' QoS=1 ...
//     Published OK (no User Properties — v3.1.1 limitation)
//     [A1] Message received:
//          topic   = events/demo/v3
//          payload = hello-from-v311
//          QoS     = 1
//          User Properties: N/A (MQTT 3.1.1 has no user properties)
//     [A1] Events pub/sub PASSED (v3.1.1)
//
// [A2] RPC attempt over MQTT 3.1.1 (should be silently dropped)
//     Command topic : commands/demo/x
//     NOTE: v3.1.1 has no ResponseTopic/CorrelationData properties.
//           The connector SILENTLY DROPS commands/queries published over v3.1.1.
//     Publishing command to 'commands/demo/x' via v3.1.1 ...
//     PUBACK received (code 0x00 = success at wire level)
//     Waiting 3 s for a reply that will NOT arrive ...
//     [A2] No reply received (EXPECTED — v3.1.1 RPC is silently dropped)
//          To use RPC, upgrade the client to MQTT 5.0 (see Section B).
//
// --- SECTION B: MQTT 5.0 (org.eclipse.paho.mqttv5.client) ---
//
// [B1] Events pub/sub with User Properties (KubeMQ Tags) over MQTT 5.0
//     ...
//     [B1] Message received:
//          User Properties (KubeMQ Tags):
//            protocol = v5
//            source = java-example
//            variant = protocol/mqtt-v311-vs-v5
//     [B1] Events pub/sub with User Properties PASSED (v5)
//
// [B2] Queues shared subscription over MQTT 5.0
//     SUBACK 0x83 received for plain queues/... (EXPECTED — use $share prefix)
//     $share consumer-1 subscribed; granted QoS=1
//     [B2] produced: task-1
//     [B2] produced: task-2
//     [B2] consumer-1 received: task-1
//     [B2] consumer-1 received: task-2
//     [B2] Shared queue consume PASSED (v5)
//
// [B3] RPC command round-trip over MQTT 5.0
//     [B3] Step 1: subscribing reply inbox BEFORE publishing command ...
//     Reply inbox subscribed; SUBACK granted QoS=1
//     [B3] Step 2: publishing command to 'commands/demo/cmd' ...
//     [B3] Step 3: PUBACK received: 0x00 (immediate, success — response comes separately)
//     [B3] Step 4: waiting for response on reply inbox (up to 10 s) ...
//     [B3] Response received on: $reply/v5-rpc-.../rq1
//          body    : '' (command response has no body)
//          user-prop: kubemq-executed = true
//          CorrelationData echoed: corr-... (matches)
//     [B3] kubemq-executed = true
//     [B3] RPC command round-trip PASSED (v5)
