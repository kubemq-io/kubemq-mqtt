/**
 * Example: events/basic-pubsub
 *
 * Demonstrates basic fire-and-forget event publish/subscribe using MQTT 5.0.
 *
 * Wire contract:
 *   - Topic prefix "events/" selects the KubeMQ Events pattern.
 *   - Subscribe uses a single-level "+" wildcard (events/demo/+) which matches a
 *     one-segment channel; publish targets the concrete topic events/demo/x.
 *     (MQTT "+" maps to KubeMQ "*"; wildcards are EVENTS-only.)
 *   - "/" in the topic maps to "." in the KubeMQ channel name:
 *       MQTT topic  events/demo/x  ->  KubeMQ channel  demo.x
 *   - QoS 1 is used for both subscribe and publish (at-least-once delivery).
 *   - MQTT 5.0 User Properties on PUBLISH are forwarded by the connector as
 *     KubeMQ message Tags (bidirectional; v5-only; caps: 32 props / 4096 B).
 *
 * Gotchas:
 *   - Retain is NOT supported: a PUBLISH with Retain=true is silently dropped
 *     (PUBACK 0x00 returned, message not delivered, publish.error audited).
 *     Never set retain on MQTT messages to KubeMQ.
 *   - Events-Store is always StartNewOnly over MQTT — no historical replay.
 *   - Literal "." in a topic segment conflates with "/" (lossy — avoid).
 *
 * KUBEMQ_MQTT_URL (default tcp://localhost:1883) selects the broker and transport.
 *
 * Run: mvn compile exec:java
 */
package io.kubemq.examples.mqtt.events.basicpubsub;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Main {

    // -------------------------------------------------------------------------
    // Broker URL — read from KUBEMQ_MQTT_URL; default tcp://localhost:1883.
    // Scheme is used as-is by the Paho client: tcp://, ssl://, ws://.
    // -------------------------------------------------------------------------
    static String brokerUrl() {
        String url = System.getenv("KUBEMQ_MQTT_URL");
        return (url != null && !url.isEmpty()) ? url : "tcp://localhost:1883";
    }

    // -------------------------------------------------------------------------
    // MQTT 5.0 connection options — keepalive 30 s, clean start.
    // -------------------------------------------------------------------------
    static MqttConnectionOptions connectionOptions() {
        // MqttConnectionOptions from org.eclipse.paho.mqttv5.client always uses
        // MQTT protocol level 5 — no setMqttVersion() needed.
        MqttConnectionOptions opts = new MqttConnectionOptions();
        opts.setCleanStart(true);
        opts.setKeepAliveInterval(30);   // seconds
        opts.setConnectionTimeout(10);   // seconds
        opts.setAutomaticReconnect(false);
        return opts;
    }

    // -------------------------------------------------------------------------
    // Main flow
    // -------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        String broker  = brokerUrl();
        // Subscribe with a single-level '+' wildcard; publish to a concrete
        // one-segment channel. The '+' matches exactly one topic level, so
        // "events/demo/+" matches "events/demo/x". (MQTT '+' maps to KubeMQ '*'.)
        // KubeMQ channel name will be "demo.x" because MQTT "/" -> KubeMQ "."
        String subTopic = "events/demo/+";
        String pubTopic = "events/demo/x";
        String payload  = "hello";

        System.out.println("=== events/basic-pubsub (MQTT 5.0) ===");
        System.out.printf("Broker     : %s%n", broker);
        System.out.printf("Sub topic  : %s  (single-level '+' wildcard)%n", subTopic);
        System.out.printf("Pub topic  : %s%n", pubTopic);
        System.out.printf("Channel    : demo.x  (MQTT '/' maps to KubeMQ '.')%n");
        System.out.println();

        // Latch: released when the subscriber receives exactly one message.
        CountDownLatch received = new CountDownLatch(1);

        // -----------------------------------------------------------------------
        // Step 1 — Create and connect the SUBSCRIBER client.
        //          Must subscribe BEFORE the publisher sends so the message is
        //          not missed (Events pattern is fire-and-forget; no persistence).
        // -----------------------------------------------------------------------
        String subClientId = "java-events-sub-" + UUID.randomUUID().toString().substring(0, 8);
        MqttAsyncClient subscriber = new MqttAsyncClient(broker, subClientId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        subscriber.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String receivedTopic, MqttMessage message) throws Exception {
                String body = new String(message.getPayload(), StandardCharsets.UTF_8);
                System.out.println("--- Message received ---");
                System.out.printf("  Topic   : %s%n", receivedTopic);
                System.out.printf("  QoS     : %d%n", message.getQos());
                System.out.printf("  Payload : %s%n", body);

                // Print MQTT 5.0 User Properties arriving as KubeMQ Tags.
                MqttProperties props = message.getProperties();
                if (props != null && props.getUserProperties() != null
                        && !props.getUserProperties().isEmpty()) {
                    System.out.println("  User Properties (KubeMQ Tags on delivery):");
                    for (UserProperty up : props.getUserProperties()) {
                        System.out.printf("    %s = %s%n", up.getKey(), up.getValue());
                    }
                } else {
                    System.out.println("  User Properties: (none)");
                }
                received.countDown();
            }

            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                System.err.printf("Subscriber disconnected: reason=%d%n",
                        disconnectResponse.getReturnCode());
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                System.err.printf("Subscriber MQTT error: %s%n", exception.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttToken token) { /* not used by subscriber */ }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.printf("Subscriber connected to %s (reconnect=%b)%n", serverURI, reconnect);
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) { /* N/A */ }
        });

        System.out.println("[1] Connecting subscriber ...");
        subscriber.connect(connectionOptions()).waitForCompletion(10_000);

        System.out.printf("[2] Subscribing to '%s' at QoS 1 ...%n", subTopic);
        IMqttToken subToken = subscriber.subscribe(subTopic, 1);
        subToken.waitForCompletion(10_000);

        // Check SUBACK reason code (0x01 = QoS 1 granted).
        int[] grantedQos = subToken.getGrantedQos();
        if (grantedQos == null || grantedQos[0] >= 0x80) {
            System.err.printf("Subscription rejected; SUBACK reason=0x%02X. " +
                    "Wildcards on non-events topics -> 0xA2. Exiting.%n",
                    grantedQos == null ? 0xFF : grantedQos[0]);
            subscriber.disconnect();
            subscriber.close();
            System.exit(1);
        }
        System.out.printf("    SUBACK granted QoS=%d%n", grantedQos[0]);

        // -----------------------------------------------------------------------
        // Step 2 — Create and connect the PUBLISHER client.
        // -----------------------------------------------------------------------
        String pubClientId = "java-events-pub-" + UUID.randomUUID().toString().substring(0, 8);
        MqttAsyncClient publisher = new MqttAsyncClient(broker, pubClientId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        publisher.setCallback(new MqttCallback() {
            @Override
            public void disconnected(MqttDisconnectResponse response) {
                System.err.printf("Publisher disconnected: reason=%d%n", response.getReturnCode());
            }
            @Override public void mqttErrorOccurred(MqttException e) {
                System.err.printf("Publisher MQTT error: %s%n", e.getMessage());
            }
            @Override public void messageArrived(String t, MqttMessage m) { /* N/A */ }
            @Override public void deliveryComplete(IMqttToken token) { /* async; we use waitForCompletion */ }
            @Override public void connectComplete(boolean reconnect, String serverURI) {
                System.out.printf("Publisher  connected to %s (reconnect=%b)%n", serverURI, reconnect);
            }
            @Override public void authPacketArrived(int reasonCode, MqttProperties properties) { /* N/A */ }
        });

        System.out.println("[3] Connecting publisher ...");
        publisher.connect(connectionOptions()).waitForCompletion(10_000);

        // -----------------------------------------------------------------------
        // Step 3 — Build the MQTT message with a User Property.
        //          The MQTT 5.0 User Property "k1=v1" is forwarded by the KubeMQ
        //          MQTT connector as a KubeMQ message Tag.
        //          - Max 32 properties, 4096 bytes total; exceeding -> PUBACK 0x97.
        //          - MQTT 3.1.1 has no user-properties: v5-only feature.
        // -----------------------------------------------------------------------
        MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(1);
        // NOTE: Do NOT set msg.setRetained(true) — KubeMQ silently drops retained
        //       publishes (PUBACK 0x00 returned, message not delivered).

        // Attach MQTT 5.0 User Properties.  These map 1:1 to KubeMQ Tags.
        MqttProperties pubProps = new MqttProperties();
        pubProps.setUserProperties(List.of(
                new UserProperty("k1",      "v1"),
                new UserProperty("example", "events/basic-pubsub")
        ));
        msg.setProperties(pubProps);

        System.out.printf("[4] Publishing to '%s' QoS=1, payload='%s', user-props={k1=v1, example=events/basic-pubsub} ...%n",
                pubTopic, payload);
        IMqttToken pubToken = publisher.publish(pubTopic, msg);
        pubToken.waitForCompletion(10_000);

        // Check PUBACK reason code (0x00 = success).
        int pubReasonCode = pubToken.getReasonCodes() != null ? pubToken.getReasonCodes()[0] : -1;
        if (pubReasonCode != 0x00) {
            System.err.printf("Publish rejected; PUBACK reason=0x%02X%n", pubReasonCode);
            System.err.println("  0x80 = broker not ready (gated); 0x97 = quota exceeded " +
                    "(too many / too large user-props); 0x90 = invalid topic / empty channel");
        } else {
            System.out.printf("    PUBACK reason=0x00 (success)%n");
        }

        // -----------------------------------------------------------------------
        // Step 4 — Wait for the subscriber to receive the message (up to 10 s).
        // -----------------------------------------------------------------------
        System.out.println("[5] Waiting for message delivery ...");
        boolean delivered = received.await(10, TimeUnit.SECONDS);
        if (!delivered) {
            System.err.println("Timed out waiting for message. " +
                    "Verify the KubeMQ MQTT connector is enabled and the broker is reachable.");
        } else {
            System.out.println();
            System.out.println("Round-trip complete.");
            System.out.println("  KubeMQ channel : demo.x");
            System.out.println("  MQTT sub topic : events/demo/+  (single-level '+' wildcard)");
            System.out.println("  MQTT pub topic : events/demo/x");
            System.out.println("  User Properties were forwarded as KubeMQ Tags (if subscriber is v5).");
        }

        // -----------------------------------------------------------------------
        // Step 5 — Clean up.
        // -----------------------------------------------------------------------
        System.out.println("[6] Disconnecting ...");
        publisher.disconnect().waitForCompletion(5_000);
        publisher.close();
        subscriber.disconnect().waitForCompletion(5_000);
        subscriber.close();
        System.out.println("Done.");
        System.exit(delivered ? 0 : 1);
    }
}

// Expected output:
// === events/basic-pubsub (MQTT 5.0) ===
// Broker     : tcp://127.0.0.1:1883
// Sub topic  : events/demo/+  (single-level '+' wildcard)
// Pub topic  : events/demo/x
// Channel    : demo.x  (MQTT '/' maps to KubeMQ '.')
//
// [1] Connecting subscriber ...
// Subscriber connected to tcp://127.0.0.1:1883 (reconnect=false)
// [2] Subscribing to 'events/demo/+' at QoS 1 ...
//     SUBACK granted QoS=1
// [3] Connecting publisher ...
// Publisher  connected to tcp://127.0.0.1:1883 (reconnect=false)
// [4] Publishing to 'events/demo/x' QoS=1, payload='hello', user-props={k1=v1, example=events/basic-pubsub} ...
//     PUBACK reason=0x00 (success)
// [5] Waiting for message delivery ...
// --- Message received ---
//   Topic   : events/demo/x
//   QoS     : 1
//   Payload : hello
//   User Properties (KubeMQ Tags on delivery):
//     k1 = v1
//     example = events/basic-pubsub
//
// Round-trip complete.
//   KubeMQ channel : demo.x
//   MQTT sub topic : events/demo/+  (single-level '+' wildcard)
//   MQTT pub topic : events/demo/x
//   User Properties were forwarded as KubeMQ Tags (if subscriber is v5).
// [6] Disconnecting ...
// Done.
