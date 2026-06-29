/**
 * Example: events-store/start-new-only
 *
 * Demonstrates KubeMQ Events-Store over MQTT using the "store/" topic prefix.
 *
 * KEY TEACHING POINT:
 *   Events-Store over MQTT is ALWAYS StartNewOnly — there is NO historical replay.
 *   A subscriber only receives messages published AFTER it subscribes.
 *   Messages published BEFORE subscribing are silently discarded from the
 *   subscriber's perspective (stored on the server but never replayed to MQTT clients).
 *
 * This example proves the constraint with three explicit steps:
 *   Step 1 — Publish BEFORE subscribing (these messages will NOT be delivered).
 *   Step 2 — Subscribe to store/demo/s.
 *   Step 3 — Publish AFTER subscribing (this message IS delivered).
 *
 * Note on the shared channel:
 *   store/demo/s is a canonical demo channel that may carry concurrent traffic
 *   from other clients (e.g. sibling examples in other languages running at the
 *   same time). To stay correct under that traffic, every message this run
 *   publishes is tagged with a unique per-run id plus a phase marker
 *   ("pre"/"post"). On receive, each message is classified by run id into
 *   own-pre / own-post / foreign; foreign messages (including non-matching
 *   payloads) are ignored. The assertions only consider THIS run's tagged
 *   messages, so the StartNewOnly guarantee stays provable without a brittle
 *   exact-count assertion against the shared channel.
 *
 * Topic:   store/demo/s  (KubeMQ channel: demo.s)
 * Pattern: Events-Store, StartNewOnly
 * QoS:     1
 * Protocol: MQTT 5.0
 *
 * KUBEMQ_MQTT_URL (default tcp://localhost:1883) selects the broker and transport.
 * Scheme tcp:// -> plain TCP, tls:// -> TLS, ws:// -> WebSocket.
 *
 * Run:
 *   export KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883
 *   mvn compile exec:java
 */
package io.kubemq.examples.mqtt.eventsstore.startnewonly;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    // The MQTT topic for Events-Store: prefix "store/" + KubeMQ channel path.
    // "store/demo/s" -> KubeMQ channel "demo.s" (slash becomes dot).
    private static final String TOPIC = "store/demo/s";

    /**
     * Read KUBEMQ_MQTT_URL (default tcp://localhost:1883).
     * Supports schemes: tcp://, tls://, ws://.
     */
    static String brokerUrl() {
        String url = System.getenv("KUBEMQ_MQTT_URL");
        return (url != null && !url.isEmpty()) ? url : "tcp://localhost:1883";
    }

    /**
     * Build MqttConnectionOptions suited for MQTT 5.0 with a 30-second keepalive.
     * Clean start = true so no prior session state interferes with this demo.
     */
    static MqttConnectionOptions connectionOptions() {
        MqttConnectionOptions opts = new MqttConnectionOptions();
        opts.setKeepAliveInterval(30);
        opts.setCleanStart(true);
        opts.setConnectionTimeout(10);
        return opts;
    }

    /**
     * Publish a single message at QoS 1 and wait for PUBACK.
     * Logs the PUBACK reason code; any non-zero code is treated as a warning.
     */
    static void publish(MqttAsyncClient client, String topic, String payload, String label)
            throws MqttException, InterruptedException {
        MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(1);
        msg.setRetained(false); // Retain is FORCED OFF by the KubeMQ MQTT connector.
        IMqttToken token = client.publish(topic, msg);
        token.waitForCompletion(5_000);
        // A successful PUBACK carries reason code 0x00; Paho exposes it via
        // getReasonCodes() (null/empty when the broker returns no explicit code).
        int[] reasonCodes = token.getReasonCodes();
        int rc = (reasonCodes != null && reasonCodes.length > 0) ? reasonCodes[0] : 0;
        System.out.printf("[%s] PUBACK received (rc=%d) — published to %s%n", label, rc, topic);
        System.out.printf("[%s] payload: %s%n", label, payload);
    }

    public static void main(String[] args) throws Exception {
        String broker = brokerUrl();
        // Unique per-run id: reused for the client-id suffix AND embedded in
        // every payload so the subscriber can ignore concurrent foreign traffic
        // on the shared channel.
        String runId = UUID.randomUUID().toString();
        String sfx = runId.substring(0, 8);

        System.out.println("=== java — Events-Store: Start-New-Only (no replay) ===");
        System.out.println("Broker : " + broker);
        System.out.println("Topic  : " + TOPIC);
        System.out.println("Pattern: Events-Store | StartNewOnly (ALWAYS — no replay over MQTT)");
        System.out.println("QoS    : 1");
        System.out.println("Proto  : MQTT 5.0");
        System.out.println("Run ID : " + runId);
        System.out.println();

        // -----------------------------------------------------------------------
        // STEP 1: Publisher connects and publishes BEFORE any subscriber exists.
        //         These messages are stored by KubeMQ but NEVER replayed to an
        //         MQTT subscriber — StartNewOnly means "only messages from now on".
        // -----------------------------------------------------------------------
        System.out.println("--- Step 1: Publish BEFORE subscribing (will NOT be delivered) ---");

        MqttAsyncClient prePublisher = new MqttAsyncClient(broker, "java-es-prepub-" + sfx);
        prePublisher.connect(connectionOptions()).waitForCompletion(10_000);
        System.out.println("[pre-publisher] connected");

        publish(prePublisher, TOPIC,
                "{\"event\":\"login\",\"user\":\"alice\",\"run_id\":\"" + runId + "\",\"phase\":\"pre\",\"ts\":\"" + Instant.now() + "\",\"note\":\"pre-subscribe — will NOT arrive\"}",
                "pre-publisher");
        publish(prePublisher, TOPIC,
                "{\"event\":\"login\",\"user\":\"bob\",\"run_id\":\"" + runId + "\",\"phase\":\"pre\",\"ts\":\"" + Instant.now() + "\",\"note\":\"pre-subscribe — will NOT arrive\"}",
                "pre-publisher");
        publish(prePublisher, TOPIC,
                "{\"event\":\"login\",\"user\":\"carol\",\"run_id\":\"" + runId + "\",\"phase\":\"pre\",\"ts\":\"" + Instant.now() + "\",\"note\":\"pre-subscribe — will NOT arrive\"}",
                "pre-publisher");

        System.out.println("[pre-publisher] 3 messages published BEFORE subscriber connected.");
        System.out.println("[pre-publisher] These are stored in Events-Store but are NOT replayed over MQTT.");
        System.out.println();

        // -----------------------------------------------------------------------
        // STEP 2: Subscriber connects and subscribes to store/demo/s.
        //         The SUBACK QoS value confirms the subscription was accepted.
        //         Because Events-Store is always StartNewOnly, only messages
        //         published AFTER this subscribe call will be delivered.
        // -----------------------------------------------------------------------
        System.out.println("--- Step 2: Subscribe (StartNewOnly — no historical replay) ---");

        // Classify only THIS run's messages (matched by run id) by phase. Foreign
        // messages on the shared channel are counted separately and ignored by the
        // assertions. The latch fires when this run's post-subscribe message lands.
        final String runTag  = "\"run_id\":\"" + runId + "\"";
        final String preTag  = "\"phase\":\"pre\"";
        final String postTag = "\"phase\":\"post\"";
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger ownPreCount  = new AtomicInteger(0);
        AtomicInteger ownPostCount = new AtomicInteger(0);
        AtomicInteger foreignCount = new AtomicInteger(0);

        MqttAsyncClient subscriber = new MqttAsyncClient(broker, "java-es-sub-" + sfx);
        subscriber.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                if (!payload.contains(runTag)) {
                    foreignCount.incrementAndGet();
                    System.out.printf("[subscriber] ignoring foreign message on %s%n", topic);
                    return;
                }
                if (payload.contains(preTag)) {
                    int n = ownPreCount.incrementAndGet();
                    System.out.printf("[subscriber] OWN-PRE message #%d received on %s%n", n, topic);
                } else if (payload.contains(postTag)) {
                    int n = ownPostCount.incrementAndGet();
                    System.out.printf("[subscriber] OWN-POST message #%d received on %s%n", n, topic);
                    latch.countDown();
                }
                System.out.printf("[subscriber] payload: %s%n", payload);
            }

            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                System.out.println("[subscriber] disconnected: " + disconnectResponse.getReasonString());
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                System.err.println("[subscriber] MQTT error: " + exception.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                // Not used by subscriber.
            }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("[subscriber] connected (reconnect=" + reconnect + ") to " + serverURI);
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) {
                // Auth not used in this example.
            }
        });

        subscriber.connect(connectionOptions()).waitForCompletion(10_000);

        IMqttToken subToken = subscriber.subscribe(TOPIC, 1);
        subToken.waitForCompletion(10_000);
        int[] grantedQos = subToken.getGrantedQos();
        int granted = (grantedQos != null && grantedQos.length > 0) ? grantedQos[0] : -1;

        if (granted > 2) {
            // SUBACK reason codes > 2 are error codes.
            // 0x83 = implementation specific (plain queues/... or QoS0 $share sub)
            // 0xA2 = wildcard subscriptions not supported (non-events wildcard)
            System.err.printf("[subscriber] SUBACK rejected: reason=0x%02X%n", granted);
            subscriber.disconnect();
            prePublisher.disconnect();
            System.exit(1);
        }

        System.out.printf("[subscriber] subscribed to %s (granted QoS %d)%n", TOPIC, granted);
        System.out.println("[subscriber] StartNewOnly: only messages published FROM NOW are delivered.");
        System.out.println("[subscriber] The 3 pre-subscribe messages will NOT arrive — no replay.");
        System.out.println();

        // Brief settle to ensure the subscribe is fully registered server-side.
        Thread.sleep(400);

        // -----------------------------------------------------------------------
        // STEP 3: Publisher publishes AFTER subscribing.
        //         This message IS delivered to the subscriber because it was
        //         published after the subscriber registered.
        // -----------------------------------------------------------------------
        System.out.println("--- Step 3: Publish AFTER subscribing (WILL be delivered) ---");

        MqttAsyncClient postPublisher = new MqttAsyncClient(broker, "java-es-postpub-" + sfx);
        postPublisher.connect(connectionOptions()).waitForCompletion(10_000);
        System.out.println("[post-publisher] connected");

        publish(postPublisher, TOPIC,
                "{\"event\":\"login\",\"user\":\"dave\",\"run_id\":\"" + runId + "\",\"phase\":\"post\",\"ts\":\"" + Instant.now() + "\",\"note\":\"post-subscribe — WILL arrive\"}",
                "post-publisher");
        System.out.println();

        // -----------------------------------------------------------------------
        // STEP 4: Wait for this run's post-subscribe message to arrive.
        // -----------------------------------------------------------------------
        System.out.println("--- Step 4: Waiting for the post-subscribe message to arrive ---");
        boolean received = latch.await(10, TimeUnit.SECONDS);

        // Brief window to catch any late replay of this run's pre-subscribe msgs.
        Thread.sleep(500);
        System.out.println();

        if (!received) {
            System.err.println("ERROR: Timed out — this run's post-subscribe message was NOT received.");
            System.err.println("Check broker connectivity and that the MQTT connector is enabled.");
            prePublisher.disconnect();
            postPublisher.disconnect();
            subscriber.disconnect();
            System.exit(1);
        }

        // -----------------------------------------------------------------------
        // SUMMARY + assertions
        //   (a) NONE of this run's pre-subscribe messages were replayed.
        //   (b) This run's post-subscribe message arrived.
        // Foreign traffic on the shared channel is tolerated, not asserted against.
        // -----------------------------------------------------------------------
        System.out.println("=== Summary ===");
        System.out.println("Pre-subscribe messages published       : 3");
        System.out.printf( "This run's pre-subscribe msgs replayed : %d  (StartNewOnly — expect 0)%n", ownPreCount.get());
        System.out.printf( "This run's post-subscribe msgs received: %d  (expect >= 1)%n", ownPostCount.get());
        System.out.printf( "Foreign messages ignored               : %d%n", foreignCount.get());
        System.out.println();

        if (ownPreCount.get() != 0) {
            System.err.printf("ERROR: StartNewOnly violation — %d of this run's pre-subscribe "
                    + "message(s) were replayed.%n", ownPreCount.get());
            prePublisher.disconnect();
            postPublisher.disconnect();
            subscriber.disconnect();
            System.exit(1);
        }

        System.out.println("MQTT Events-Store constraint confirmed:");
        System.out.println("  - Topic prefix 'store/' selects the KubeMQ Events-Store pattern.");
        System.out.println("  - Over MQTT, Events-Store is ALWAYS StartNewOnly.");
        System.out.println("  - This run's pre-subscribe messages were NOT replayed (foreign traffic ignored).");
        System.out.println("  - Historical replay (StartFromFirst, StartFromSequence, etc.) is");
        System.out.println("    NOT available via MQTT — use the gRPC SDK for replay.");
        System.out.println("  - Retained messages are silently dropped (RetainAvailable=0).");

        // Clean disconnect.
        prePublisher.disconnect().waitForCompletion(3_000);
        prePublisher.close();
        postPublisher.disconnect().waitForCompletion(3_000);
        postPublisher.close();
        subscriber.disconnect().waitForCompletion(3_000);
        subscriber.close();

        System.out.println("\nDone.");
    }
}
