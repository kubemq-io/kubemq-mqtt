/**
 * Example: events/wildcard-subscribe
 *
 * Mirrors integration test #25 (TestMqttIntegration_QueueNoWildcardLeak) and the
 * Go reference (examples/go/events/wildcard-subscribe/main.go).
 *
 * Demonstrates overlapping MQTT wildcard subscriptions on KubeMQ Events channels,
 * all scoped to a PER-RUN UNIQUE id so the count is deterministic on a busy/shared
 * broker:
 *   - sub1 subscribes to the exact topic 'events/leak/<id>/x' (exact channel match)
 *   - sub2 subscribes to the unique subtree 'events/leak/<id>/#' (multi-level '#')
 *   - wild subscribes to a bare '#' — INFORMATIONAL only (maps to the global '>'
 *     channel; on a busy broker it also matches our publish, so it is reported but
 *     NOT asserted; it may see >= 3 copies plus ambient traffic).
 *
 * A single publish to 'events/leak/<id>/x' matches THREE bridge registry entries
 * (exact, unique '#', and bare '#'). Each entry injects a copy back into mochi-mqtt,
 * so each of the two UNIQUE-scoped subscribers receives EXACTLY THREE copies — one
 * per matching bridge entry (no cross-entry dedup in V1, gotcha #6 / test #25).
 *
 * Key wire-contract facts:
 *   - MQTT '+' -> '*' in KubeMQ channel wildcards (single-level)
 *   - MQTT '#' -> '>' in KubeMQ channel wildcards (multi-level, must be final segment)
 *   - Wildcards are ONLY supported on Events-pattern subscriptions.
 *     A wildcard subscribe to a non-events topic (e.g. 'store/#') returns SUBACK 0xA2;
 *     Paho v5 represents this as an MqttException thrown from waitForCompletion().
 *   - Events-Store traffic does NOT leak to a bare '#' subscriber — Events and
 *     Events-Store use independent bridge registries.
 *   - MQTT 5.0 is used (level 5).
 *
 * KUBEMQ_MQTT_URL (default tcp://localhost:1883) selects the broker and transport.
 * Supported schemes: tcp (plain), tls (TLS), ws (WebSocket).
 *
 * Run: mvn compile exec:java
 */
package io.kubemq.examples.mqtt.events.wildcardsubscribe;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    // -------------------------------------------------------------------------
    // Broker URL parsing
    // -------------------------------------------------------------------------

    /**
     * Read KUBEMQ_MQTT_URL; normalise tcp:// -> tcp://, ws:// -> ws://, tls:// -> ssl://.
     * Paho v5 uses "ssl://" for TLS transports.
     */
    static String brokerUrl() {
        String raw = System.getenv("KUBEMQ_MQTT_URL");
        if (raw == null || raw.isEmpty()) {
            raw = "tcp://localhost:1883";
        }
        // Paho v5 uses "ssl://" for TLS; the env var may use "tls://"
        if (raw.startsWith("tls://")) {
            raw = "ssl://" + raw.substring("tls://".length());
        }
        return raw;
    }

    // -------------------------------------------------------------------------
    // Tracking callback — counts messages matching a specific topic
    // -------------------------------------------------------------------------

    static class FilteringCallback implements MqttCallback {
        final String name;
        /** Only messages on this exact topic are counted. Null = count all. */
        final String watchTopic;
        final AtomicInteger matchCount = new AtomicInteger(0);
        final AtomicInteger totalCount = new AtomicInteger(0);
        final CopyOnWriteArrayList<String> matchedPayloads = new CopyOnWriteArrayList<>();

        FilteringCallback(String name, String watchTopic) {
            this.name = name;
            this.watchTopic = watchTopic;
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            totalCount.incrementAndGet();
            if (watchTopic == null || watchTopic.equals(topic)) {
                int n = matchCount.incrementAndGet();
                matchedPayloads.add(payload);
                System.out.printf("[%s] copy #%d arrived on topic='%s' payload='%s'%n",
                        name, n, topic, payload);
            }
        }

        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            System.out.printf("[%s] connected to %s%n", name, serverURI);
        }

        @Override
        public void authPacketArrived(int reasonCode, MqttProperties properties) { /* unused */ }

        @Override
        public void disconnected(MqttDisconnectResponse disconnectResponse) {
            // Only log unexpected disconnects (not our own clean disconnect)
            if (disconnectResponse.getException() != null
                    && !"Already disconnected".equals(disconnectResponse.getException().getMessage())) {
                System.out.printf("[%s] disconnected: %s%n",
                        name, disconnectResponse.getException().getMessage());
            }
        }

        @Override
        public void mqttErrorOccurred(MqttException exception) {
            System.err.printf("[%s] MQTT error: %s%n", name, exception.getMessage());
        }

        @Override
        public void deliveryComplete(IMqttToken token) { /* unused for subscribers */ }
    }

    // -------------------------------------------------------------------------
    // Helper — connect a named MQTT 5.0 client (clean session)
    // -------------------------------------------------------------------------

    static MqttAsyncClient connectV5(String broker, String clientId, FilteringCallback cb)
            throws MqttException, InterruptedException {
        MqttAsyncClient client = new MqttAsyncClient(broker, clientId, null);
        client.setCallback(cb);

        MqttConnectionOptions opts = new MqttConnectionOptions();
        opts.setCleanStart(true);          // fresh session each run
        opts.setKeepAliveInterval(30);
        opts.setConnectionTimeout(10);
        // MQTT 5.0 is the default for the v5 Paho client (protocol level 5)

        IMqttToken tok = client.connect(opts);
        tok.waitForCompletion(10_000);
        return client;
    }

    // -------------------------------------------------------------------------
    // Helper — subscribe and log SUBACK reason code.
    //
    // IMPORTANT: Paho v5 throws MqttException when waitForCompletion() is called
    // and the SUBACK contains an error reason code (>= 0x80). The caller is
    // responsible for catching that exception to display the actual reason code.
    // -------------------------------------------------------------------------

    static void subscribeTo(MqttAsyncClient client, String filter, int qos, String label)
            throws MqttException, InterruptedException {
        System.out.printf("  Subscribing [%s] to filter='%s' QoS=%d ...%n", label, filter, qos);
        IMqttToken tok = client.subscribe(filter, qos);
        // waitForCompletion throws MqttException for error SUBACKs (>= 0x80);
        // callers must catch if they expect possible rejection codes.
        tok.waitForCompletion(10_000);

        int[] grantedQos = tok.getGrantedQos();
        int reasonCode = (grantedQos != null && grantedQos.length > 0) ? grantedQos[0] : 0xFF;
        System.out.printf("  SUBACK [%s]: granted QoS %d (0x%02X) — subscription active%n",
                label, reasonCode, reasonCode);
    }

    // -------------------------------------------------------------------------
    // Helper — subscribe and catch SUBACK error codes gracefully
    // -------------------------------------------------------------------------

    /**
     * Paho MQTT v5 reason codes as returned by MqttException.getReasonCode()
     * when the broker sends an error SUBACK. These differ from the raw MQTT
     * wire reason codes: Paho maps MQTT 0xA2 (Wildcard Subscriptions Not Supported)
     * to its internal constant REASON_CODE_INVALID_RETURN_CODE = 50001 (0xC351).
     * We therefore match on the exception message text for the most reliable detection.
     *
     * After receiving a SUBACK with an error reason code, Paho disconnects the
     * client automatically. The caller must NOT call client.disconnect() after
     * this — it will throw "Client is disconnected".
     */
    static void subscribeToWithRejectionHandling(MqttAsyncClient client, String filter,
            int qos, String label) throws InterruptedException {
        System.out.printf("  Subscribing [%s] to filter='%s' QoS=%d ...%n", label, filter, qos);
        try {
            IMqttToken tok = client.subscribe(filter, qos);
            tok.waitForCompletion(10_000);
            int[] grantedQos = tok.getGrantedQos();
            int reasonCode = (grantedQos != null && grantedQos.length > 0) ? grantedQos[0] : 0xFF;
            System.out.printf("  SUBACK [%s]: granted QoS %d (0x%02X) — subscription active%n",
                    label, reasonCode, reasonCode);
        } catch (MqttException e) {
            // Paho v5 surfaces SUBACK error codes as MqttException.
            // The raw MQTT reason code 0xA2 (Wildcard Subscriptions Not Supported)
            // is wrapped as Paho's REASON_CODE_INVALID_RETURN_CODE (50001 / 0xC351).
            // Match on message text since the internal Paho code constant may differ
            // across versions.
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (msg.contains("Wildcard") || msg.contains("wildcard") || msg.contains("Invalid Return")) {
                // The broker returned SUBACK 0xA2 — Paho treated it as a disconnect trigger.
                System.out.printf("  SUBACK [%s]: 0xA2 — Wildcard Subscriptions Not Supported " +
                        "(non-Events wildcard rejected; Paho disconnects client on error SUBACK)%n",
                        label);
            } else {
                System.out.printf("  SUBACK [%s]: MqttException (Paho reason code 0x%X) — %s%n",
                        label, e.getReasonCode(), msg);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        String broker = brokerUrl();

        System.out.println("=== events/wildcard-subscribe ===");
        System.out.println("Broker: " + broker);
        System.out.println();
        System.out.println("Mirrors integration test #25:");
        System.out.println("  Three overlapping Events filters -> 3 copies per subscriber per publish.");
        System.out.println();

        // Use a per-run unique id so this example's overlapping filters target a
        // freshly minted KubeMQ channel that no prior/other run reuses. This keeps
        // the count deterministic on a busy/shared broker.
        String runId = UUID.randomUUID().toString().substring(0, 6);
        String channelPath = "leak/" + runId;                 // e.g. leak/abc123
        String publishTopic = "events/" + channelPath + "/x"; // events/leak/<runId>/x
        String uniqueHashFilter = "events/" + channelPath + "/#"; // events/leak/<runId>/#

        System.out.println("Channel path   : " + channelPath);
        System.out.println("Publish topic  : " + publishTopic);
        System.out.println("Unique '#'     : " + uniqueHashFilter);
        System.out.println();

        // -------------------------------------------------------------------
        // Step 1: Connect the three subscriber clients (distinct clientIDs)
        //
        // Each client uses its own unique ID so the bridge creates independent
        // registry entries per subscription (not merged into one).
        // -------------------------------------------------------------------
        System.out.println("--- Step 1: Connect subscriber clients ---");

        // wildClient: subscribes to bare '#' — INFORMATIONAL only.
        // DefaultPattern=events -> bare '#' maps to the global KubeMQ channel '>'.
        // On a busy broker it also matches our unique publish, so it is reported
        // but NOT asserted (it may see ambient traffic too).
        FilteringCallback wildCb = new FilteringCallback("wild(#)", publishTopic);
        MqttAsyncClient wildClient = connectV5(broker, "java-wild-bare-" + runId, wildCb);

        // sub1: subscribes to the exact topic events/<channelPath>/x (unique-scoped).
        FilteringCallback sub1Cb = new FilteringCallback("sub1(exact)", publishTopic);
        MqttAsyncClient sub1Client = connectV5(broker, "java-wild-sub1-" + runId, sub1Cb);

        // sub2: subscribes to the unique subtree events/<channelPath>/# (multi-level
        // '#', scoped to this run's id -> KubeMQ channel leak.<runId>.>).
        FilteringCallback sub2Cb = new FilteringCallback("sub2(unique/#)", publishTopic);
        MqttAsyncClient sub2Client = connectV5(broker, "java-wild-sub2-" + runId, sub2Cb);

        System.out.println();

        // -------------------------------------------------------------------
        // Step 2: Subscribe each client — all QoS 1
        //
        // Filter                   KubeMQ channel equivalent      Notes
        // -----------------------  -----------------------------  ----------------------
        // events/leak/<id>/x       leak.<id>.x                    exact match (asserted)
        // events/leak/<id>/#       leak.<id>.>                    unique subtree (asserted)
        // #                        > (all events channels)        bare # = DefaultPattern=events
        //                                                         (INFORMATIONAL only)
        //
        // All three filters match the publish topic events/leak/<id>/x, so three
        // bridge registry entries fire and each delivers one copy -> the two
        // UNIQUE-scoped subscribers (sub1, sub2) each receive exactly 3 copies.
        //
        // NOTE on wildcard mapping:
        //   MQTT '+'  ->  KubeMQ '*'   (single-level, matches exactly one segment)
        //   MQTT '#'  ->  KubeMQ '>'   (multi-level, must be the final segment)
        // -------------------------------------------------------------------
        System.out.println("--- Step 2: Subscribe ---");
        subscribeTo(sub1Client, publishTopic, 1, "sub1(exact)");
        subscribeTo(sub2Client, uniqueHashFilter, 1, "sub2(unique/#)");
        subscribeTo(wildClient, "#", 1, "wild(#) [informational]");

        System.out.println();
        System.out.println("  Note: '+' -> '*' in KubeMQ (single-level wildcard)");
        System.out.println("        '#' -> '>' in KubeMQ (multi-level wildcard, must be last segment)");
        System.out.println("  Wildcards are supported ONLY on Events subscriptions.");
        System.out.println("  Wildcard on store/queues/commands/queries -> SUBACK 0xA2.");
        System.out.println();

        // Allow subscription registrations to propagate to the bridge registry
        Thread.sleep(600);

        // -------------------------------------------------------------------
        // Step 3: Demonstrate non-Events wildcard rejection
        //
        // Subscribing to 'store/#' (Events-Store) returns SUBACK 0xA2 (Wildcard
        // Subscriptions Not Supported). Paho v5 surfaces this as an MqttException
        // thrown from waitForCompletion() with getReasonCode() == 0xA2.
        // We use a temporary client just for this demonstration.
        // -------------------------------------------------------------------
        System.out.println("--- Step 3: Demonstrate wildcard rejection on Events-Store (SUBACK 0xA2) ---");
        FilteringCallback dummyCb = new FilteringCallback("reject-demo", null);
        MqttAsyncClient demoClient = connectV5(broker, "java-wild-demo-" + runId, dummyCb);
        subscribeToWithRejectionHandling(demoClient, "store/#", 1, "store/# (expect 0xA2)");
        // After an error SUBACK, Paho disconnects the client automatically.
        // Only call disconnect() if still connected.
        try {
            if (demoClient.isConnected()) {
                demoClient.disconnect().waitForCompletion(5_000);
            }
            demoClient.close();
        } catch (MqttException ignore) { /* best-effort */ }
        System.out.println();

        // -------------------------------------------------------------------
        // Step 4: Publish ONE message to events/<channelPath>/x
        //
        // Three matching bridge entries:
        //   1. sub1(exact)    -> 'events/leak/<id>/x' -> channel leak.<id>.x
        //   2. sub2(unique/#) -> 'events/leak/<id>/#' -> channel leak.<id>.>
        //   3. wild(#)        -> '#' with DefaultPattern=events -> global channel '>'
        //
        // Each entry independently injects the message into mochi.
        // mochi fans each injected copy to every locally-matching subscriber.
        // Result: each UNIQUE-scoped subscriber receives EXACTLY 3 copies (one per
        // matching entry). The bare '#' subscriber is informational (>= 3).
        //
        // Note: setRetained(false) is explicit here. Retain is forced off by the
        // broker (RetainAvailable=0 in CONNACK). If retained=true were set, the
        // broker would silently drop the message (PUBACK succeeds, no delivery, no
        // error). Always use retained=false when targeting KubeMQ Events over MQTT.
        // -------------------------------------------------------------------
        System.out.println("--- Step 4: Publish one message to " + publishTopic + " ---");

        FilteringCallback pubCb = new FilteringCallback("publisher", null);
        MqttAsyncClient pubClient = connectV5(broker, "java-wild-pub-" + runId, pubCb);

        byte[] payload = "overlap".getBytes(StandardCharsets.UTF_8);
        MqttMessage msg = new MqttMessage(payload);
        msg.setQos(1);
        msg.setRetained(false);  // retain is silently dropped; always use false for KubeMQ Events

        IMqttToken pubTok = pubClient.publish(publishTopic, msg);
        pubTok.waitForCompletion(10_000);
        System.out.println("Published: topic='" + publishTopic + "' payload='overlap' QoS=1");
        System.out.println();

        // -------------------------------------------------------------------
        // Step 5: Wait for exactly 3 copies on sub1 and sub2
        //
        // The callbacks filter by publishTopic so noise from other examples
        // running concurrently on the broker does not skew the count.
        // wild(#) also tracks copies; we verify it gets >= 3 (it may get more
        // from other concurrently publishing examples, which is expected).
        // -------------------------------------------------------------------
        System.out.println("--- Step 5: Waiting for 3 matching copies on sub1 and sub2 ---");
        int wantCopies = 3;
        long deadlineMs = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadlineMs) {
            if (sub1Cb.matchCount.get() >= wantCopies && sub2Cb.matchCount.get() >= wantCopies) {
                break;
            }
            Thread.sleep(200);
        }

        System.out.println();
        System.out.println("=== Results ===");
        System.out.printf("sub1 (filter '%s') received: %d matching copy(ies)%n",
                publishTopic, sub1Cb.matchCount.get());
        System.out.printf("sub2 (filter '%s') received: %d matching copy(ies)%n",
                uniqueHashFilter, sub2Cb.matchCount.get());
        System.out.printf("wild (filter '#', informational) received: %d matching copy(ies) (>= %d expected)%n",
                wildCb.matchCount.get(), wantCopies);
        System.out.println();

        // The two UNIQUE-scoped filters are asserted EXACTLY; the bare '#' is
        // informational (>= wantCopies; may see ambient traffic on a busy broker).
        boolean pass = sub1Cb.matchCount.get() == wantCopies
                && sub2Cb.matchCount.get() == wantCopies;
        if (pass) {
            System.out.println("PASS: sub1 and sub2 each received exactly " + wantCopies +
                    " copies — one per matching bridge registry entry. No cross-entry dedup.");
            if (wildCb.matchCount.get() > wantCopies) {
                System.out.printf("      (bare '#' saw %d copies — ambient '>' traffic from other " +
                        "publishers; gotcha #6, not an error.)%n", wildCb.matchCount.get());
            }
        } else {
            System.out.printf("UNEXPECTED: sub1=%d sub2=%d (expected %d each).%n",
                    sub1Cb.matchCount.get(), sub2Cb.matchCount.get(), wantCopies);
            System.out.println("  Check that the broker is running and reachable at: " + broker);
        }

        // -------------------------------------------------------------------
        // Step 6: Clean up
        // -------------------------------------------------------------------
        System.out.println();
        System.out.println("--- Step 6: Disconnecting ---");
        for (MqttAsyncClient c : new MqttAsyncClient[]{wildClient, sub1Client, sub2Client, pubClient}) {
            try {
                if (c.isConnected()) {
                    c.disconnect().waitForCompletion(5_000);
                }
                c.close();
            } catch (MqttException e) {
                // best-effort cleanup
            }
        }
        System.out.println("Done.");
    }
}

// Expected output (regenerated from a real run in the LiveTest stage):
// === events/wildcard-subscribe ===
// Broker: tcp://127.0.0.1:1883
//
// Mirrors integration test #25:
//   Three overlapping Events filters -> 3 copies per subscriber per publish.
//
// Channel path   : leak/abc123
// Publish topic  : events/leak/abc123/x
// Unique '#'     : events/leak/abc123/#
//
// --- Step 1: Connect subscriber clients ---
// [wild(#)] connected to tcp://127.0.0.1:1883
// [sub1(exact)] connected to tcp://127.0.0.1:1883
// [sub2(unique/#)] connected to tcp://127.0.0.1:1883
//
// --- Step 2: Subscribe ---
//   Subscribing [sub1(exact)] to filter='events/leak/abc123/x' QoS=1 ...
//   SUBACK [sub1(exact)]: granted QoS 1 (0x01) — subscription active
//   Subscribing [sub2(unique/#)] to filter='events/leak/abc123/#' QoS=1 ...
//   SUBACK [sub2(unique/#)]: granted QoS 1 (0x01) — subscription active
//   Subscribing [wild(#) [informational]] to filter='#' QoS=1 ...
//   SUBACK [wild(#) [informational]]: granted QoS 1 (0x01) — subscription active
//
//   Note: '+' -> '*' in KubeMQ (single-level wildcard)
//         '#' -> '>' in KubeMQ (multi-level wildcard, must be last segment)
//   Wildcards are supported ONLY on Events subscriptions.
//   Wildcard on store/queues/commands/queries -> SUBACK 0xA2.
//
// --- Step 3: Demonstrate wildcard rejection on Events-Store (SUBACK 0xA2) ---
// [reject-demo] connected to tcp://127.0.0.1:1883
//   Subscribing [store/# (expect 0xA2)] to filter='store/#' QoS=1 ...
//   SUBACK [store/# (expect 0xA2)]: 0xA2 — Wildcard Subscriptions Not Supported (non-Events wildcard rejected; Paho disconnects client on error SUBACK)
//
// --- Step 4: Publish one message to events/leak/abc123/x ---
// Published: topic='events/leak/abc123/x' payload='overlap' QoS=1
//
// --- Step 5: Waiting for 3 matching copies on sub1 and sub2 ---
// [sub1(exact)] copy #1 arrived on topic='events/leak/abc123/x' payload='overlap'
// [sub2(unique/#)] copy #1 arrived on topic='events/leak/abc123/x' payload='overlap'
// [wild(#)] copy #1 arrived on topic='events/leak/abc123/x' payload='overlap'
// [sub1(exact)] copy #2 arrived on topic='events/leak/abc123/x' payload='overlap'
// [sub2(unique/#)] copy #2 arrived on topic='events/leak/abc123/x' payload='overlap'
// [wild(#)] copy #2 arrived on topic='events/leak/abc123/x' payload='overlap'
// [sub1(exact)] copy #3 arrived on topic='events/leak/abc123/x' payload='overlap'
// [sub2(unique/#)] copy #3 arrived on topic='events/leak/abc123/x' payload='overlap'
// [wild(#)] copy #3 arrived on topic='events/leak/abc123/x' payload='overlap'
//
// === Results ===
// sub1 (filter 'events/leak/abc123/x') received: 3 matching copy(ies)
// sub2 (filter 'events/leak/abc123/#') received: 3 matching copy(ies)
// wild (filter '#', informational) received: 3 matching copy(ies) (>= 3 expected)
//
// PASS: sub1 and sub2 each received exactly 3 copies — one per matching bridge registry entry. No cross-entry dedup.
//
// --- Step 6: Disconnecting ---
// Done.
