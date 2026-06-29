/**
 * Example: connectivity/websocket
 *
 * Demonstrates connecting to the KubeMQ MQTT broker over WebSocket (ws:// scheme)
 * using MQTT 5.0.  Publishes one event and receives it back, confirming that the
 * WebSocket transport layer is transparent to the MQTT protocol.
 *
 * Transport selection: the KUBEMQ_MQTT_URL env var controls the broker URL.
 * When the scheme is ws:// or wss:// the Paho v5 client automatically uses
 * WebSocket framing for the MQTT bytes — no extra configuration is needed.
 *
 * Wire contract (from spec §6.3 row 9):
 *   topic   events/demo/ws   → KubeMQ Events channel "demo.ws"
 *   QoS     1
 *   MQTT    5.0 (CONNECT protocol level 5)
 *
 * Reason codes handled:
 *   0x00  success
 *   0x80  broker not ready (publish gated)
 *   0x8B  server shutting down (DISCONNECT)
 *   0x90  topic-name-invalid (empty channel)
 *
 * Run: mvn compile exec:java
 */
package io.kubemq.examples.mqtt.connectivity.websocket;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Main {

    // -------------------------------------------------------------------------
    // Broker URL resolution
    // -------------------------------------------------------------------------

    /**
     * Returns the broker URL to use, substituting the ws:// default when
     * KUBEMQ_MQTT_URL is absent or empty.
     *
     * Accepted schemes understood by the Paho v5 client:
     *   tcp://   — plain MQTT over TCP
     *   tls://   — MQTT over TLS
     *   ws://    — MQTT over WebSocket          ← what this example tests
     *   wss://   — MQTT over secure WebSocket
     */
    static String brokerUrl() {
        String url = System.getenv("KUBEMQ_MQTT_URL");
        if (url != null && !url.isEmpty()) {
            return url;
        }
        return "ws://localhost:8083/";
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        String broker   = brokerUrl();
        String clientId = "kubemq-mqtt-java-ws-" + UUID.randomUUID().toString().substring(0, 8);
        String topic    = "events/demo/ws";   // → KubeMQ Events channel "demo.ws"
        String payload  = "Hello via WebSocket from Java MQTT 5.0!";

        System.out.println("=== connectivity/websocket ===");
        System.out.println("Broker   : " + broker);
        System.out.println("ClientID : " + clientId);
        System.out.println("Topic    : " + topic + "  (KubeMQ Events channel: demo.ws)");
        System.out.println();

        // ------------------------------------------------------------------
        // Latch: wait until subscriber receives one message.
        // ------------------------------------------------------------------
        CountDownLatch received = new CountDownLatch(1);

        // ------------------------------------------------------------------
        // Step 1: create subscriber client and connect
        // ------------------------------------------------------------------
        String subClientId = clientId + "-sub";
        MqttAsyncClient subscriber = new MqttAsyncClient(broker, subClientId,
                /* persistence */ null);

        subscriber.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String t, MqttMessage msg) {
                System.out.printf("[sub] Received on '%s' (QoS %d): %s%n",
                        t, msg.getQos(),
                        new String(msg.getPayload(), StandardCharsets.UTF_8));
                received.countDown();
            }

            @Override
            public void disconnected(MqttDisconnectResponse resp) {
                System.out.println("[sub] Disconnected: " + resp.getReasonString());
            }

            @Override
            public void mqttErrorOccurred(MqttException e) {
                System.err.println("[sub] MQTT error: " + e.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                // publish confirmations on sub client (none expected here)
            }

            @Override
            public void connectComplete(boolean reconnect, String uri) {
                System.out.println("[sub] Connected to: " + uri
                        + (reconnect ? " (reconnect)" : ""));
            }

            @Override
            public void authPacketArrived(int rc, MqttProperties props) {
                // AUTH packet handling not needed for this example
            }
        });

        MqttConnectionOptions subOpts = new MqttConnectionOptions();
        subOpts.setCleanStart(true);
        subOpts.setKeepAliveInterval(30);
        // MQTT 5.0 is the default for the Paho v5 client; no extra flag needed.

        System.out.println("[sub] Connecting ...");
        subscriber.connect(subOpts).waitForCompletion(10_000);
        System.out.println("[sub] Connected via WebSocket (MQTT 5.0).");

        // ------------------------------------------------------------------
        // Step 2: subscribe to events/demo/ws at QoS 1
        // ------------------------------------------------------------------
        System.out.println("[sub] Subscribing to '" + topic + "' at QoS 1 ...");
        subscriber.subscribe(topic, 1).waitForCompletion(10_000);
        System.out.println("[sub] Subscribed.");
        System.out.println();

        // Allow the subscription to register with the KubeMQ bridge.
        Thread.sleep(300);

        // ------------------------------------------------------------------
        // Step 3: create publisher client and connect
        // ------------------------------------------------------------------
        String pubClientId = clientId + "-pub";
        MqttAsyncClient publisher = new MqttAsyncClient(broker, pubClientId,
                /* persistence */ null);

        publisher.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String t, MqttMessage msg) { /* not expected */ }

            @Override
            public void disconnected(MqttDisconnectResponse resp) {
                System.out.println("[pub] Disconnected: " + resp.getReasonString());
            }

            @Override
            public void mqttErrorOccurred(MqttException e) {
                System.err.println("[pub] MQTT error: " + e.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                System.out.println("[pub] PUBACK received (reason: "
                        + reasonCodeLabel(token.getReasonCodes()) + ").");
            }

            @Override
            public void connectComplete(boolean reconnect, String uri) {
                System.out.println("[pub] Connected to: " + uri
                        + (reconnect ? " (reconnect)" : ""));
            }

            @Override
            public void authPacketArrived(int rc, MqttProperties props) { }
        });

        MqttConnectionOptions pubOpts = new MqttConnectionOptions();
        pubOpts.setCleanStart(true);
        pubOpts.setKeepAliveInterval(30);

        System.out.println("[pub] Connecting ...");
        publisher.connect(pubOpts).waitForCompletion(10_000);
        System.out.println("[pub] Connected via WebSocket (MQTT 5.0).");

        // ------------------------------------------------------------------
        // Step 4: publish one message to events/demo/ws at QoS 1
        // ------------------------------------------------------------------
        MqttMessage message = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        message.setQos(1);
        // Retain MUST NOT be set: the KubeMQ MQTT connector forces RetainAvailable=0.
        // A retain=true publish is SILENTLY DROPPED (PUBACK 0x00 but message is gone).
        message.setRetained(false);

        System.out.println("[pub] Publishing to '" + topic + "' (QoS 1): " + payload);
        publisher.publish(topic, message).waitForCompletion(10_000);

        // ------------------------------------------------------------------
        // Step 5: wait for subscriber to receive the message (up to 10 s)
        // ------------------------------------------------------------------
        boolean ok = received.await(10, TimeUnit.SECONDS);
        if (!ok) {
            System.err.println("ERROR: timed out waiting for message delivery.");
            System.exit(1);
        }
        System.out.println();
        System.out.println("Round-trip complete over WebSocket transport.");

        // ------------------------------------------------------------------
        // Step 6: clean disconnect
        // ------------------------------------------------------------------
        publisher.disconnect().waitForCompletion(5_000);
        publisher.close();
        System.out.println("[pub] Disconnected.");

        subscriber.disconnect().waitForCompletion(5_000);
        subscriber.close();
        System.out.println("[sub] Disconnected.");

        System.out.println();
        System.out.println("=== Done ===");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Human-readable label for a PUBACK/SUBACK reason code array. */
    static String reasonCodeLabel(int[] codes) {
        if (codes == null || codes.length == 0) return "n/a";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < codes.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format("0x%02X", codes[i]));
            switch (codes[i]) {
                case 0x00 -> sb.append("(Success)");
                case 0x80 -> sb.append("(Unspecified error / broker not ready)");
                case 0x83 -> sb.append("(Implementation specific — QoS0 queue / plain queue sub / foreign $reply / RPC subscribe)");
                case 0x8B -> sb.append("(Server shutting down)");
                case 0x90 -> sb.append("(Topic name invalid — empty channel or DefaultPattern=none)");
                case 0x97 -> sb.append("(Quota exceeded — RpcMaxPending / user-prop cap)");
                case 0x9A -> sb.append("(Retain not supported — only Will-retain at CONNECT)");
                case 0xA2 -> sb.append("(Wildcard subscriptions not supported on non-Events topics)");
            }
        }
        return sb.toString();
    }
}

// Expected output:
// === connectivity/websocket ===
// Broker   : ws://127.0.0.1:8083/
// ClientID : kubemq-mqtt-java-ws-<8hex>
// Topic    : events/demo/ws  (KubeMQ Events channel: demo.ws)
//
// [sub] Connecting ...
// [sub] Connected to: ws://127.0.0.1:8083/
// [sub] Connected via WebSocket (MQTT 5.0).
// [sub] Subscribing to 'events/demo/ws' at QoS 1 ...
// [sub] Subscribed.
//
// [pub] Connecting ...
// [pub] Connected to: ws://127.0.0.1:8083/
// [pub] Connected via WebSocket (MQTT 5.0).
// [pub] Publishing to 'events/demo/ws' (QoS 1): Hello via WebSocket from Java MQTT 5.0!
// [pub] PUBACK received (reason: 0x00(Success)).
// [sub] Received on 'events/demo/ws' (QoS 1): Hello via WebSocket from Java MQTT 5.0!
//
// Round-trip complete over WebSocket transport.
// [pub] Disconnected.
// [sub] Disconnected.
//
// === Done ===
