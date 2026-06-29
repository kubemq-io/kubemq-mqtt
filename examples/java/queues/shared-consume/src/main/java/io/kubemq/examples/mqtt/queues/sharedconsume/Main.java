/**
 * Example: queues/shared-consume
 *
 * Demonstrates KubeMQ Queues consumption over MQTT 5.0 using shared subscriptions.
 *
 * Wire contract:
 *   Producer publishes to : queues/demo/q           (any QoS)
 *   Consumer subscribes to: $share/g1/queues/demo/q (MQTT 5.0, QoS >= 1)
 *
 * Ack model: ack-on-PUBACK. The Paho MQTT 5 client sends PUBACK automatically
 * when the message-arrived callback returns (auto-ack). No PUBACK within
 * QueueAckTimeoutSeconds (default 30 s) causes the broker to NAck and redeliver
 * the message to another consumer.
 *
 * Negative paths (documented, not exercised here):
 *   QoS 0 shared subscribe -> SUBACK 0x83   (test #8)
 *   Plain queues/... subscribe -> SUBACK 0x83 (test #9)
 *
 * GOTCHA: The $share group name (g1 here) is for audit/metrics only.
 * ALL groups compete in ONE shared KubeMQ queue pool. There are NO per-group
 * message copies. This is different from standard MQTT 5.0 broker semantics.
 *
 * KUBEMQ_MQTT_URL (default tcp://localhost:1883) selects broker and transport.
 *
 * Run: mvn compile exec:java
 */
package io.kubemq.examples.mqtt.queues.sharedconsume;

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
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    private static final int MESSAGE_COUNT = 4;
    // Canonical topics per S6.1.4
    private static final String PRODUCER_TOPIC = "queues/demo/q";
    private static final String SHARED_TOPIC   = "$share/g1/queues/demo/q";
    private static final int QOS = 1; // QoS 0 -> SUBACK 0x83

    /** Read broker URL from env; default tcp://localhost:1883. */
    static String brokerUrl() {
        String url = System.getenv("KUBEMQ_MQTT_URL");
        return (url != null && !url.isEmpty()) ? url : "tcp://localhost:1883";
    }

    public static void main(String[] args) throws Exception {
        String broker = brokerUrl();
        System.out.println("[shared-consume] broker  : " + broker);
        System.out.println("[shared-consume] produce : " + PRODUCER_TOPIC);
        System.out.println("[shared-consume] consume : " + SHARED_TOPIC + " (QoS " + QOS + ")");
        System.out.println();

        // Latch counts down each time a consumer receives a message.
        CountDownLatch received = new CountDownLatch(MESSAGE_COUNT);
        AtomicInteger totalReceived = new AtomicInteger(0);

        // ------------------------------------------------------------------ //
        // Step 1: Start two competing consumers (same shared topic, same group)
        // ------------------------------------------------------------------ //
        MqttAsyncClient consumer1 = createConsumer(broker, "consumer-1", received, totalReceived);
        MqttAsyncClient consumer2 = createConsumer(broker, "consumer-2", received, totalReceived);

        // Give subscriptions time to register before publishing
        Thread.sleep(600);

        // ------------------------------------------------------------------ //
        // Step 2: Produce messages
        // ------------------------------------------------------------------ //
        MqttAsyncClient producer = createProducer(broker);
        System.out.println("[producer] publishing " + MESSAGE_COUNT + " messages -> " + PRODUCER_TOPIC);

        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            String payload = "order-" + i;
            MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
            msg.setQos(QOS);
            // Retain MUST NOT be set — the connector forces RetainAvailable=0;
            // a retain-flagged publish is silently dropped (not delivered).
            msg.setRetained(false);
            IMqttToken token = producer.publish(PRODUCER_TOPIC, msg);
            token.waitForCompletion(5_000);
            System.out.println("[producer] published  : " + payload);
        }

        // ------------------------------------------------------------------ //
        // Step 3: Wait for all messages to arrive (max 15 s)
        // ------------------------------------------------------------------ //
        boolean allArrived = received.await(15, TimeUnit.SECONDS);
        System.out.println();
        if (allArrived) {
            System.out.println("[shared-consume] all " + MESSAGE_COUNT + " messages received ("
                    + totalReceived.get() + " total)");
        } else {
            System.out.println("[shared-consume] WARN: only " + totalReceived.get()
                    + "/" + MESSAGE_COUNT + " messages received within the timeout");
        }

        // ------------------------------------------------------------------ //
        // Step 4: Clean up
        // ------------------------------------------------------------------ //
        disconnectQuietly(producer,   "producer");
        disconnectQuietly(consumer1,  "consumer-1");
        disconnectQuietly(consumer2,  "consumer-2");
        System.out.println("[shared-consume] done.");
    }

    // ---------------------------------------------------------------------- //
    // Helpers                                                                  //
    // ---------------------------------------------------------------------- //

    /**
     * Create and connect an MQTT 5.0 consumer that subscribes to SHARED_TOPIC
     * at QoS 1 and prints each received message. PUBACK is sent automatically
     * by the Paho library after the callback returns (auto-ack).
     */
    private static MqttAsyncClient createConsumer(
            String broker,
            String name,
            CountDownLatch latch,
            AtomicInteger counter) throws MqttException, InterruptedException {

        // Each client needs a unique clientId to avoid session collision.
        String clientId = name + "-" + UUID.randomUUID().toString().substring(0, 6);
        MqttAsyncClient client = new MqttAsyncClient(broker, clientId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        client.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                System.out.println("[" + name + "] received : " + payload
                        + " (msgId=" + message.getId() + ")");
                // PUBACK is sent automatically by Paho after this method returns,
                // which acknowledges the message and removes it from the queue.
                counter.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                // Disconnect with un-acked messages causes immediate NAck/requeue.
                // Here we disconnect cleanly after all messages are processed.
                System.out.println("[" + name + "] disconnected: "
                        + disconnectResponse.getReasonString());
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                System.err.println("[" + name + "] MQTT error: " + exception.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttToken token) { /* publish-only */ }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("[" + name + "] connected  : " + serverURI
                        + (reconnect ? " (reconnect)" : ""));
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) { /* unused */ }
        });

        MqttConnectionOptions opts = new MqttConnectionOptions();
        opts.setCleanStart(true);
        opts.setKeepAliveInterval(30);
        // MQTT 5.0 is the default for paho mqttv5 client; no explicit version field.

        IMqttToken connectToken = client.connect(opts);
        connectToken.waitForCompletion(5_000);

        // Subscribe using shared subscription at QoS 1.
        // Plain queues/... subscribe -> SUBACK 0x83 (rejected by the connector).
        // QoS 0 shared subscribe    -> SUBACK 0x83 (rejected by the connector).
        IMqttToken subToken = client.subscribe(SHARED_TOPIC, QOS);
        subToken.waitForCompletion(5_000);

        int[] grantedQos = subToken.getGrantedQos();
        int grantedCode = (grantedQos != null && grantedQos.length > 0) ? grantedQos[0] : -1;
        if (grantedCode == 0x83) {
            System.err.println("[" + name + "] SUBACK 0x83 — subscription rejected by broker. "
                    + "Ensure QoS >= 1 and the $share prefix is present.");
        } else {
            System.out.println("[" + name + "] subscribed : " + SHARED_TOPIC
                    + " (granted QoS=" + grantedCode + ")");
        }

        return client;
    }

    /**
     * Create and connect an MQTT 5.0 producer client (publish-only).
     */
    private static MqttAsyncClient createProducer(String broker) throws MqttException {
        String clientId = "producer-" + UUID.randomUUID().toString().substring(0, 6);
        MqttAsyncClient client = new MqttAsyncClient(broker, clientId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        client.setCallback(new MqttCallback() {
            @Override public void messageArrived(String topic, MqttMessage message) { }
            @Override public void disconnected(MqttDisconnectResponse r) { }
            @Override public void mqttErrorOccurred(MqttException e) {
                System.err.println("[producer] MQTT error: " + e.getMessage());
            }
            @Override public void deliveryComplete(IMqttToken token) { }
            @Override public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("[producer]  connected  : " + serverURI);
            }
            @Override public void authPacketArrived(int reasonCode, MqttProperties properties) { }
        });

        MqttConnectionOptions opts = new MqttConnectionOptions();
        opts.setCleanStart(true);
        opts.setKeepAliveInterval(30);

        client.connect(opts).waitForCompletion(5_000);
        return client;
    }

    private static void disconnectQuietly(MqttAsyncClient client, String label) {
        try {
            if (client.isConnected()) {
                client.disconnect().waitForCompletion(3_000);
            }
            client.close();
        } catch (MqttException e) {
            System.err.println("[" + label + "] disconnect error: " + e.getMessage());
        }
    }
}
