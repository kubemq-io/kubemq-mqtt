/**
 * Example: queues/ack-and-redelivery
 *
 * Demonstrates KubeMQ Queue ack-on-PUBACK semantics and automatic redelivery.
 *
 * Wire contract:
 *   - Produce to:  queues/<channel>               (plain publish, QoS 1)
 *   - Consume via: $share/<group>/queues/<channel> (MQTT 5.0, QoS 1)
 *   - Ack model:   PUBACK = ack. No PUBACK within QueueAckTimeoutSeconds
 *                  (default 30 s) → NAck → redeliver.
 *   - Disconnect with unacked messages → immediate NAck → requeue (no loss).
 *
 * This example shows the fast-path redelivery: Consumer-1 receives a message
 * but intentionally holds the PUBACK (setManualAcks=true), then disconnects.
 * Consumer-2 (already subscribed on the same shared group) receives the
 * requeued message and acknowledges it, completing at-least-once delivery.
 *
 * The ack-timeout path (QueueAckTimeoutSeconds, 30 s default) is the slower
 * redelivery trigger: if the client stays connected but never calls
 * messageArrivedComplete(), the broker NAcks and requeues after the timeout.
 * Both paths are explained; only the disconnect path is demonstrated live
 * (no 30-second wait in the example).
 *
 * KUBEMQ_MQTT_URL (default tcp://localhost:1883) selects the broker.
 *
 * Run: mvn compile exec:java
 */
package io.kubemq.examples.mqtt.queues.acknowledgeredelivery;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Returns the broker URL from KUBEMQ_MQTT_URL (default tcp://localhost:1883).
     * Recognises tcp://, tls://, and ws:// schemes.
     */
    static String brokerUrl() {
        String url = System.getenv("KUBEMQ_MQTT_URL");
        return (url != null && !url.isEmpty()) ? url : "tcp://localhost:1883";
    }

    static MqttConnectionOptions connectOptions() {
        MqttConnectionOptions opts = new MqttConnectionOptions();
        opts.setCleanStart(true);
        opts.setKeepAliveInterval(30);
        // MQTT 5.0 is the default for the Paho v5 client.
        return opts;
    }

    // -----------------------------------------------------------------------
    // Inner helper: a MqttCallback that queues received messages
    // -----------------------------------------------------------------------

    static class RecordingCallback implements MqttCallback {
        final String name;
        final BlockingQueue<MqttMessage> inbox = new ArrayBlockingQueue<>(64);

        RecordingCallback(String name) {
            this.name = name;
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            System.out.printf("[%s] messageArrived: topic=%s  payload=%s  id=%d  qos=%d%n",
                    name, topic,
                    new String(message.getPayload(), StandardCharsets.UTF_8),
                    message.getId(), message.getQos());
            inbox.offer(message);
        }

        @Override
        public void disconnected(MqttDisconnectResponse response) {
            System.out.printf("[%s] disconnected: reasonCode=%d%n",
                    name, response.getReturnCode());
        }

        @Override
        public void mqttErrorOccurred(MqttException exception) {
            System.err.printf("[%s] MQTT error: %s%n", name, exception.getMessage());
        }

        @Override
        public void deliveryComplete(IMqttToken token) { /* publish side, not used here */ }

        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            System.out.printf("[%s] connected to %s (reconnect=%b)%n", name, serverURI, reconnect);
        }

        @Override
        public void authPacketArrived(int reasonCode, MqttProperties properties) { /* not used */ }
    }

    // -----------------------------------------------------------------------
    // Main
    // -----------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        String broker = brokerUrl();
        // Channel name. '/' → '.' when mapped to KubeMQ channel name (demo.rq).
        String channel = "demo/rq";
        // Produce topic: queues/<channel>
        String produceTopic = "queues/" + channel;
        // Consume topic: $share/<group>/queues/<channel>  (MQTT 5.0, QoS 1)
        String consumeTopic = "$share/g1/queues/" + channel;

        System.out.println("=== queues/ack-and-redelivery ===");
        System.out.println("Broker:        " + broker);
        System.out.println("Produce topic: " + produceTopic);
        System.out.println("Consume topic: " + consumeTopic);
        System.out.println();

        // ------------------------------------------------------------------
        // Step 1: Connect Consumer-1 (manual ack — will NOT send PUBACK)
        // ------------------------------------------------------------------
        System.out.println("[Step 1] Connecting Consumer-1 (manual ack — will withhold PUBACK) ...");
        RecordingCallback cb1 = new RecordingCallback("Consumer-1");
        MqttAsyncClient consumer1 = new MqttAsyncClient(broker, "java-mqtt-q-c1-" + UUID.randomUUID(), null);
        consumer1.setCallback(cb1);
        // Manual ack: messageArrivedComplete() must be called explicitly to send PUBACK.
        // Until PUBACK is sent the message remains "in-flight" (unacked).
        consumer1.setManualAcks(true);
        consumer1.connect(connectOptions()).waitForCompletion(10_000);

        MqttSubscription sub1 = new MqttSubscription(consumeTopic, 1);
        consumer1.subscribe(new MqttSubscription[]{sub1}, null, null, new MqttProperties())
                .waitForCompletion(10_000);
        System.out.printf("[Consumer-1] subscribed to %s (QoS 1, manual ack)%n", consumeTopic);

        // ------------------------------------------------------------------
        // Step 2: Connect Consumer-2 (auto ack — will send PUBACK immediately)
        // ------------------------------------------------------------------
        System.out.println("[Step 2] Connecting Consumer-2 (auto ack) ...");
        RecordingCallback cb2 = new RecordingCallback("Consumer-2");
        MqttAsyncClient consumer2 = new MqttAsyncClient(broker, "java-mqtt-q-c2-" + UUID.randomUUID(), null);
        consumer2.setCallback(cb2);
        // setManualAcks(false) is the default: PUBACK is sent automatically by the library.
        consumer2.setManualAcks(false);
        consumer2.connect(connectOptions()).waitForCompletion(10_000);

        MqttSubscription sub2 = new MqttSubscription(consumeTopic, 1);
        consumer2.subscribe(new MqttSubscription[]{sub2}, null, null, new MqttProperties())
                .waitForCompletion(10_000);
        System.out.printf("[Consumer-2] subscribed to %s (QoS 1, auto ack)%n", consumeTopic);

        // Brief pause to let both subscriptions register in the KubeMQ bridge
        // before publishing.
        Thread.sleep(500);

        // ------------------------------------------------------------------
        // Step 3: Produce one message
        // ------------------------------------------------------------------
        System.out.println("[Step 3] Producing one message to " + produceTopic + " ...");
        MqttAsyncClient producer = new MqttAsyncClient(broker, "java-mqtt-q-prod-" + UUID.randomUUID(), null);
        producer.connect(connectOptions()).waitForCompletion(10_000);

        MqttMessage outgoing = new MqttMessage("requeue-me".getBytes(StandardCharsets.UTF_8));
        outgoing.setQos(1);
        outgoing.setRetained(false); // retain is always off on this broker
        producer.publish(produceTopic, outgoing).waitForCompletion(10_000);
        System.out.println("[Producer]    PUBACK received — message enqueued.");
        producer.disconnect().waitForCompletion(5_000);
        producer.close();

        // ------------------------------------------------------------------
        // Step 4: Consumer-1 receives the message but intentionally holds PUBACK
        // ------------------------------------------------------------------
        System.out.println("[Step 4] Waiting for Consumer-1 to receive the message ...");
        MqttMessage msg1 = cb1.inbox.poll(10, TimeUnit.SECONDS);
        if (msg1 == null) {
            System.err.println("ERROR: Consumer-1 did not receive the message within 10 s.");
            System.exit(1);
        }
        System.out.printf("[Consumer-1] received: \"%s\" (id=%d) — withholding PUBACK intentionally.%n",
                new String(msg1.getPayload(), StandardCharsets.UTF_8), msg1.getId());
        System.out.println("             (Normally, calling messageArrivedComplete(id, qos) here would");
        System.out.println("              send the PUBACK and ack the message. We skip it on purpose.)");
        System.out.println("             (Ack-timeout path: if we stayed connected and never called");
        System.out.println("              messageArrivedComplete(), KubeMQ would NAck and redeliver");
        System.out.println("              after QueueAckTimeoutSeconds — 30 s by default.)");

        // ------------------------------------------------------------------
        // Step 5: Consumer-1 disconnects without acking → immediate NAck → requeue
        // ------------------------------------------------------------------
        System.out.println("[Step 5] Consumer-1 disconnecting (unacked message → immediate NAck → requeue) ...");
        // Disconnect cleanly (reason code 0). The broker's OnDisconnect hook fires
        // nackPendingForClient, which issues NAckRange for any unacked inflight
        // messages on this client — the message is immediately requeued with no loss.
        consumer1.disconnect().waitForCompletion(5_000);
        consumer1.close();
        System.out.println("[Consumer-1] disconnected.");

        // ------------------------------------------------------------------
        // Step 6: Consumer-2 receives the requeued message and auto-acks it
        // ------------------------------------------------------------------
        System.out.println("[Step 6] Waiting for Consumer-2 to receive the redelivered message ...");
        MqttMessage msg2 = cb2.inbox.poll(15, TimeUnit.SECONDS);
        if (msg2 == null) {
            System.err.println("ERROR: Consumer-2 did not receive the redelivered message within 15 s.");
            consumer2.disconnect().waitForCompletion(5_000);
            consumer2.close();
            System.exit(1);
        }
        System.out.printf("[Consumer-2] received (redelivered): \"%s\" — PUBACK sent automatically.%n",
                new String(msg2.getPayload(), StandardCharsets.UTF_8));
        System.out.println("             Message is now acked and removed from the queue.");

        // ------------------------------------------------------------------
        // Step 7: Cleanup
        // ------------------------------------------------------------------
        consumer2.disconnect().waitForCompletion(5_000);
        consumer2.close();

        System.out.println();
        System.out.println("=== Done: at-least-once delivery demonstrated ===");
        System.out.println("  1. Message produced to queues/" + channel);
        System.out.println("  2. Consumer-1 received it, withheld PUBACK, disconnected");
        System.out.println("  3. KubeMQ immediately requeued (NAck on disconnect)");
        System.out.println("  4. Consumer-2 received the redelivered message and acked it");
        System.out.println("     (no message loss; at-least-once guarantee fulfilled)");
    }
}

// Expected output:
// === queues/ack-and-redelivery ===
// Broker:        tcp://127.0.0.1:1883
// Produce topic: queues/demo/rq
// Consume topic: $share/g1/queues/demo/rq
//
// [Step 1] Connecting Consumer-1 (manual ack — will withhold PUBACK) ...
// [Consumer-1] connected to tcp://127.0.0.1:1883 (reconnect=false)
// [Consumer-1] subscribed to $share/g1/queues/demo/rq (QoS 1, manual ack)
// [Step 2] Connecting Consumer-2 (auto ack) ...
// [Consumer-2] connected to tcp://127.0.0.1:1883 (reconnect=false)
// [Consumer-2] subscribed to $share/g1/queues/demo/rq (QoS 1, auto ack)
// [Step 3] Producing one message to queues/demo/rq ...
// [Producer]    PUBACK received — message enqueued.
// [Step 4] Waiting for Consumer-1 to receive the message ...
// [Consumer-1] messageArrived: topic=$share/g1/queues/demo/rq  payload=requeue-me  id=1  qos=1
// [Consumer-1] received: "requeue-me" (id=1) — withholding PUBACK intentionally.
//              (Normally, calling messageArrivedComplete(id, qos) here would
//               send the PUBACK and ack the message. We skip it on purpose.)
//              (Ack-timeout path: if we stayed connected and never called
//               messageArrivedComplete(), KubeMQ would NAck and redeliver
//               after QueueAckTimeoutSeconds — 30 s by default.)
// [Step 5] Consumer-1 disconnecting (unacked message → immediate NAck → requeue) ...
// [Consumer-1] disconnected.
// [Step 6] Waiting for Consumer-2 to receive the redelivered message ...
// [Consumer-2] messageArrived: topic=$share/g1/queues/demo/rq  payload=requeue-me  id=1  qos=1
// [Consumer-2] received (redelivered): "requeue-me" — PUBACK sent automatically.
//              Message is now acked and removed from the queue.
//
// === Done: at-least-once delivery demonstrated ===
//   1. Message produced to queues/demo/rq
//   2. Consumer-1 received it, withheld PUBACK, disconnected
//   3. KubeMQ immediately requeued (NAck on disconnect)
//   4. Consumer-2 received the redelivered message and acked it
//      (no message loss; at-least-once guarantee fulfilled)
