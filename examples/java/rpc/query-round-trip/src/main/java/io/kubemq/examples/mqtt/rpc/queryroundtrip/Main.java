/**
 * Example: rpc/query-round-trip
 *
 * Demonstrates a KubeMQ Query RPC round-trip over MQTT 5.0.
 *
 * RPC requires MQTT 5.0. Publishing to queries/ with MQTT 3.1.1 is silently
 * dropped — the PUBACK is still acknowledged at the wire level but the request
 * never reaches a responder (no response topic header is available in v3.1.1).
 *
 * Wire protocol (MQTT 5.0 only):
 *   1. Client SUBSCRIBES to its own reply inbox: $reply/<clientID>/inbox  QoS 1
 *   2. Client PUBLISHES to queries/demo/rpc  QoS 1 with:
 *        Properties.ResponseTopic  = "$reply/<own-clientID>/inbox"
 *        Properties.CorrelationData = <correlation bytes>
 *        payload                   = query body
 *   3. Broker sends PUBACK IMMEDIATELY on receipt — NOT after the response.
 *      The client MUST implement its own response-wait timeout.
 *   4. A gRPC-side responder processes the query and sends a response.
 *   5. Broker delivers response to $reply/<clientID>/inbox:
 *        payload           = query reply body
 *        user-properties   = kubemq-metadata + any tags set by the responder
 *        CorrelationData   = echoed from the request
 *
 * Canonical topics:
 *   Request:  queries/demo/rpc  (KubeMQ channel demo.rpc)
 *   Reply:    $reply/<clientID>/inbox  (mochi-local, never bridged/stored)
 *
 * MQTT clients CANNOT be RPC responders (SUBACK 0x83 on subscribe to
 * commands/ or queries/). A gRPC-side responder is required. The test stage
 * starts the shared test-harness responder:
 *   .work/test-harness/responder/ -channel demo.rpc -type query
 *
 * Relevant MQTT 5.0 reason codes:
 *   0x83 — foreign $reply namespace used in ResponseTopic (broker rejects)
 *   0x97 — RpcMaxPending quota exceeded
 *
 * KUBEMQ_MQTT_URL (default tcp://localhost:1883) selects the broker and
 * transport scheme (tcp/tls/ws).
 *
 * Run: mvn compile exec:java
 */
package io.kubemq.examples.mqtt.rpc.queryroundtrip;

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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {

    // ---------------------------------------------------------------------------
    // Broker URL from environment (scheme drives transport selection).
    // ---------------------------------------------------------------------------
    static String brokerUrl() {
        String url = System.getenv("KUBEMQ_MQTT_URL");
        return (url != null && !url.isEmpty()) ? url : "tcp://localhost:1883";
    }

    public static void main(String[] args) throws Exception {

        String brokerUrl = brokerUrl();

        // The KubeMQ channel is demo.rpc; MQTT topic prefix 'queries/' selects the
        // Queries (RPC) pattern. '/' in the topic becomes '.' in the KubeMQ channel,
        // so queries/demo/rpc -> KubeMQ channel demo.rpc.
        String clientId   = "java-mqtt-query-client-" + UUID.randomUUID().toString().substring(0, 6);
        String replyTopic = "$reply/" + clientId + "/inbox";
        String queryTopic = "queries/demo/rpc";
        byte[] correlationData = "corr-123".getBytes(StandardCharsets.UTF_8);
        byte[] queryPayload    = "ping".getBytes(StandardCharsets.UTF_8);

        // Latch for the single expected response.
        BlockingQueue<MqttMessage> responseQueue = new ArrayBlockingQueue<>(1);

        System.out.println("[rpc/query-round-trip] connecting to " + brokerUrl + " as " + clientId);

        MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        client.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) {
                System.out.println("[response] arrived on topic: " + topic);
                responseQueue.offer(message);
            }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("[connection] " + (reconnect ? "reconnected" : "connected") + " to " + serverURI);
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) {}

            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                System.out.println("[connection] disconnected: returnCode=" + disconnectResponse.getReturnCode());
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                System.err.println("[error] " + exception.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttToken token) {}
        });

        // Connect with MQTT 5.0 (MqttConnectionOptions defaults to v5).
        MqttConnectionOptions opts = new MqttConnectionOptions();
        opts.setCleanStart(true);
        opts.setKeepAliveInterval(30);
        opts.setAutomaticReconnect(false);

        IMqttToken connectToken = client.connect(opts);
        connectToken.waitForCompletion(5_000);
        System.out.println("[step 1] connected (MQTT 5.0)");

        // ---------------------------------------------------------------------------
        // Step 1: Subscribe to own reply inbox BEFORE publishing.
        // The $reply/<clientID>/... namespace is mochi-local and is NEVER bridged
        // or stored. Use own namespace only — a foreign namespace causes PUBACK 0x83.
        // ---------------------------------------------------------------------------
        System.out.println("[step 2] subscribing to reply inbox: " + replyTopic);
        IMqttToken subToken = client.subscribe(replyTopic, 1);
        subToken.waitForCompletion(5_000);

        int[] grantedQoS = subToken.getGrantedQos();
        int grantedCode  = (grantedQoS != null && grantedQoS.length > 0) ? grantedQoS[0] : -1;
        if (grantedCode == 0x83) {
            System.err.println("[error] SUBACK 0x83 — subscription rejected (foreign $reply namespace or other impl-specific error)");
            client.disconnect().waitForCompletion(3_000);
            client.close();
            return;
        }
        System.out.println("[step 2] subscribed to reply inbox (grantedQoS=" + grantedCode + ")");

        // Short settle pause so the broker registers the subscription before publish.
        Thread.sleep(200);

        // ---------------------------------------------------------------------------
        // Step 2: Publish the query to queries/demo/rpc with MQTT 5.0 properties.
        // ---------------------------------------------------------------------------
        MqttProperties pubProps = new MqttProperties();
        pubProps.setResponseTopic(replyTopic);
        pubProps.setCorrelationData(correlationData);

        MqttMessage queryMsg = new MqttMessage(queryPayload);
        queryMsg.setQos(1);
        queryMsg.setRetained(false);   // Retain is forced off (RetainAvailable=0). A runtime retained
                                       // publish is NOT rejected — PUBACK still succeeds (0x00) but the
                                       // message is silently dropped and a publish.error is audited.
                                       // (CONNACK 0x9A applies only to Will-retain at CONNECT.)
        queryMsg.setProperties(pubProps);

        System.out.println("[step 3] publishing query to: " + queryTopic);
        System.out.println("         payload:         " + new String(queryPayload, StandardCharsets.UTF_8));
        System.out.println("         ResponseTopic:   " + replyTopic);
        System.out.println("         CorrelationData: " + new String(correlationData, StandardCharsets.UTF_8));

        IMqttToken pubToken = client.publish(queryTopic, queryMsg);
        pubToken.waitForCompletion(5_000);

        // PUBACK is sent IMMEDIATELY on receipt by the broker — before the gRPC
        // responder has processed or replied. This is NOT a signal that the
        // response has been received; the client MUST implement its own timeout.
        System.out.println("[step 4] PUBACK received (immediate — response is NOT yet here; waiting up to 10 s)");

        // ---------------------------------------------------------------------------
        // Step 3: Wait for the response with a client-side timeout.
        // ---------------------------------------------------------------------------
        MqttMessage response = responseQueue.poll(10, TimeUnit.SECONDS);

        if (response == null) {
            System.err.println("[timeout] no response received within 10 s");
            System.err.println("          Ensure the gRPC responder is running:");
            System.err.println("            .work/test-harness/responder -channel demo.rpc -type query");
            client.disconnect().waitForCompletion(3_000);
            client.close();
            return;
        }

        // ---------------------------------------------------------------------------
        // Step 4: Print the response payload, CorrelationData echo, and user props.
        // ---------------------------------------------------------------------------
        System.out.println("[step 5] response received:");
        System.out.println("         body:            " + new String(response.getPayload(), StandardCharsets.UTF_8));

        MqttProperties respProps = response.getProperties();
        if (respProps != null) {
            byte[] echoedCorr = respProps.getCorrelationData();
            if (echoedCorr != null) {
                String echoed = new String(echoedCorr, StandardCharsets.UTF_8);
                boolean matches = java.util.Arrays.equals(echoedCorr, correlationData);
                System.out.println("         CorrelationData: " + echoed + " (echoed=" + matches + ")");
            }

            List<UserProperty> userProps = respProps.getUserProperties();
            if (userProps != null && !userProps.isEmpty()) {
                System.out.println("         user-properties:");
                for (UserProperty up : userProps) {
                    System.out.println("           " + up.getKey() + " = " + up.getValue());
                }
            }
        }

        // ---------------------------------------------------------------------------
        // Clean disconnect.
        // ---------------------------------------------------------------------------
        System.out.println("[step 6] disconnecting");
        client.disconnect().waitForCompletion(3_000);
        client.close();
        System.out.println("[done] query round-trip completed successfully");
    }
}
