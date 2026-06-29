/**
 * Example: rpc/command-round-trip
 *
 * Demonstrates a KubeMQ Command RPC round-trip over MQTT 5.0.
 *
 * RPC requires MQTT 5.0. Publishing to commands/ with MQTT 3.1.1 is
 * silently dropped (no error returned to the client, no response arrives).
 *
 * Protocol flow (MQTT 5.0 only):
 *   1. Client SUBSCRIBES to its own reply topic:
 *        $reply/<clientID>/inbox
 *      The $reply/ namespace is mochi-local; other clients cannot subscribe
 *      to your reply topic (SUBACK 0x83 if they try).
 *   2. Client PUBLISHES to commands/demo/cmd with MQTT 5.0 properties:
 *        Properties.ResponseTopic  = $reply/<own-clientID>/inbox
 *        Properties.CorrelationData = <unique bytes per request>
 *      The ResponseTopic MUST be in your own $reply/<clientID>/... namespace;
 *      a foreign namespace yields PUBACK 0x83.
 *   3. Broker sends PUBACK immediately on receipt — before the gRPC-side
 *      responder has answered.  The client MUST implement its own timeout.
 *   4. The gRPC-side responder (started by the test stage) processes the
 *      command and sends a response via the KubeMQ gRPC API.
 *   5. Broker delivers the response to $reply/<clientID>/inbox.
 *      Command response has NO body.  Result is conveyed via user properties:
 *        kubemq-executed = "true"  (command succeeded)
 *        kubemq-executed = "false" + kubemq-error = "<msg>"  (failure)
 *      CorrelationData is echoed verbatim on the response message.
 *
 * Relevant reason codes:
 *   0x83  impl-specific  — missing/foreign ResponseTopic; RPC subscribe attempt
 *   0x97  quota exceeded — RpcMaxPending full
 *   0x80  broker not ready
 *
 * KUBEMQ_MQTT_URL (default tcp://localhost:1883) selects the broker.
 * The MQTT topic is commands/demo/cmd; the KubeMQ channel is demo.cmd
 * (topic '/' maps to '.' in the channel name).
 *
 * Run:
 *   mvn compile exec:java
 */
package io.kubemq.examples.mqtt.rpc.commandroundtrip;

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

    /** Read the broker URL from the environment; default to localhost:1883. */
    static String brokerUrl() {
        String url = System.getenv("KUBEMQ_MQTT_URL");
        return (url != null && !url.isEmpty()) ? url : "tcp://localhost:1883";
    }

    /**
     * Normalise a broker URL so it is acceptable to the Paho v5 client.
     *
     * Paho v5 uses "tcp://", "ssl://", and "ws://" schemes.  KubeMQ examples
     * advertise "tls://" — map that to "ssl://" so the URL is valid.
     */
    static String normaliseBrokerUrl(String url) {
        if (url.startsWith("tls://")) {
            return "ssl://" + url.substring(6);
        }
        return url;
    }

    public static void main(String[] args) throws Exception {
        // ------------------------------------------------------------------ //
        // Configuration
        // ------------------------------------------------------------------ //
        String rawUrl    = brokerUrl();
        String broker    = normaliseBrokerUrl(rawUrl);
        String clientId  = "java-mqtt-cmd-" + UUID.randomUUID().toString().substring(0, 8);

        // MQTT topic: commands/<channel>   KubeMQ channel: demo.cmd
        // Topic '/' maps to '.' in the KubeMQ channel name.
        String commandTopic = "commands/demo/cmd";
        String replyTopic   = "$reply/" + clientId + "/inbox";

        // Correlation bytes — unique per request so the client can match
        // the response to the request in a real application.
        byte[] correlationData = ("cmd-" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);

        // Command payload — arbitrary bytes; the gRPC responder receives this
        // as the command body but the response carries NO body.
        byte[] payload = "execute-action".getBytes(StandardCharsets.UTF_8);

        // Response timeout — the PUBACK arrives before the response, so the
        // client must wait independently.  10 seconds is comfortable for a
        // local broker with a running responder.
        long responseTimeoutSeconds = 10;

        System.out.println("=== rpc/command-round-trip (MQTT 5.0) ===");
        System.out.println("Broker:        " + rawUrl);
        System.out.println("ClientID:      " + clientId);
        System.out.println("Command topic: " + commandTopic);
        System.out.println("Reply topic:   " + replyTopic);
        System.out.println();

        // ------------------------------------------------------------------ //
        // Step 1 — Create an MQTT 5.0 async client
        // ------------------------------------------------------------------ //
        MqttAsyncClient client = new MqttAsyncClient(broker, clientId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        CountDownLatch responseLatch = new CountDownLatch(1);

        // Holder for the response so the callback can pass it to main().
        final String[]   executedFlag = {null};
        final String[]   errorFlag    = {null};
        // byte[][] so the callback can store a byte[] value through the
        // effectively-final single-element array idiom.
        final byte[][]   echoedCorr   = {null};

        client.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                System.out.println("[callback] Response arrived on: " + topic);

                // Command response has NO body — payload should be empty.
                byte[] body = message.getPayload();
                if (body != null && body.length > 0) {
                    System.out.println("[callback] Unexpected body (" + body.length + " bytes): "
                            + new String(body, StandardCharsets.UTF_8));
                } else {
                    System.out.println("[callback] Body: (empty — correct for command response)");
                }

                // Extract user properties: kubemq-executed and optional kubemq-error.
                MqttProperties props = message.getProperties();
                if (props != null) {
                    List<UserProperty> userProps = props.getUserProperties();
                    if (userProps != null) {
                        for (UserProperty up : userProps) {
                            System.out.println("[callback] User property: " + up.getKey() + " = " + up.getValue());
                            if ("kubemq-executed".equals(up.getKey())) {
                                executedFlag[0] = up.getValue();
                            } else if ("kubemq-error".equals(up.getKey())) {
                                errorFlag[0] = up.getValue();
                            }
                        }
                    }

                    // Correlation data must be echoed verbatim.
                    byte[] cd = props.getCorrelationData();
                    if (cd != null) {
                        echoedCorr[0] = cd;
                        System.out.println("[callback] CorrelationData echoed: "
                                + new String(cd, StandardCharsets.UTF_8));
                    }
                }

                responseLatch.countDown();
            }

            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                System.out.println("[callback] Disconnected: " + disconnectResponse.getReasonString());
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                System.err.println("[callback] MQTT error: " + exception.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                // Called when PUBACK/PUBCOMP is received.
                // NOTE: for RPC the PUBACK arrives immediately on receipt at the
                // broker, NOT after the gRPC responder has answered.
                System.out.println("[callback] PUBACK received (immediate — before response)");
            }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("[callback] Connect complete (reconnect=" + reconnect + ")");
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) {
                // Not used in this example.
            }
        });

        // ------------------------------------------------------------------ //
        // Step 2 — Connect with MQTT 5.0
        // ------------------------------------------------------------------ //
        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        connOpts.setCleanStart(true);
        connOpts.setKeepAliveInterval(30);
        // No username/password — auth is disabled on the test broker.

        System.out.println("Connecting to broker ...");
        IMqttToken connToken = client.connect(connOpts);
        connToken.waitForCompletion(5_000);
        System.out.println("Connected.");
        System.out.println();

        // ------------------------------------------------------------------ //
        // Step 3 — Subscribe to the reply inbox BEFORE publishing
        //
        // Gotcha: subscribe FIRST.  If the broker delivers the response before
        // the subscription is established the response is lost and the client
        // times out.  On a fast local broker this is unlikely but required by
        // the RPC flow contract.
        // ------------------------------------------------------------------ //
        System.out.println("Subscribing to reply topic: " + replyTopic);
        IMqttToken subToken = client.subscribe(replyTopic, 1);
        subToken.waitForCompletion(5_000);
        System.out.println("Subscribed (QoS 1).");
        System.out.println();

        // ------------------------------------------------------------------ //
        // Step 4 — Build and publish the command with MQTT 5.0 properties
        // ------------------------------------------------------------------ //
        MqttProperties pubProps = new MqttProperties();
        pubProps.setResponseTopic(replyTopic);
        pubProps.setCorrelationData(correlationData);
        // Optional: tag the request with MQTT 5.0 user properties ->
        // mapped to KubeMQ Tags (up to 32 properties / 4096 bytes total).
        pubProps.setUserProperties(List.of(
                new UserProperty("example", "command-round-trip"),
                new UserProperty("language", "java")
        ));

        MqttMessage pubMsg = new MqttMessage(payload);
        pubMsg.setQos(1);
        pubMsg.setProperties(pubProps);

        System.out.println("Publishing command to: " + commandTopic);
        System.out.println("  Payload:         " + new String(payload, StandardCharsets.UTF_8));
        System.out.println("  ResponseTopic:   " + replyTopic);
        System.out.println("  CorrelationData: " + new String(correlationData, StandardCharsets.UTF_8));
        System.out.println();

        IMqttToken pubToken = client.publish(commandTopic, pubMsg);
        pubToken.waitForCompletion(5_000);
        // PUBACK arrives here — immediately on broker receipt, NOT after the
        // gRPC responder responds.  The deliveryComplete callback fires above.

        // ------------------------------------------------------------------ //
        // Step 5 — Wait for the response with a self-imposed timeout
        //
        // Reason: PUBACK is sent on receipt, so the client must wait
        // independently.  RpcMaxPending exceeded -> 0x97; a missing responder
        // simply means the response never arrives and the client times out.
        // ------------------------------------------------------------------ //
        System.out.println("Waiting up to " + responseTimeoutSeconds + "s for command response ...");
        boolean received = responseLatch.await(responseTimeoutSeconds, TimeUnit.SECONDS);

        // ------------------------------------------------------------------ //
        // Step 6 — Evaluate the result
        // ------------------------------------------------------------------ //
        System.out.println();
        if (!received) {
            System.err.println("ERROR: No response received within " + responseTimeoutSeconds + "s.");
            System.err.println("  Is the gRPC-side command responder running?");
            System.err.println("  Expected: cd .work/test-harness/responder && go run . -channel demo.cmd -type command");
            client.disconnect().waitForCompletion(3_000);
            client.close();
            System.exit(1);
        }

        System.out.println("Command result:");
        System.out.println("  kubemq-executed: " + executedFlag[0]);
        if (errorFlag[0] != null) {
            System.out.println("  kubemq-error:    " + errorFlag[0]);
        }

        // Verify CorrelationData was echoed unchanged.
        String sentCorr   = new String(correlationData, StandardCharsets.UTF_8);
        String echoedStr  = (echoedCorr[0] != null)
                ? new String(echoedCorr[0], StandardCharsets.UTF_8) : "(not present)";
        boolean corrMatch = sentCorr.equals(echoedStr);
        System.out.println("  CorrelationData match: " + corrMatch
                + " (sent=" + sentCorr + ", echoed=" + echoedStr + ")");
        System.out.println();

        if ("true".equals(executedFlag[0]) && corrMatch) {
            System.out.println("SUCCESS: command executed; CorrelationData echoed correctly.");
        } else if ("false".equals(executedFlag[0])) {
            System.out.println("COMMAND FAILED (kubemq-executed=false): " + errorFlag[0]);
        } else {
            System.out.println("WARNING: unexpected response state — executed=" + executedFlag[0]);
        }

        // ------------------------------------------------------------------ //
        // Step 7 — Disconnect cleanly
        // ------------------------------------------------------------------ //
        System.out.println();
        System.out.println("Disconnecting ...");
        client.disconnect().waitForCompletion(3_000);
        client.close();
        System.out.println("Done.");
    }
}
