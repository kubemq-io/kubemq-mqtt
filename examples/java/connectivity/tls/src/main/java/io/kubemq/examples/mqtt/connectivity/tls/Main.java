/**
 * Example: connectivity/tls
 *
 * Demonstrates connecting to KubeMQ's MQTT broker over TLS (tls:// scheme)
 * using MQTT 5.0 (Eclipse Paho MQTTv5 client).
 *
 * The example uses InsecureSkipVerify (all-trusting TrustManager) so it works
 * without provisioning a CA certificate. In production ALWAYS supply the server
 * CA cert (or mutual-TLS keypair) and remove the all-trusting trust manager.
 *
 * After connecting it performs a basic Events pub/sub over the encrypted link:
 *   - Subscribes to topic  events/demo/tls  at QoS 1
 *   - Publishes one message to the same topic at QoS 1
 *   - Waits to receive the message, then disconnects cleanly
 *
 * LIVE BROKER NOTE: The TLS listener on the test broker (tls://127.0.0.1:8883)
 * is CLOSED — this example compiles correctly and demonstrates the full setup,
 * but end-to-end verification requires a TLS-enabled KubeMQ broker. The live
 * test is therefore marked N/A on the reference broker.
 *
 * Env vars:
 *   KUBEMQ_MQTT_URL  — broker URL (default tls://localhost:8883)
 *                      Must use the tls:// scheme for TLS transport.
 *
 * Run:
 *   cd examples/java/connectivity/tls
 *   KUBEMQ_MQTT_URL=tls://127.0.0.1:8883 mvn compile exec:java
 */
package io.kubemq.examples.mqtt.connectivity.tls;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Main {

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    static String brokerUrl() {
        String url = System.getenv("KUBEMQ_MQTT_URL");
        if (url != null && !url.isEmpty()) return url;
        return "tls://localhost:8883";
    }

    /**
     * Normalise a broker URL so it is acceptable to the Paho v5 client.
     *
     * Paho v5's NetworkModuleService only registers the schemes "tcp://",
     * "ssl://", "ws://", and "wss://".  KubeMQ advertises TLS endpoints with
     * the "tls://" scheme — passing that verbatim to MqttAsyncClient throws
     * "no NetworkModule installed for scheme tls".  Map "tls://" -> "ssl://"
     * (the Paho scheme for TLS) so the URL is valid.  We keep the original
     * tls:// form for logging.
     */
    static String normaliseBrokerUrl(String url) {
        if (url.startsWith("tls://")) {
            return "ssl://" + url.substring("tls://".length());
        }
        return url;
    }

    // KubeMQ MQTT topic for Events pattern: events/<channel>
    // '/' in the topic becomes '.' in the KubeMQ channel name.
    static final String TOPIC = "events/demo/tls";

    static final int QOS = 1;

    // -------------------------------------------------------------------------
    // SSL context: InsecureSkipVerify (dev/demo only)
    //
    // PRODUCTION NOTE: Replace this with a proper SSLContext that loads
    // the server CA certificate (or the full mutual-TLS keypair) using a
    // real TrustManagerFactory backed by a KeyStore. Example:
    //
    //   KeyStore ts = KeyStore.getInstance("JKS");
    //   ts.load(new FileInputStream("truststore.jks"), "password".toCharArray());
    //   TrustManagerFactory tmf = TrustManagerFactory.getInstance(
    //       TrustManagerFactory.getDefaultAlgorithm());
    //   tmf.init(ts);
    //   SSLContext ctx = SSLContext.getInstance("TLSv1.3");
    //   ctx.init(null, tmf.getTrustManagers(), null);
    // -------------------------------------------------------------------------

    static SSLContext insecureSslContext() throws Exception {
        TrustManager[] trustAll = new TrustManager[]{
            new X509TrustManager() {
                @Override public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                @Override public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }
        };
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, trustAll, new java.security.SecureRandom());
        return ctx;
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        String rawBroker = brokerUrl();
        // Paho v5 has no "tls" NetworkModule — map tls:// -> ssl:// for the client
        // while still showing the original tls:// URL in the logs for clarity.
        String broker = normaliseBrokerUrl(rawBroker);
        String clientId = "kubemq-mqtt-tls-java-" + UUID.randomUUID().toString().substring(0, 8);

        System.out.println("[connectivity/tls] Connecting to: " + rawBroker
                + (broker.equals(rawBroker) ? "" : "  (Paho scheme: " + broker + ")"));
        System.out.printf("[connectivity/tls] ClientID: %s%n", clientId);
        System.out.println("[connectivity/tls] Topic: " + TOPIC + "  QoS: " + QOS);
        System.out.println("[connectivity/tls] SSL: InsecureSkipVerify=true  " +
                           "(production: supply a trusted CA cert)");

        // Build connection options with TLS socket factory
        MqttConnectionOptions opts = new MqttConnectionOptions();
        opts.setCleanStart(true);
        opts.setKeepAliveInterval(30);
        opts.setConnectionTimeout(10);
        // Attach the SSLSocketFactory so Paho uses TLS for the tls:// URL
        opts.setSocketFactory(insecureSslContext().getSocketFactory());

        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<String> receivedPayload = new AtomicReference<>();
        AtomicReference<MqttException> fatalError = new AtomicReference<>();

        MqttAsyncClient client = new MqttAsyncClient(broker, clientId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        client.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                System.out.printf("[connectivity/tls] Received on topic=%s  payload=%s  QoS=%d%n",
                        topic, payload, message.getQos());
                receivedPayload.set(payload);
                received.countDown();
            }

            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                // Reason code 0x8B = server shutting down; others may indicate errors.
                int rc = disconnectResponse.getReturnCode();
                if (rc != 0) {
                    System.err.printf("[connectivity/tls] Disconnected unexpectedly: " +
                                      "reasonCode=0x%02X  msg=%s%n",
                            rc, disconnectResponse.getReasonString());
                }
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                System.err.println("[connectivity/tls] MQTT error: " + exception.getMessage());
                fatalError.set(exception);
                received.countDown(); // unblock if waiting
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                // Invoked when a QoS>=1 publish is acknowledged by the broker.
            }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.printf("[connectivity/tls] %s to: %s%n",
                        reconnect ? "Reconnected" : "Connected", serverURI);
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) {}
        });

        // Step 1: Connect
        IMqttToken connToken = client.connect(opts);
        connToken.waitForCompletion(10_000);
        // connectComplete callback fires here; additional CONNACK detail:
        System.out.printf("[connectivity/tls] CONNACK: reasonCode=0x%02X (0x00=success)%n",
                connToken.getReasonCodes() != null && connToken.getReasonCodes().length > 0
                        ? connToken.getReasonCodes()[0] : 0);

        // Step 2: Subscribe to events/demo/tls at QoS 1
        // Only Events subscriptions accept wildcards; plain single-channel sub is always valid.
        System.out.println("[connectivity/tls] Subscribing to: " + TOPIC + " at QoS " + QOS);
        IMqttToken subToken = client.subscribe(TOPIC, QOS);
        subToken.waitForCompletion(5_000);
        int[] subReasonCodes = subToken.getReasonCodes();
        int subRc = (subReasonCodes != null && subReasonCodes.length > 0) ? subReasonCodes[0] : -1;
        System.out.printf("[connectivity/tls] SUBACK: reasonCode=0x%02X  " +
                          "(0x01=QoS1 granted; 0x80=failure; 0xA2=wildcard not supported)%n", subRc);
        if (subRc == 0x80 || subRc == 0x83 || subRc == 0x8F || subRc == 0x90) {
            System.err.printf("[connectivity/tls] Subscribe rejected (reasonCode=0x%02X). " +
                              "Check broker config and topic format.%n", subRc);
            client.disconnect().waitForCompletion(3_000);
            client.close();
            System.exit(1);
        }

        // Small pause to ensure subscription is registered before publishing
        Thread.sleep(200);

        // Step 3: Publish to events/demo/tls at QoS 1
        // KubeMQ maps this topic to channel "demo.tls" in the Events pattern.
        String payload = "Hello via TLS from Java MQTT 5.0! timestamp=" + System.currentTimeMillis();
        MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(QOS);
        msg.setRetained(false); // KubeMQ MQTT: RetainAvailable=0; retained publishes are silently dropped.
        System.out.printf("[connectivity/tls] Publishing: topic=%s  payload=%s  QoS=%d%n",
                TOPIC, payload, QOS);
        IMqttToken pubToken = client.publish(TOPIC, msg);
        pubToken.waitForCompletion(5_000);
        System.out.printf("[connectivity/tls] PUBACK received: reasonCode=0x%02X " +
                          "(0x00=success; 0x80=broker not ready; 0x97=quota exceeded)%n",
                pubToken.getReasonCodes() != null && pubToken.getReasonCodes().length > 0
                        ? pubToken.getReasonCodes()[0] : 0);

        // Step 4: Wait for the message to arrive back on the subscriber
        System.out.println("[connectivity/tls] Waiting for message delivery (max 5 s)...");
        boolean arrived = received.await(5, TimeUnit.SECONDS);
        if (!arrived || fatalError.get() != null) {
            String reason = fatalError.get() != null ? fatalError.get().getMessage() : "timeout";
            System.err.println("[connectivity/tls] Did not receive message: " + reason);
            System.err.println("[connectivity/tls] NOTE: If the TLS listener is closed on this " +
                               "broker, this is expected — test is N/A.");
        } else {
            System.out.println("[connectivity/tls] Round-trip complete over TLS.");
        }

        // Step 5: Clean disconnect
        System.out.println("[connectivity/tls] Disconnecting...");
        client.disconnect().waitForCompletion(3_000);
        client.close();
        System.out.println("[connectivity/tls] Disconnected cleanly.");
    }
}
