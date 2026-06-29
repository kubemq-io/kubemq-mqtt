/**
 * Example: connectivity/auth-jwt
 *
 * Demonstrates connecting to KubeMQ's MQTT broker using JWT authentication.
 * The JWT token is placed in the MQTT CONNECT Password field (PasswordFlag=true).
 * The Username field is set to a display-only sentinel value and is NOT used for
 * authentication by the KubeMQ MQTT connector.
 *
 * Wire contract (auth):
 *   - CONNECT Password  = KubeMQ JWT token (bytes of the token string)
 *   - CONNECT Username  = display-only; NOT used for auth (any value or empty)
 *   - PasswordFlag must be true (Paho sets it automatically when password != null)
 *   - Auth-enabled broker: empty/invalid password -> CONNACK 0x86 (bad auth)
 *   - Auth-enabled broker: valid JWT but ACL deny -> PUBACK/SUBACK 0x87 (not authorized)
 *
 * LIVE BROKER NOTE: Auth is DISABLED on the test broker (tcp://127.0.0.1:1883).
 * The CONNECT succeeds regardless of the token value. Full JWT enforcement
 * (CONNACK 0x86 on invalid token) requires an auth-enabled broker.
 * This example is therefore marked partial/N/A for live enforcement.
 *
 * After connecting, the example performs a round-trip pub/sub on the MQTT topic
 * "events/demo/auth" (KubeMQ channel: "demo.auth") to prove the authenticated
 * connection is functional.
 *
 * Environment variables:
 *   KUBEMQ_MQTT_URL   — broker URL; default tcp://localhost:1883
 *   KUBEMQ_MQTT_TOKEN — JWT token string; default "demo-token" (harmless on open broker)
 *
 * Run: mvn compile exec:java
 */
package io.kubemq.examples.mqtt.connectivity.authjwt;

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
    // Broker URL — read from KUBEMQ_MQTT_URL; default tcp://localhost:1883.
    // Supported schemes: tcp://, tls://, ws://
    // NOTE: In production, always use tls:// when sending JWTs — the token
    //       travels in the CONNECT Password field in cleartext at the MQTT layer.
    //       Sending a JWT over unencrypted TCP exposes the credential on the wire.
    // -------------------------------------------------------------------------
    static String brokerUrl() {
        String url = System.getenv("KUBEMQ_MQTT_URL");
        return (url != null && !url.isEmpty()) ? url : "tcp://localhost:1883";
    }

    // -------------------------------------------------------------------------
    // JWT token — read from KUBEMQ_MQTT_TOKEN.
    // On an auth-disabled broker (like the test broker at tcp://127.0.0.1:1883)
    // any value (including the default "demo-token") will connect successfully.
    // On an auth-enabled broker this MUST be a valid KubeMQ-issued JWT.
    // -------------------------------------------------------------------------
    static String jwtToken() {
        String tok = System.getenv("KUBEMQ_MQTT_TOKEN");
        return (tok != null && !tok.isEmpty()) ? tok : "demo-token";
    }

    // -------------------------------------------------------------------------
    // Build MQTT 5.0 connection options with JWT in the Password field.
    // -------------------------------------------------------------------------
    static MqttConnectionOptions connectionOptions(String jwtToken) {
        // org.eclipse.paho.mqttv5.client is always MQTT 5.0 — no setMqttVersion() needed.
        MqttConnectionOptions opts = new MqttConnectionOptions();
        opts.setCleanStart(true);
        opts.setKeepAliveInterval(30);                        // seconds
        opts.setConnectionTimeout(10);                        // seconds
        opts.setAutomaticReconnect(false);

        // Auth: JWT token in the Password field.
        // Paho automatically sets PasswordFlag=true when password is non-null.
        // Username is display-only in KubeMQ's auth model; set it to a meaningful
        // string for log/audit visibility but it is NOT used for authentication.
        opts.setPassword(jwtToken.getBytes(StandardCharsets.UTF_8));
        opts.setUserName("kubemq-mqtt-java-example");

        return opts;
    }

    // -------------------------------------------------------------------------
    // Main flow
    // -------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        String broker = brokerUrl();
        String token  = jwtToken();

        // MQTT topic -> KubeMQ channel: "events/demo/auth" -> "demo.auth"
        String topic   = "events/demo/auth";
        String payload = "Hello with JWT auth!";

        System.out.println("=== connectivity/auth-jwt (MQTT 5.0) ===");
        System.out.printf("Broker  : %s%n", broker);
        System.out.printf("Topic   : %s  (KubeMQ channel: demo.auth)%n", topic);
        System.out.printf("Token   : %s%n",
                token.equals("demo-token") ? token + "  [default — set KUBEMQ_MQTT_TOKEN for a real JWT]"
                                           : token.substring(0, Math.min(token.length(), 20)) + "...");
        System.out.println();
        System.out.println("NOTE: Auth is DISABLED on the test broker.");
        System.out.println("      JWT enforcement (CONNACK 0x86 on invalid token) requires");
        System.out.println("      an auth-enabled broker. This run demonstrates the setup.");
        System.out.println("      SECURITY: Always use TLS (tls://) in production to protect");
        System.out.println("      the JWT credential in transit (Password travels in cleartext).");
        System.out.println();

        // -----------------------------------------------------------------------
        // Step 1 — Subscribe BEFORE the publisher sends (Events = fire-and-forget;
        //          no persistence, message is lost if no subscriber is present).
        // -----------------------------------------------------------------------
        CountDownLatch received = new CountDownLatch(1);

        String subClientId = "java-auth-sub-" + UUID.randomUUID().toString().substring(0, 8);
        MqttAsyncClient subscriber = new MqttAsyncClient(
                broker, subClientId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        subscriber.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String receivedTopic, MqttMessage message) throws Exception {
                String body = new String(message.getPayload(), StandardCharsets.UTF_8);
                System.out.println("--- Message received ---");
                System.out.printf("  Topic   : %s%n", receivedTopic);
                System.out.printf("  QoS     : %d%n", message.getQos());
                System.out.printf("  Payload : %s%n", body);
                System.out.println("  (Authenticated connection confirmed functional.)");
                received.countDown();
            }

            @Override
            public void disconnected(MqttDisconnectResponse response) {
                System.err.printf("Subscriber disconnected: reasonCode=%d%n",
                        response.getReturnCode());
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

        System.out.println("[1] Connecting subscriber with JWT in Password field ...");
        try {
            subscriber.connect(connectionOptions(token)).waitForCompletion(10_000);
        } catch (MqttException e) {
            int rc = e.getReasonCode();
            System.err.printf("Subscriber CONNECT failed: reasonCode=0x%02X%n", rc);
            if (rc == 0x86) {
                System.err.println("  CONNACK 0x86: bad auth — empty or invalid JWT.");
                System.err.println("  Set KUBEMQ_MQTT_TOKEN to a valid KubeMQ JWT token.");
            } else if (rc == 0x87) {
                System.err.println("  CONNACK 0x87: not authorized — JWT valid but ACL denied.");
            }
            throw e;
        }

        System.out.printf("[2] Subscribing to '%s' at QoS 1 ...%n", topic);
        IMqttToken subToken = subscriber.subscribe(topic, 1);
        subToken.waitForCompletion(10_000);

        int[] grantedQos = subToken.getGrantedQos();
        if (grantedQos == null || grantedQos[0] >= 0x80) {
            int subackCode = grantedQos == null ? 0xFF : grantedQos[0];
            System.err.printf("Subscription rejected; SUBACK reason=0x%02X%n", subackCode);
            if (subackCode == 0x87) {
                System.err.println("  ACL deny: authenticated JWT does not permit subscribe on this channel.");
            }
            subscriber.disconnect();
            subscriber.close();
            System.exit(1);
        }
        System.out.printf("    SUBACK granted QoS=%d%n", grantedQos[0]);

        // -----------------------------------------------------------------------
        // Step 2 — Connect the publisher with the same JWT credential.
        // -----------------------------------------------------------------------
        String pubClientId = "java-auth-pub-" + UUID.randomUUID().toString().substring(0, 8);
        MqttAsyncClient publisher = new MqttAsyncClient(
                broker, pubClientId,
                new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());

        publisher.setCallback(new MqttCallback() {
            @Override
            public void disconnected(MqttDisconnectResponse response) {
                System.err.printf("Publisher disconnected: reasonCode=%d%n",
                        response.getReturnCode());
            }
            @Override public void mqttErrorOccurred(MqttException e) {
                System.err.printf("Publisher MQTT error: %s%n", e.getMessage());
            }
            @Override public void messageArrived(String t, MqttMessage m) { /* N/A */ }
            @Override public void deliveryComplete(IMqttToken t) { /* we use waitForCompletion */ }
            @Override public void connectComplete(boolean reconnect, String serverURI) {
                System.out.printf("Publisher  connected to %s (reconnect=%b)%n", serverURI, reconnect);
            }
            @Override public void authPacketArrived(int reasonCode, MqttProperties properties) { /* N/A */ }
        });

        System.out.println("[3] Connecting publisher with JWT in Password field ...");
        try {
            publisher.connect(connectionOptions(token)).waitForCompletion(10_000);
        } catch (MqttException e) {
            int rc = e.getReasonCode();
            System.err.printf("Publisher CONNECT failed: reasonCode=0x%02X%n", rc);
            if (rc == 0x86) {
                System.err.println("  CONNACK 0x86: bad auth — empty or invalid JWT.");
            }
            subscriber.disconnect();
            subscriber.close();
            throw e;
        }

        // -----------------------------------------------------------------------
        // Step 3 — Publish.
        // NOTE: Do NOT set retain=true — KubeMQ silently drops retained publishes:
        //       PUBACK 0x00 is returned (success) but the message is not delivered.
        //       The RetainAvailable capability is 0 on this broker.
        // -----------------------------------------------------------------------
        MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(1);
        // msg.setRetained(true)  <- NEVER do this; message would be silently dropped.

        System.out.printf("[4] Publishing to '%s' QoS=1, payload='%s' ...%n", topic, payload);
        IMqttToken pubToken = publisher.publish(topic, msg);
        pubToken.waitForCompletion(10_000);

        int pubReasonCode = pubToken.getReasonCodes() != null ? pubToken.getReasonCodes()[0] : -1;
        if (pubReasonCode != 0x00) {
            System.err.printf("Publish rejected; PUBACK reason=0x%02X%n", pubReasonCode);
            if (pubReasonCode == 0x87) {
                System.err.println("  0x87: not authorized — JWT does not permit publish on this channel (ACL).");
            } else if (pubReasonCode == 0x80) {
                System.err.println("  0x80: broker not ready (gated).");
            }
        } else {
            System.out.printf("    PUBACK reason=0x00 (success)%n");
        }

        // -----------------------------------------------------------------------
        // Step 4 — Wait for the subscriber to receive the message (up to 10 s).
        // -----------------------------------------------------------------------
        System.out.println("[5] Waiting for message delivery (up to 10 s) ...");
        boolean delivered = received.await(10, TimeUnit.SECONDS);
        if (!delivered) {
            System.err.println("Timed out waiting for message.");
            System.err.println("Verify the KubeMQ MQTT connector is reachable and the JWT is accepted.");
        } else {
            System.out.println();
            System.out.println("Round-trip complete on authenticated connection.");
            System.out.println("  MQTT topic     : " + topic);
            System.out.println("  KubeMQ channel : demo.auth");
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

// Expected output (auth-disabled test broker, default token):
// === connectivity/auth-jwt (MQTT 5.0) ===
// Broker  : tcp://127.0.0.1:1883
// Topic   : events/demo/auth  (KubeMQ channel: demo.auth)
// Token   : demo-token  [default — set KUBEMQ_MQTT_TOKEN for a real JWT]
//
// NOTE: Auth is DISABLED on the test broker.
//       JWT enforcement (CONNACK 0x86 on invalid token) requires
//       an auth-enabled broker. This run demonstrates the setup.
//       SECURITY: Always use TLS (tls://) in production to protect
//       the JWT credential in transit (Password travels in cleartext).
//
// [1] Connecting subscriber with JWT in Password field ...
// Subscriber connected to tcp://127.0.0.1:1883 (reconnect=false)
// [2] Subscribing to 'events/demo/auth' at QoS 1 ...
//     SUBACK granted QoS=1
// [3] Connecting publisher with JWT in Password field ...
// Publisher  connected to tcp://127.0.0.1:1883 (reconnect=false)
// [4] Publishing to 'events/demo/auth' QoS=1, payload='Hello with JWT auth!' ...
//     PUBACK reason=0x00 (success)
// [5] Waiting for message delivery (up to 10 s) ...
// --- Message received ---
//   Topic   : events/demo/auth
//   QoS     : 1
//   Payload : Hello with JWT auth!
//   (Authenticated connection confirmed functional.)
//
// Round-trip complete on authenticated connection.
//   MQTT topic     : events/demo/auth
//   KubeMQ channel : demo.auth
// [6] Disconnecting ...
// Done.
