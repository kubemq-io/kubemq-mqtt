/**
 * Example: rpc/query-round-trip
 *
 * Demonstrates RPC Queries over MQTT 5.0. MQTT clients CANNOT be RPC responders
 * (subscribe to queries/<ch> -> SUBACK 0x83). The responder MUST be a gRPC-side
 * KubeMQ client (use the test harness at .work/test-harness/responder/).
 *
 * Flow (S6.1.6 / rpc_bridge.go):
 *   1. Client subscribes to its OWN reply topic: $reply/<clientID>/inbox (QoS 1) FIRST.
 *   2. Client publishes to queries/demo/rpc with:
 *        Properties.ResponseTopic = $reply/<clientID>/inbox   (own namespace — foreign -> PUBACK 0x83)
 *        Properties.CorrelationData = Buffer("query-corr-1")
 *        Properties.UserProperties = { source: "js-example" }  (optional tags)
 *   3. PUBACK arrives IMMEDIATELY on receipt — NOT after the response.
 *      The client MUST implement its own response-wait timeout.
 *   4. When the gRPC responder answers, the response arrives on $reply/<clientID>/inbox:
 *        - body  = the query reply body (non-empty, unlike commands)
 *        - user-props include kubemq-metadata + any tags from the responder
 *        - CorrelationData echoed so the client can match request to response
 *
 * Query response vs Command response:
 *   - Query:   body present + user-props (kubemq-metadata + tags)
 *   - Command: NO body      + user-props kubemq-executed("true"/"false") + optional kubemq-error
 *
 * PREREQUISITE: a gRPC query responder must be running for channel demo.rpc.
 * Start it with:
 *   cd .work/test-harness/responder && go run . -channel demo.rpc -type query
 *
 * Run: npx tsx rpc/query-round-trip/index.ts
 */
import mqtt, { type MqttClient } from 'mqtt';

function brokerUrl(): string {
  return process.env['KUBEMQ_MQTT_URL'] ?? 'tcp://localhost:1883';
}

const CLIENT_ID = 'js-mqtt-query-client';
const REPLY_TOPIC = `$reply/${CLIENT_ID}/inbox`;
const QUERY_TOPIC = 'queries/demo/rpc';
const CORRELATION_DATA = Buffer.from('query-corr-1');
const RPC_TIMEOUT_MS = 30_000;

async function main(): Promise<void> {
  const url = brokerUrl();

  const client: MqttClient = mqtt.connect(url, {
    clientId: CLIENT_ID,
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
  });

  await new Promise<void>((resolve, reject) => {
    client.once('connect', () => resolve());
    client.once('error', reject);
  });

  console.log('[client] connected (MQTT 5.0)');

  // Step 1: subscribe to OWN reply topic BEFORE publishing.
  // Using a foreign $reply namespace (not own clientID) returns PUBACK 0x83.
  await new Promise<void>((resolve, reject) => {
    client.subscribe(REPLY_TOPIC, { qos: 1 }, (err, granted) => {
      if (err) { reject(err); return; }
      const code = granted?.[0]?.qos ?? -1;
      if (code > 2) {
        reject(new Error(`Reply subscribe rejected: 0x${code.toString(16)}`));
        return;
      }
      console.log(`[client] subscribed to reply topic: ${REPLY_TOPIC}`);
      resolve();
    });
  });

  // Step 2: arm the response listener BEFORE publishing.
  // PUBACK is immediate — the response may arrive very quickly after publish;
  // registering the handler first avoids a race window.
  const responsePromise = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`[client] RPC timeout (${RPC_TIMEOUT_MS}ms) — is a gRPC query responder running? (go run .work/test-harness/responder/ -channel demo.rpc -type query)`)),
      RPC_TIMEOUT_MS,
    );

    client.on('message', (topic, payload, packet) => {
      if (topic !== REPLY_TOPIC) return;
      clearTimeout(timer);

      const props = packet.properties as {
        correlationData?: Buffer;
        userProperties?: Record<string, string | string[]>;
      } | undefined;

      const corrEchoed = props?.correlationData?.toString() ?? '(none)';
      const userProps = props?.userProperties ?? {};
      // Query response ALWAYS has a body (unlike commands which have none).
      const bodyStr = payload.length > 0 ? payload.toString() : '(empty body — unexpected for queries)';

      console.log(`[client] query response received on ${topic}`);
      console.log(`[client]   CorrelationData echoed: ${corrEchoed}`);
      console.log(`[client]   body: ${bodyStr}`);
      console.log(`[client]   user-props: ${JSON.stringify(userProps)}`);
      resolve();
    });
  });

  // Step 3: publish the query with ResponseTopic + CorrelationData (MQTT 5.0 only).
  // A v3.1.1 publish to queries/<ch> is silently dropped (PUBACK still succeeds).
  await new Promise<void>((resolve, reject) => {
    client.publish(
      QUERY_TOPIC,
      JSON.stringify({ request: 'hello' }),
      {
        qos: 1,
        properties: {
          responseTopic: REPLY_TOPIC,
          correlationData: CORRELATION_DATA,
          // Optional: v5 User Properties become KubeMQ request tags.
          userProperties: { source: 'js-example' },
        },
      },
      (err) => {
        if (err) { reject(err); return; }
        // PUBACK arrives IMMEDIATELY on receipt, NOT after the gRPC responder replies.
        // The client must wait for the response independently (responsePromise above).
        console.log(`[client] query published to ${QUERY_TOPIC} (PUBACK received immediately — waiting for response)`);
        resolve();
      },
    );
  });

  await responsePromise;

  await client.endAsync();
  console.log('[done]');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output (with a running gRPC query responder on channel demo.rpc):
// [client] connected (MQTT 5.0)
// [client] subscribed to reply topic: $reply/js-mqtt-query-client/inbox
// [client] query published to queries/demo/rpc (PUBACK received immediately — waiting for response)
// [client] query response received on $reply/js-mqtt-query-client/inbox
// [client]   CorrelationData echoed: query-corr-1
// [client]   body: pong:{"request":"hello"}
// [client]   user-props: {"kubemq-metadata":"responder/query","responder":"mqtt-test","rpc-type":"query"}
// [done]
