/**
 * Example: rpc/command-round-trip
 *
 * Demonstrates MQTT 5.0 Command (fire-with-ack RPC) round-trip via KubeMQ.
 *
 * Wire contract (MQTT 5.0 ONLY — v3.1.1 publishes to commands/ are silently dropped):
 *   1. Client SUBSCRIBES to its own $reply/<clientID>/inbox BEFORE publishing.
 *   2. Client PUBLISHES to commands/demo/cmd with:
 *        Properties.ResponseTopic = "$reply/<own-clientID>/inbox"  (MUST be own namespace)
 *        Properties.CorrelationData = <opaque bytes>
 *   3. PUBACK is sent IMMEDIATELY on receipt (NOT after command execution).
 *      The client MUST implement its own response-wait timeout.
 *   4. A gRPC-side responder (via kubemq-go) handles the command on channel demo.cmd
 *      and sends a CommandResponse back through the broker.
 *   5. Response arrives on $reply/...: NO body; user-properties contain:
 *        - kubemq-executed = "true" | "false"
 *        - kubemq-error    = "<error text>" (only when executed=false)
 *      CorrelationData is echoed on the response.
 *
 * MQTT clients CANNOT register as RPC responders (SUBACK 0x83). The gRPC test
 * responder at .work/test-harness/responder/ must be running on channel demo.cmd:
 *   go run .work/test-harness/responder/ -channel demo.cmd -type command
 *
 * RPC reason codes to watch:
 *   0x83 PUBACK — ResponseTopic rejected (must be in own $reply/<clientID>/... namespace)
 *   0x97 PUBACK — RpcMaxPending quota exceeded (too many in-flight RPC requests)
 *   0x83 SUBACK — RPC subscribe by MQTT client rejected (not a valid responder role)
 *
 * Topic grammar: commands/<ch> -> KubeMQ channel demo.cmd ('/' becomes '.' in channel name).
 *
 * Run: npx tsx rpc/command-round-trip/index.ts
 */
import mqtt, { type MqttClient } from 'mqtt';
import crypto from 'node:crypto';

function brokerUrl(): string {
  return process.env['KUBEMQ_MQTT_URL'] ?? 'tcp://localhost:1883';
}

// Use a unique client ID so the $reply namespace is unambiguous per run.
const CLIENT_ID = `js-mqtt-cmd-${crypto.randomBytes(4).toString('hex')}`;
// Reply topic MUST be in the client's own $reply/<clientID>/... namespace.
// The broker enforces this: a ResponseTopic in a foreign namespace → PUBACK 0x83.
const REPLY_TOPIC = `$reply/${CLIENT_ID}/inbox`;
// commands/demo/cmd → KubeMQ channel demo.cmd (topic '/' becomes '.' in the channel name).
const COMMAND_TOPIC = 'commands/demo/cmd';
// Unique correlation ID to match this request's response.
const CORRELATION_DATA = Buffer.from(crypto.randomUUID());
// Self-imposed response timeout. The broker sends PUBACK immediately; the actual
// command response arrives later as a separate PUBLISH from the broker once the
// gRPC responder executes the command. 30 s matches the server default
// CONNECTORSMQTT_RPC_TIMEOUT_SECONDS.
const RPC_TIMEOUT_MS = 30_000;

async function main(): Promise<void> {
  const url = brokerUrl();
  console.log(`[client] connecting to ${url} as ${CLIENT_ID} (MQTT 5.0)`);

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

  // Step 1: Subscribe to OWN reply topic BEFORE publishing.
  // If done after the publish the response could arrive and be missed.
  console.log(`[client] Step 1: subscribing to reply topic ${REPLY_TOPIC}`);
  await new Promise<void>((resolve, reject) => {
    client.subscribe(REPLY_TOPIC, { qos: 1 }, (err, granted) => {
      if (err) { reject(err); return; }
      const code: number = (granted?.[0]?.qos as number) ?? -1;
      if (code > 2) {
        const msg = code === 0x83
          ? 'SUBACK 0x83: reply subscribe rejected — topic must be in $reply/<own-clientID>/... namespace'
          : `Reply subscribe rejected: 0x${code.toString(16)}`;
        reject(new Error(msg));
        return;
      }
      console.log(`[client]   subscribed (granted QoS ${code})`);
      resolve();
    });
  });

  // Step 2: Arm the response listener BEFORE publishing (avoid a race).
  // Command response semantics differ from query:
  //   - Payload: empty (NO body)
  //   - User-Property kubemq-executed: "true" if the command succeeded, "false" otherwise
  //   - User-Property kubemq-error: present only when executed=false
  //   - CorrelationData: echoed verbatim from the original publish
  const responsePromise = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(
        `[client] RPC timeout (${RPC_TIMEOUT_MS}ms) — is a gRPC command responder running on channel demo.cmd?\n` +
        '  Start it with: go run .work/test-harness/responder/ -channel demo.cmd -type command',
      )),
      RPC_TIMEOUT_MS,
    );

    client.on('message', (topic, payload, packet) => {
      if (topic !== REPLY_TOPIC) return;
      clearTimeout(timer);

      const props = packet.properties as {
        correlationData?: Buffer;
        userProperties?: Record<string, string | string[]>;
      } | undefined;

      const echoedCorr = props?.correlationData?.toString() ?? '(none)';
      const userProps = (props?.userProperties ?? {}) as Record<string, string>;

      // Command response: NO body. Execution outcome in user-props.
      const bodyStr = payload.length > 0
        ? payload.toString()
        : '(empty — expected for command responses)';
      const executed = userProps['kubemq-executed'] ?? 'unknown';
      const errorMsg = userProps['kubemq-error'] ?? '(none)';
      const corrMatch = echoedCorr === CORRELATION_DATA.toString();

      console.log(`[client] Step 4: command response received on ${topic}`);
      console.log(`[client]   payload (should be empty): ${bodyStr}`);
      console.log(`[client]   kubemq-executed:           ${executed}`);
      if (userProps['kubemq-error']) {
        console.log(`[client]   kubemq-error:              ${errorMsg}`);
      }
      console.log(`[client]   correlationData echoed:    ${echoedCorr} (match=${corrMatch})`);

      // Log all user-properties (includes any extra tags from the responder).
      const allProps = Object.entries(userProps);
      if (allProps.length > 0) {
        console.log('[client]   all user-properties:');
        for (const [k, v] of allProps) {
          console.log(`[client]     ${k} = ${v}`);
        }
      }

      if (executed === 'true') {
        console.log('[client] Command executed successfully.');
      } else {
        console.warn(`[client] Command NOT executed. Error: ${errorMsg}`);
      }
      resolve();
    });
  });

  // Step 3: Publish the command with ResponseTopic and CorrelationData.
  const cmdPayload = JSON.stringify({ action: 'restart', target: 'service-a', requestedBy: 'mqtt-client' });
  console.log(`[client] Step 3: publishing command to ${COMMAND_TOPIC}`);
  console.log(`[client]   payload: ${cmdPayload}`);
  console.log(`[client]   correlationData: ${CORRELATION_DATA.toString()}`);
  await new Promise<void>((resolve, reject) => {
    client.publish(
      COMMAND_TOPIC,
      cmdPayload,
      {
        qos: 1,
        properties: {
          responseTopic: REPLY_TOPIC,
          correlationData: CORRELATION_DATA,
        },
      },
      (err, puback) => {
        if (err) { reject(err); return; }
        // Check PUBACK reason code (MQTT.js surfaces it via callback second arg
        // or via the 'packetsend' event; reason code 0x00 = success).
        const rc = (puback as { reasonCode?: number } | undefined)?.reasonCode ?? 0;
        switch (rc) {
          case 0x00:
            // PUBACK arrives IMMEDIATELY — the broker does NOT wait for the gRPC
            // responder to execute the command before acknowledging the publish.
            // The actual response comes later as a separate PUBLISH to $reply/...
            console.log('[client]   PUBACK received immediately (execution outcome pending on reply topic)');
            console.log('[client] Step 3 done: waiting for command response...');
            resolve();
            break;
          case 0x80:
            reject(new Error('PUBACK 0x80: broker not ready'));
            break;
          case 0x83:
            reject(new Error('PUBACK 0x83: ResponseTopic rejected — must be in own $reply/<clientID>/... namespace'));
            break;
          case 0x97:
            reject(new Error('PUBACK 0x97: RpcMaxPending quota exceeded — too many in-flight RPC requests'));
            break;
          default:
            if (rc > 0) {
              reject(new Error(`PUBACK reason=0x${rc.toString(16)}`));
            } else {
              console.log('[client]   PUBACK received immediately (execution outcome pending on reply topic)');
              console.log('[client] Step 3 done: waiting for command response...');
              resolve();
            }
        }
      },
    );
  });

  await responsePromise;

  await client.endAsync();
  console.log('[done] Disconnected cleanly.');
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});

// Expected output (with a running gRPC command responder on channel demo.cmd):
// [client] connecting to tcp://127.0.0.1:1883 as js-mqtt-cmd-<hex> (MQTT 5.0)
// [client] connected (MQTT 5.0)
// [client] Step 1: subscribing to reply topic $reply/js-mqtt-cmd-<hex>/inbox
// [client]   subscribed (granted QoS 1)
// [client] Step 3: publishing command to commands/demo/cmd
// [client]   payload: {"action":"restart","target":"service-a","requestedBy":"mqtt-client"}
// [client]   correlationData: <uuid>
// [client]   PUBACK received immediately (execution outcome pending on reply topic)
// [client] Step 3 done: waiting for command response...
// [client] Step 4: command response received on $reply/js-mqtt-cmd-<hex>/inbox
// [client]   payload (should be empty): (empty — expected for command responses)
// [client]   kubemq-executed:           true
// [client]   correlationData echoed:    <uuid> (match=true)
// [client]   all user-properties:
// [client]     kubemq-executed = true
// [client] Command executed successfully.
// [done] Disconnected cleanly.
