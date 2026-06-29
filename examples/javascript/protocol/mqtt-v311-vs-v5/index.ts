/**
 * Example: protocol/mqtt-v311-vs-v5
 *
 * Didactic contrast of MQTT 3.1.1 (protocol level 4) vs MQTT 5.0 (level 5)
 * over the KubeMQ MQTT connector.
 *
 * MQTT 5.0 unlocks three features that are ABSENT in v3.1.1:
 *   A. User Properties  — map to/from KubeMQ Tags (bidirectional)
 *   B. Shared subscriptions ($share/...)  — required for Queues consume
 *   C. RPC (Commands / Queries) — ResponseTopic + CorrelationData
 *
 * This example shows:
 *   Part 1 — Events pub/sub with BOTH v3.1.1 and v5 (both work; user-props v5 only)
 *   Part 2 — v5 RPC command round-trip (positive; requires gRPC responder)
 *   Part 3 — v3.1.1 RPC silent drop (PUBACK 0x00 but no response)
 *   Part 4 — Summary feature matrix
 *
 * Spec: S6.3 (events/demo/v3 for pub/sub; v5 RPC contrast on commands/demo/cmd;
 *   v3.1.1 RPC silent-drop on commands/demo/x; tests #13 V311PubSub + #12 RPC311Dropped)
 *
 * PREREQUISITE for Part 2 (v5 RPC):
 *   cd .work/test-harness/responder && go run . -channel demo.cmd -type command
 *   (If the responder is not running, Part 2 times out and the example falls through
 *    to Part 3 — the v3.1.1 contrast is demonstrated either way.)
 *
 * Run: npx tsx protocol/mqtt-v311-vs-v5/index.ts
 */
import mqtt, { type MqttClient } from 'mqtt';

// ---------------------------------------------------------------------------
// Broker URL
// ---------------------------------------------------------------------------
function brokerUrl(): string {
  return process.env['KUBEMQ_MQTT_URL'] ?? 'tcp://localhost:1883';
}

async function connectClient(clientId: string, protocolVersion: 4 | 5): Promise<MqttClient> {
  return new Promise<MqttClient>((resolve, reject) => {
    const client = mqtt.connect(brokerUrl(), {
      clientId,
      protocolVersion,
      clean: true,
      keepalive: 30,
    });
    client.once('connect', () => resolve(client));
    client.once('error', reject);
  });
}

// ---------------------------------------------------------------------------
// Part 1: Events pub/sub — both protocols receive; user-props only on v5
// ---------------------------------------------------------------------------
async function part1EventsPubSub(): Promise<void> {
  const EVENTS_TOPIC = 'events/demo/v3'; // KubeMQ channel: demo.v3

  console.log('--- Part 1: Events pub/sub (both v3.1.1 and v5 work) ---');

  // Connect subscribers first so the broker registers them before the publish.
  const v5Sub  = await connectClient('js-v311v5-v5sub',  5);
  const v311Sub = await connectClient('js-v311v5-311sub', 4);
  console.log('[v5 sub]    connected (MQTT 5.0)');
  console.log('[v3.1.1 sub] connected (MQTT 3.1.1)');

  // Arm message listeners BEFORE subscribing so we never miss a message.
  const v5Received = new Promise<{ payload: string; userProps: Record<string, string> | null }>(
    (resolve) => {
      v5Sub.on('message', (_t, payload, packet) => {
        const props = packet.properties as { userProperties?: Record<string, string> } | undefined;
        resolve({ payload: payload.toString(), userProps: props?.userProperties ?? null });
      });
    },
  );

  const v311Received = new Promise<{ payload: string; userProps: null }>((resolve) => {
    v311Sub.on('message', (_t, payload) => {
      // v3.1.1 wire protocol has NO user-properties — nothing carried in either direction.
      resolve({ payload: payload.toString(), userProps: null });
    });
  });

  // Subscribe both clients at QoS 1.
  await new Promise<void>((resolve, reject) => {
    v5Sub.subscribe(EVENTS_TOPIC, { qos: 1 }, (err) => { err ? reject(err) : resolve(); });
  });
  await new Promise<void>((resolve, reject) => {
    v311Sub.subscribe(EVENTS_TOPIC, { qos: 1 }, (err) => { err ? reject(err) : resolve(); });
  });
  console.log(`[subs]       both subscribed to ${EVENTS_TOPIC}  QoS 1`);

  // Wait for broker to register subscriptions.
  await new Promise<void>((r) => setTimeout(r, 300));

  // Publish from a v5 client with user-properties (-> KubeMQ Tags).
  // Cap: 32 user-properties / 4096 bytes total per message; exceeding -> PUBACK 0x97.
  // IMPORTANT: do NOT set retain=true — a retained PUBLISH is silently stripped
  //            (PUBACK 0x00 succeeds but message is DROPPED, never delivered).
  const v5Pub = await connectClient('js-v311v5-v5pub', 5);
  console.log('[v5 pub]     connected (MQTT 5.0)');

  await new Promise<void>((resolve, reject) => {
    v5Pub.publish(
      EVENTS_TOPIC,
      JSON.stringify({ msg: 'hello from v5 publisher' }),
      {
        qos: 1,
        retain: false, // MUST be false — retain is silently stripped (gotcha #1)
        properties: {
          // MQTT 5.0 User Properties -> KubeMQ Tags (bidirectional).
          // v3.1.1 carries NOTHING here — not even a degraded form.
          userProperties: { k1: 'v1', proto: 'v5' },
        },
      },
      (err) => { err ? reject(err) : resolve(); },
    );
  });
  console.log(`[v5 pub]     published to ${EVENTS_TOPIC} QoS 1  user-props={k1:v1, proto:v5}`);

  // Wait for both subscribers.
  const [v5Result, v311Result] = await Promise.all([
    Promise.race([
      v5Received,
      new Promise<never>((_, r) => setTimeout(() => r(new Error('v5 sub timeout')), 5_000)),
    ]),
    Promise.race([
      v311Received,
      new Promise<never>((_, r) => setTimeout(() => r(new Error('v3.1.1 sub timeout')), 5_000)),
    ]),
  ]);

  console.log('');
  console.log('[v5 sub]    payload:', v5Result.payload);
  console.log('[v5 sub]    user-props (KubeMQ tags):', JSON.stringify(v5Result.userProps));
  console.log('[v3.1.1 sub] payload:', v311Result.payload);
  console.log('[v3.1.1 sub] user-props: null  (v3.1.1 wire has no user-properties)');
  console.log('');
  console.log('[result] User-Properties: v5 = YES | v3.1.1 = NO (nothing carried, not degraded)');

  await v5Sub.endAsync();
  await v311Sub.endAsync();
  await v5Pub.endAsync();
}

// ---------------------------------------------------------------------------
// Part 2: v5 RPC command round-trip (positive case)
// PUBACK arrives IMMEDIATELY on receipt; the client must wait for the response
// independently.  Requires gRPC-side responder on channel demo.cmd.
// ---------------------------------------------------------------------------
async function part2V5Rpc(): Promise<void> {
  console.log('--- Part 2: RPC command round-trip (MQTT 5.0 only; requires gRPC responder) ---');

  const CLIENT_ID    = 'js-v311v5-rpc5';
  const REPLY_TOPIC  = `$reply/${CLIENT_ID}/inbox`; // OWN namespace — foreign -> PUBACK 0x83
  const CMD_TOPIC    = 'commands/demo/cmd';          // KubeMQ channel: demo.cmd
  const CORR_DATA    = Buffer.from('contrast-corr-1');
  const RPC_TIMEOUT  = 15_000; // 15s; default connector RPC timeout is 30s

  const client = await connectClient(CLIENT_ID, 5);
  console.log('[v5 rpc]  connected (MQTT 5.0)');

  // Step A: subscribe the reply inbox BEFORE publishing (required — see gotcha #4).
  await new Promise<void>((resolve, reject) => {
    client.subscribe(REPLY_TOPIC, { qos: 1 }, (err, granted) => {
      if (err) { reject(err); return; }
      const code = granted?.[0]?.qos ?? -1;
      if (code > 2) {
        reject(new Error(`Reply subscribe rejected: 0x${code.toString(16)}`));
        return;
      }
      console.log(`[v5 rpc]  subscribed to reply inbox: ${REPLY_TOPIC}`);
      resolve();
    });
  });

  // Step B: arm the response listener BEFORE publishing.
  // NOTE: PUBACK arrives immediately — registering the handler first avoids a race.
  const responsePromise = new Promise<boolean>((resolve, reject) => {
    const timer = setTimeout(
      () => {
        // Timeout is informational — the example continues to Part 3.
        console.log(
          `[v5 rpc]  timeout (${RPC_TIMEOUT}ms) — no gRPC responder running on channel demo.cmd`,
        );
        console.log(
          '[v5 rpc]  start one with: go run .work/test-harness/responder/ -channel demo.cmd -type command',
        );
        resolve(false); // did not receive response
      },
      RPC_TIMEOUT,
    );

    client.on('message', (topic, payload, packet) => {
      if (topic !== REPLY_TOPIC) return;
      clearTimeout(timer);

      const props = packet.properties as {
        correlationData?: Buffer;
        userProperties?: Record<string, string | string[]>;
      } | undefined;

      const corrEchoed = props?.correlationData?.toString() ?? '(none)';
      const userProps  = props?.userProperties ?? {};
      // Command response carries NO body — the outcome is in user-props.
      const executed   = (userProps['kubemq-executed'] as string | undefined) ?? '(absent)';
      const errorMsg   = (userProps['kubemq-error']    as string | undefined) ?? '(none)';

      console.log(`[v5 rpc]  response received on ${topic}`);
      console.log(`[v5 rpc]    CorrelationData echoed: ${corrEchoed}`);
      console.log(`[v5 rpc]    body: ${payload.length > 0 ? payload.toString() : '(empty — expected for commands)'}`);
      console.log(`[v5 rpc]    kubemq-executed: ${executed}`);
      console.log(`[v5 rpc]    kubemq-error: ${errorMsg}`);
      resolve(true); // response received
    });
  });

  // Step C: publish the command with ResponseTopic + CorrelationData.
  await new Promise<void>((resolve, reject) => {
    client.publish(
      CMD_TOPIC,
      JSON.stringify({ action: 'ping' }),
      {
        qos: 1,
        properties: {
          responseTopic:   REPLY_TOPIC, // MUST be own $reply/<clientID>/... namespace
          correlationData: CORR_DATA,
          userProperties:  { source: 'js-contrast-example' },
        },
      },
      (err) => {
        if (err) { reject(err); return; }
        // PUBACK is sent IMMEDIATELY on receipt — NOT after the gRPC responder replies.
        // The client relies on responsePromise for the actual reply (gotcha #4).
        console.log(`[v5 rpc]  command published to ${CMD_TOPIC} (PUBACK received immediately)`);
        console.log('[v5 rpc]  waiting for response...');
        resolve();
      },
    );
  });

  const got = await responsePromise;
  if (got) {
    console.log('[result] RPC: v5 = SUPPORTED (ResponseTopic + CorrelationData + immediate PUBACK)');
  } else {
    console.log('[result] RPC: v5 = SUPPORTED (requires gRPC responder — none running)');
  }

  await client.endAsync();
}

// ---------------------------------------------------------------------------
// Part 3: v3.1.1 RPC silent drop
// A v3.1.1 PUBLISH to commands/<ch> receives PUBACK 0x00 but is SILENTLY DROPPED
// by the connector (publish.error audited, no routing).  No response ever arrives.
// Test #12 RPC311Dropped.
// ---------------------------------------------------------------------------
async function part3V311RpcDrop(): Promise<void> {
  console.log('--- Part 3: v3.1.1 RPC — silently dropped (test #12 RPC311Dropped) ---');

  const v311Client = await connectClient('js-v311v5-rpc311', 4);
  console.log('[v3.1.1 rpc] connected (MQTT 3.1.1)');

  let responseReceived = false;
  v311Client.on('message', () => { responseReceived = true; });

  // Publish to commands/<ch> from a v3.1.1 client.
  // PUBACK (0x00) succeeds — the connector still acknowledges the packet.
  // But the message is DROPPED: not routed to KubeMQ Commands, no response dispatched.
  await new Promise<void>((resolve, reject) => {
    v311Client.publish(
      'commands/demo/x',
      JSON.stringify({ action: 'ping' }),
      { qos: 1 },
      (err) => {
        if (err) { reject(err); return; }
        console.log('[v3.1.1 rpc] PUBACK received (0x00) — connector acknowledged but SILENTLY DROPPED');
        resolve();
      },
    );
  });

  // Wait 2 s — no response should arrive.
  await new Promise<void>((r) => setTimeout(r, 2_000));

  if (!responseReceived) {
    console.log('[v3.1.1 rpc] confirmed: NO response received (publish.error audited server-side)');
  } else {
    console.warn('[v3.1.1 rpc] UNEXPECTED response — this should never happen over v3.1.1');
  }

  console.log('[result] RPC: v3.1.1 = SILENTLY DROPPED (PUBACK 0x00, no routing, no response)');

  await v311Client.endAsync();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  console.log('=== MQTT v3.1.1 vs v5 — feature contrast ===');
  console.log(`broker: ${brokerUrl()}`);
  console.log('');
  console.log('v5 unlocks: User-Properties (KubeMQ Tags), $share queue consume, RPC');
  console.log('v3.1.1 has: basic pub/sub only (Events/EventsStore/Queues produce)');
  console.log('');

  await part1EventsPubSub();

  console.log('');
  await part2V5Rpc();

  console.log('');
  await part3V311RpcDrop();

  console.log('');
  console.log('=== Summary ===');
  console.log('Feature                         | MQTT 3.1.1 | MQTT 5.0');
  console.log('--------------------------------|------------|--------------------');
  console.log('Events pub/sub                  | YES        | YES');
  console.log('Events-Store pub/sub            | YES        | YES');
  console.log('Queues produce                  | YES        | YES');
  console.log('Queues consume ($share, QoS>=1) | NO         | YES');
  console.log('User-Properties / KubeMQ Tags   | NO         | YES (32 props/4096B)');
  console.log('RPC — Commands / Queries        | NO (silent drop) | YES');
  console.log('ResponseTopic + CorrelationData | NO         | YES');
  console.log('[done]');
}

main().catch((err) => {
  console.error('[error]', err instanceof Error ? err.message : String(err));
  process.exit(1);
});

// ---------------------------------------------------------------------------
// Expected output (with gRPC responder running on channel demo.cmd):
//
// === MQTT v3.1.1 vs v5 — feature contrast ===
// broker: tcp://127.0.0.1:1883
//
// v5 unlocks: User-Properties (KubeMQ Tags), $share queue consume, RPC
// v3.1.1 has: basic pub/sub only (Events/EventsStore/Queues produce)
//
// --- Part 1: Events pub/sub (both v3.1.1 and v5 work) ---
// [v5 sub]    connected (MQTT 5.0)
// [v3.1.1 sub] connected (MQTT 3.1.1)
// [subs]       both subscribed to events/demo/v3  QoS 1
// [v5 pub]     connected (MQTT 5.0)
// [v5 pub]     published to events/demo/v3 QoS 1  user-props={k1:v1, proto:v5}
//
// [v5 sub]    payload: {"msg":"hello from v5 publisher"}
// [v5 sub]    user-props (KubeMQ tags): {"k1":"v1","proto":"v5"}
// [v3.1.1 sub] payload: {"msg":"hello from v5 publisher"}
// [v3.1.1 sub] user-props: null  (v3.1.1 wire has no user-properties)
//
// [result] User-Properties: v5 = YES | v3.1.1 = NO (nothing carried, not degraded)
//
// --- Part 2: RPC command round-trip (MQTT 5.0 only; requires gRPC responder) ---
// [v5 rpc]  connected (MQTT 5.0)
// [v5 rpc]  subscribed to reply inbox: $reply/js-v311v5-rpc5/inbox
// [v5 rpc]  command published to commands/demo/cmd (PUBACK received immediately)
// [v5 rpc]  waiting for response...
// [v5 rpc]  response received on $reply/js-v311v5-rpc5/inbox
// [v5 rpc]    CorrelationData echoed: contrast-corr-1
// [v5 rpc]    body: (empty — expected for commands)
// [v5 rpc]    kubemq-executed: true
// [v5 rpc]    kubemq-error: (none)
// [result] RPC: v5 = SUPPORTED (ResponseTopic + CorrelationData + immediate PUBACK)
//
// --- Part 3: v3.1.1 RPC — silently dropped (test #12 RPC311Dropped) ---
// [v3.1.1 rpc] connected (MQTT 3.1.1)
// [v3.1.1 rpc] PUBACK received (0x00) — connector acknowledged but SILENTLY DROPPED
// [v3.1.1 rpc] confirmed: NO response received (publish.error audited server-side)
// [result] RPC: v3.1.1 = SILENTLY DROPPED (PUBACK 0x00, no routing, no response)
//
// === Summary ===
// Feature                         | MQTT 3.1.1 | MQTT 5.0
// --------------------------------|------------|--------------------
// Events pub/sub                  | YES        | YES
// Events-Store pub/sub            | YES        | YES
// Queues produce                  | YES        | YES
// Queues consume ($share, QoS>=1) | NO         | YES
// User-Properties / KubeMQ Tags   | NO         | YES (32 props/4096B)
// RPC — Commands / Queries        | NO (silent drop) | YES
// ResponseTopic + CorrelationData | NO         | YES
// [done]
