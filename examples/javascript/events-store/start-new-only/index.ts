/**
 * Example: events-store/start-new-only
 *
 * Demonstrates Events-Store over MQTT — which is ALWAYS StartNewOnly.
 * There is NO historical replay over MQTT (gotcha #2). Messages published
 * before a subscription are NOT delivered.
 *
 * Flow (mirrors integration test #4 EventsStoreStartNewOnly):
 *   1. Publish 3 messages to store/demo/s BEFORE subscribing.
 *   2. Subscribe to store/demo/s.
 *   3. Publish 1 message AFTER subscribing.
 *   4. Assert: (a) NONE of this run's pre-subscribe messages were replayed and
 *      (b) this run's post-subscribe message arrived.
 *
 * Note on the shared channel:
 *   store/demo/s is a canonical demo channel that may carry concurrent traffic
 *   from other clients (e.g. sibling examples in other languages running at the
 *   same time). To stay correct under that traffic, every message this run
 *   publishes is tagged with a unique per-run id plus a phase marker
 *   ("pre"/"post"). On receive, each message is classified by run id into
 *   own-pre / own-post / foreign; foreign messages (including non-matching
 *   payloads) are ignored. The assertions only consider THIS run's tagged
 *   messages, so the StartNewOnly guarantee stays provable without a brittle
 *   exact-count assertion against the shared channel.
 *
 * Canonical (S6.1.3): pub store/demo/s (x3 before sub), sub store/demo/s,
 * pub store/demo/s (x1 after); pre-sub NOT delivered; QoS 1.
 *
 * Run: npx tsx events-store/start-new-only/index.ts
 */
import { randomUUID } from 'node:crypto';
import mqtt, { type MqttClient } from 'mqtt';

function brokerUrl(): string {
  return process.env['KUBEMQ_MQTT_URL'] ?? 'tcp://localhost:1883';
}

// Per-run tagged payload: "<runId>|<phase>|<body>". The run id lets the
// subscriber tell THIS run's traffic apart from concurrent foreign traffic on
// the shared channel; the phase ("pre"/"post") distinguishes pre-subscribe
// messages (which must NOT be replayed) from the post-subscribe message.
function tagPayload(runId: string, phase: 'pre' | 'post', body: string): string {
  return `${runId}|${phase}|${body}`;
}

type Kind = 'own-pre' | 'own-post' | 'foreign';

// Classify a received payload by run id and phase. Anything that does not carry
// this run's id (including malformed payloads) is 'foreign' and ignored.
function classify(runId: string, payload: string): Kind {
  const parts = payload.split('|');
  if (parts.length < 2 || parts[0] !== runId) return 'foreign';
  if (parts[1] === 'pre') return 'own-pre';
  if (parts[1] === 'post') return 'own-post';
  return 'foreign';
}

async function publish(client: MqttClient, topic: string, payload: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    client.publish(topic, payload, { qos: 1 }, (err) => {
      if (err) { reject(err); return; }
      resolve();
    });
  });
}

async function main(): Promise<void> {
  const url = brokerUrl();
  const topic = 'store/demo/s';
  // Unique per-run id, embedded in every payload so the subscriber can ignore
  // concurrent foreign traffic on the shared channel.
  const runId = randomUUID();
  console.log(`[run] id: ${runId}`);

  // Publisher client (MQTT 5.0)
  const publisher: MqttClient = mqtt.connect(url, {
    clientId: `js-mqtt-store-pub-${runId.slice(0, 8)}`,
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
  });

  await new Promise<void>((resolve, reject) => {
    publisher.once('connect', () => resolve());
    publisher.once('error', reject);
  });

  console.log('[publisher] connected');

  // Step 1: publish 3 messages BEFORE the subscriber connects.
  console.log('[publisher] publishing 3 pre-subscription messages...');
  for (let i = 1; i <= 3; i++) {
    await publish(
      publisher,
      topic,
      tagPayload(runId, 'pre', JSON.stringify({ seq: i, phase: 'pre-sub' })),
    );
    console.log(`[publisher] sent pre-sub message ${i}`);
  }

  // Small delay to ensure messages are processed before subscriber arrives.
  await new Promise<void>((r) => setTimeout(r, 300));

  // Subscriber client
  const subscriber: MqttClient = mqtt.connect(url, {
    clientId: `js-mqtt-store-sub-${runId.slice(0, 8)}`,
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
  });

  await new Promise<void>((resolve, reject) => {
    subscriber.once('connect', () => resolve());
    subscriber.once('error', reject);
  });

  console.log('[subscriber] connected');

  // Classify only THIS run's messages by phase. Foreign messages on the shared
  // channel are counted separately and ignored by the assertions.
  const ownPre: string[] = [];
  const ownPost: string[] = [];
  const foreign: string[] = [];
  const ownPostArrived = new Promise<void>((resolve) => {
    subscriber.on('message', (_topic, payload) => {
      const text = payload.toString();
      switch (classify(runId, text)) {
        case 'own-pre':
          ownPre.push(text);
          console.log(`[subscriber] received OWN-PRE: ${text}`);
          break;
        case 'own-post':
          ownPost.push(text);
          console.log(`[subscriber] received OWN-POST: ${text}`);
          resolve();
          break;
        case 'foreign':
          foreign.push(text);
          console.log(`[subscriber] ignoring foreign message: ${text}`);
          break;
      }
    });
  });

  // Step 2: subscribe AFTER pre-sub messages were published.
  await new Promise<void>((resolve, reject) => {
    subscriber.subscribe(topic, { qos: 1 }, (err) => {
      if (err) { reject(err); return; }
      console.log(`[subscriber] subscribed to ${topic}`);
      resolve();
    });
  });

  await new Promise<void>((r) => setTimeout(r, 300));

  // Step 3: publish 1 message AFTER subscribing — this one should be received.
  await publish(publisher, topic, tagPayload(runId, 'post', JSON.stringify({ seq: 4, phase: 'post-sub' })));
  console.log('[publisher] sent post-sub message (seq=4)');

  // Wait up to 5 s for this run's post-sub message.
  const timeout = new Promise<void>((_, reject) =>
    setTimeout(() => reject(new Error("Timed out waiting for this run's post-sub message")), 5_000),
  );
  await Promise.race([ownPostArrived, timeout]);

  // Brief drain — give a chance for any (unexpected) replay to arrive.
  await new Promise<void>((r) => setTimeout(r, 500));

  console.log(`[result] this run's pre-subscribe msgs replayed : ${ownPre.length} (StartNewOnly — expect 0)`);
  console.log(`[result] this run's post-subscribe msgs received: ${ownPost.length} (expect >= 1)`);
  console.log(`[result] foreign messages ignored               : ${foreign.length}`);

  await publisher.endAsync();
  await subscriber.endAsync();

  // (a) The StartNewOnly guarantee: NONE of this run's pre-subscribe messages
  //     are replayed to a subscription that started after they were published.
  if (ownPre.length !== 0) {
    throw new Error(
      `[FAIL] StartNewOnly violation: ${ownPre.length} of this run's pre-subscribe message(s) were replayed`,
    );
  }
  // (b) This run's uniquely-tagged post-subscribe message IS delivered.
  if (ownPost.length < 1) {
    throw new Error("[FAIL] this run's post-subscribe message was not delivered");
  }
  console.log('[PASS] StartNewOnly confirmed — no pre-subscribe replay; this run\'s post-subscribe message delivered');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output (run id and foreign count vary per run):
// [run] id: 3f2a1c9e-...
// [publisher] connected
// [publisher] publishing 3 pre-subscription messages...
// [publisher] sent pre-sub message 1
// [publisher] sent pre-sub message 2
// [publisher] sent pre-sub message 3
// [subscriber] connected
// [subscriber] subscribed to store/demo/s
// [publisher] sent post-sub message (seq=4)
// [subscriber] received OWN-POST: 3f2a1c9e-...|post|{"seq":4,"phase":"post-sub"}
// [result] this run's pre-subscribe msgs replayed : 0 (StartNewOnly — expect 0)
// [result] this run's post-subscribe msgs received: 1 (expect >= 1)
// [result] foreign messages ignored               : 0
// [PASS] StartNewOnly confirmed — no pre-subscribe replay; this run's post-subscribe message delivered
