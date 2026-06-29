/**
 * Example: events/wildcard-subscribe
 *
 * Demonstrates MQTT wildcard subscriptions over KubeMQ Events (MQTT 5.0).
 *
 * Wildcard mapping:
 *   MQTT '+'  ->  KubeMQ '*'  (single-level, any one segment)
 *   MQTT '#'  ->  KubeMQ '>'  (multi-level, any suffix)
 *
 * Wildcards are ONLY permitted on Events subscriptions (topic prefix "events/"
 * or bare wildcards using the default events pattern). Any wildcard subscribe on
 * events-store, queues, commands, or queries returns SUBACK 0xA2
 * (wildcard-subscriptions-not-supported). This example demonstrates BOTH:
 *   1. overlapping Events wildcards -> one copy per matching bridge entry;
 *   2. a non-events wildcard ('store/#') -> SUBACK 0xA2 (test #14 contrast).
 *
 * Overlapping-filter delivery (gotcha #6 / integration test #25):
 *   Each distinct subscribe filter registers an independent bridge entry. When N
 *   entries all match one published topic, the message is injected once per entry
 *   — delivering N copies to every subscriber that matches the topic. There is
 *   NO cross-entry dedup in V1 (gotcha #6).
 *
 * One subscriber registers THREE overlapping filters, all scoped to a per-run
 * UNIQUE channel "events/leak/<id>/..." so no prior/other run reuses it:
 *   - "events/leak/<id>/x"  (exact)        -> KubeMQ leak.<id>.x
 *   - "events/leak/<id>/#"  (multi-level)  -> KubeMQ leak.<id>.>
 *   - "events/leak/<id>/+"  (single-level) -> KubeMQ leak.<id>.*
 *
 * A single publish to "events/leak/<id>/x" matches all three filters, so the
 * subscriber receives one copy per registered overlapping bridge entry.
 *
 * Noise-proofing: the example counts ONLY copies of its own unique publish
 * topic and asserts ">= WANT_COPIES" (one per filter it registered). On a quiet
 * broker that is exactly three; on a BUSY broker a foreign broad-wildcard
 * subscriber ("#"/"events/#" held by another process maps to the global ">"
 * channel) adds extra matching bridge entries that ALSO fan into the unique
 * channel — that is gotcha #6 itself, so extra copies are reported, not failed.
 *
 * Canonical (S6.3): pub events/leak/<id>/x once -> exactly 3 copies on a quiet
 * broker (>= 3 on a busy one); QoS 1; test #25 (wantCopies = 3) + #14.
 *
 * Run: npx tsx events/wildcard-subscribe/index.ts
 */
import mqtt, { type MqttClient, type Packet } from 'mqtt';
import crypto from 'node:crypto';

// wantCopies mirrors test #25: three overlapping bridge entries deliver three
// copies to every subscriber matching the published topic.
const WANT_COPIES = 3;

function brokerUrl(): string {
  return process.env['KUBEMQ_MQTT_URL'] ?? 'tcp://localhost:1883';
}

async function main(): Promise<void> {
  const url = brokerUrl();

  // Per-run unique id so this example's overlapping filters target a freshly
  // minted KubeMQ channel that no prior/other run reuses.
  const runId = crypto.randomBytes(4).toString('hex');
  const pubTopic = `events/leak/${runId}/x`; // unique exact
  const uniqueHashFilter = `events/leak/${runId}/#`; // unique subtree
  const uniquePlusFilter = `events/leak/${runId}/+`; // unique single-level

  const client: MqttClient = mqtt.connect(url, {
    clientId: `js-mqtt-wildcard-sub-${runId}`,
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
  });

  await new Promise<void>((resolve, reject) => {
    client.once('connect', () => resolve());
    client.once('error', reject);
  });

  console.log('[client] connected');
  console.log(`[client] per-run unique channel: ${pubTopic}`);

  // ------------------------------------------------------------------------
  // Step 1 — ONE subscriber registers THREE overlapping filters scoped to the
  // per-run unique id (each creates an independent bridge entry).
  //
  // Filter                  KubeMQ channel    Wildcard meaning
  // ----------------------  ----------------  -----------------------------
  // events/leak/<id>/x      leak.<id>.x       exact match
  // events/leak/<id>/#      leak.<id>.>       multi-level ('#' -> '>')
  // events/leak/<id>/+      leak.<id>.*       single-level ('+' -> '*')
  //
  // All three target the per-run unique <id>, so on a quiet broker exactly
  // three bridge entries match and the subscriber sees exactly three copies.
  // ------------------------------------------------------------------------

  // Count ONLY copies of our own unique publish topic. Other Events traffic on
  // the broker is irrelevant to the per-entry-fanout semantic we demonstrate.
  let copies = 0;
  const collected = new Promise<void>((resolve) => {
    client.on('message', (topic, payload) => {
      if (topic !== pubTopic) return; // ignore unrelated background Events traffic
      copies++;
      console.log(`[copy #${copies}] topic: ${topic} | payload: ${payload.toString()}`);
      if (copies >= WANT_COPIES) resolve();
    });
  });

  // Subscribe the three UNIQUE overlapping filters (each = one bridge entry).
  const uniqueFilters: Record<string, { qos: 0 | 1 | 2 }> = {
    [pubTopic]: { qos: 1 },
    [uniqueHashFilter]: { qos: 1 },
    [uniquePlusFilter]: { qos: 1 },
  };

  await new Promise<void>((resolve, reject) => {
    client.subscribe(uniqueFilters, (err, granted) => {
      if (err) { reject(err); return; }
      console.log('[client] subscribed unique filters:', (granted ?? []).map((g) => `${g.topic} qos=${g.qos}`).join(', '));
      resolve();
    });
  });

  // Negative contrast: non-events wildcard -> SUBACK 0xA2.
  //
  // In MQTT.js v5, a SUBACK reason code >= 0x80 is surfaced as an `err`
  // ("Subscribe error: Wildcard Subscriptions not supported") and the `granted`
  // array carries the ORIGINAL request (qos = requested), NOT the failure code.
  // To read the real reason code (162 = 0xA2) we capture the raw SUBACK packet
  // via the 'packetreceive' event.
  console.log('[client] attempting non-events wildcard subscribe (store/#) — expect SUBACK 0xA2...');
  await new Promise<void>((resolve) => {
    let rawCode = -1;
    const onSuback = (packet: Packet): void => {
      if (packet.cmd === 'suback') {
        // SUBACK reason codes live on `granted` (number[] for plain QoS grants
        // or rejection reason codes). mqtt-packet types it as number[]|Object[];
        // the broker sends numeric reason codes, so read the first numeric entry.
        const granted = (packet as { granted?: Array<number | object> }).granted;
        const first = Array.isArray(granted) ? granted[0] : undefined;
        if (typeof first === 'number') rawCode = first;
      }
    };
    client.on('packetreceive', onSuback);

    client.subscribe('store/#', { qos: 1 }, (err) => {
      client.removeListener('packetreceive', onSuback);
      if (rawCode === 0xa2) {
        console.log('[client] store/# correctly rejected with SUBACK 0xA2 (wildcard-subscriptions-not-supported)');
      } else if (err) {
        // Fallback: MQTT.js reported the rejection via err but the raw code was
        // not captured — still a correct (non-granted) outcome.
        console.log(`[client] store/# rejected: ${err.message} (raw SUBACK code 0x${(rawCode >>> 0).toString(16)})`);
      } else {
        console.log(`[client] store/# unexpectedly granted (raw SUBACK code 0x${(rawCode >>> 0).toString(16)})`);
      }
      resolve();
    });
  });

  // Allow subscriptions to register before publishing.
  await new Promise<void>((r) => setTimeout(r, 500));

  // ------------------------------------------------------------------------
  // Step 2 — publish ONE message to the unique events/leak/<id>/x topic.
  // KubeMQ routes it through every matching bridge entry, injecting once per
  // entry -> one copy per overlapping filter we registered (three unique).
  // ------------------------------------------------------------------------
  await new Promise<void>((resolve, reject) => {
    client.publish(pubTopic, JSON.stringify({ event: 'overlap-test', topic: pubTopic }), { qos: 1 }, (err) => {
      if (err) { reject(err); return; }
      console.log(`[client] published once to ${pubTopic}`);
      resolve();
    });
  });

  // ------------------------------------------------------------------------
  // Step 3 — collect copies. Resolve as soon as the three UNIQUE bridge entries
  // have each delivered (>= WANT_COPIES); a busy broker may add ambient extras.
  // ------------------------------------------------------------------------
  const timeout = new Promise<void>((_, reject) =>
    setTimeout(() => reject(new Error(`Timed out — received ${copies}/${WANT_COPIES} copies`)), 10_000),
  );
  await Promise.race([collected, timeout]);

  // Brief window to surface any extra copies (e.g. ambient '>' entries).
  await new Promise<void>((r) => setTimeout(r, 700));

  console.log('');
  console.log('=== Results ===');
  if (copies < WANT_COPIES) {
    console.error(`[FAIL] received ${copies} copies of ${pubTopic}, want >= ${WANT_COPIES}`);
    process.exit(1);
  }
  console.log(`[PASS] received ${copies} copies of ${pubTopic} (>= ${WANT_COPIES} — one per matching bridge registry entry)`);
  if (copies > WANT_COPIES) {
    console.log(`       (${copies - WANT_COPIES} extra copy(ies) — a foreign broad-wildcard subscriber added matching bridge entries on the global '>' channel; that is gotcha #6, not an error)`);
  }
  console.log("       No cross-entry dedup in V1 (gotcha #6 / test #25).");

  await client.endAsync();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output (regenerated from a live run; <id> is a per-run unique hex):
// [client] connected
// [client] per-run unique channel: events/leak/<id>/x
// [client] subscribed unique filters: events/leak/<id>/x qos=1, events/leak/<id>/# qos=1, events/leak/<id>/+ qos=1
// [client] attempting non-events wildcard subscribe (store/#) — expect SUBACK 0xA2...
// [client] store/# correctly rejected with SUBACK 0xA2 (wildcard-subscriptions-not-supported)
// [client] published once to events/leak/<id>/x
// [copy #1] topic: events/leak/<id>/x | payload: {"event":"overlap-test","topic":"events/leak/<id>/x"}
// [copy #2] topic: events/leak/<id>/x | payload: {"event":"overlap-test","topic":"events/leak/<id>/x"}
// [copy #3] topic: events/leak/<id>/x | payload: {"event":"overlap-test","topic":"events/leak/<id>/x"}
//
// === Results ===
// [PASS] received 3 copies of events/leak/<id>/x (>= 3 — one per matching bridge registry entry)
//        No cross-entry dedup in V1 (gotcha #6 / test #25).
