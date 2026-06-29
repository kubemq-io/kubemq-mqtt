/**
 * Example: queues/shared-consume
 *
 * Demonstrates KubeMQ Queues over MQTT:
 *   - PRODUCE: plain publish to queues/<ch> (any QoS)
 *   - CONSUME: shared subscription $share/<group>/queues/<ch> at QoS >= 1
 *
 * Negative paths documented (S6.1.4 / M-3):
 *   - QoS 0 shared-queue subscribe -> SUBACK 0x83 (test #8)
 *   - Plain queues/<ch> subscribe (no $share) -> SUBACK 0x83 (test #9)
 *
 * Canonical (S6.1.4): produce queues/demo/q; consume $share/g1/queues/demo/q;
 * QoS 1; test #5 QueueAckHappyPath.
 *
 * NOTE: The $share group name is audit/metrics-only — ALL groups compete in ONE
 * shared KubeMQ queue pool (not per-group-copy semantics). See gotcha #3.
 *
 * Run: npx tsx queues/shared-consume/index.ts
 */
import mqtt, { type MqttClient, type Packet } from 'mqtt';

function brokerUrl(): string {
  return process.env['KUBEMQ_MQTT_URL'] ?? 'tcp://localhost:1883';
}

/**
 * Subscribe to a single filter and return the RAW SUBACK reason code.
 *
 * MQTT.js surfaces a SUBACK reason code >= 0x80 as an `err` ("Subscribe error:
 * Implementation specific error") and the `granted` callback arg carries the
 * ORIGINAL request (qos = requested), NOT the failure code. To read the real
 * code (e.g. 131 = 0x83) we capture the raw SUBACK via 'packetreceive'.
 */
function subscribeRawCode(
  client: MqttClient,
  filter: string,
  qos: 0 | 1 | 2,
): Promise<number> {
  return new Promise<number>((resolve) => {
    let rawCode = -1;
    const onSuback = (packet: Packet): void => {
      if (packet.cmd === 'suback') {
        const granted = (packet as { granted?: Array<number | object> }).granted;
        const first = Array.isArray(granted) ? granted[0] : undefined;
        if (typeof first === 'number') rawCode = first;
      }
    };
    client.on('packetreceive', onSuback);
    client.subscribe(filter, { qos }, () => {
      client.removeListener('packetreceive', onSuback);
      resolve(rawCode);
    });
  });
}

function connect(clientId: string): Promise<MqttClient> {
  return new Promise<MqttClient>((resolve, reject) => {
    const client = mqtt.connect(brokerUrl(), {
      clientId,
      protocolVersion: 5,
      clean: true,
      keepalive: 30,
    });
    client.once('connect', () => resolve(client));
    client.once('error', reject);
  });
}

async function main(): Promise<void> {
  const url = brokerUrl();
  const produceTopic = 'queues/demo/q';
  const consumeTopic = '$share/g1/queues/demo/q';

  // ---------------------------------------------------------------------------
  // Negative-path checks run on a SEPARATE, short-lived probe client that is
  // disconnected before the real consumer subscribes.
  //
  // WHY a separate client: registering a rejected queue filter (plain queues/…
  // or QoS-0 share) on the SAME client ID that later does the valid shared
  // consume corrupts that client's queue-bridge registration on the connector,
  // and the valid subscription then never receives deliveries. A real consumer
  // never mixes rejected and valid queue subscriptions — so we keep them apart.
  // ---------------------------------------------------------------------------
  const probe = await connect('js-mqtt-queue-probe');
  console.log('[probe] connected (negative-path checks)');

  // Negative path #1: QoS 0 shared queue subscribe -> SUBACK 0x83.
  console.log('[probe] testing QoS 0 shared-queue subscribe (expect 0x83)...');
  const qos0Code = await subscribeRawCode(probe, consumeTopic, 0);
  if (qos0Code === 0x83) {
    console.log('[probe] QoS 0 correctly rejected with SUBACK 0x83 (impl-specific error)');
  } else {
    console.log(`[probe] QoS 0 grant code: 0x${(qos0Code >>> 0).toString(16)} (expected 0x83)`);
  }

  // Negative path #2: plain queues/<ch> subscribe (no $share) -> SUBACK 0x83.
  console.log('[probe] testing plain queues/demo/q subscribe (expect 0x83)...');
  const plainCode = await subscribeRawCode(probe, 'queues/demo/q', 1);
  if (plainCode === 0x83) {
    console.log('[probe] plain queues/ correctly rejected with SUBACK 0x83 (impl-specific error)');
  } else {
    console.log(`[probe] plain queues/ grant code: 0x${(plainCode >>> 0).toString(16)} (expected 0x83)`);
  }

  await probe.endAsync();
  console.log('[probe] disconnected');

  // ---------------------------------------------------------------------------
  // Valid consume on a CLEAN consumer client.
  // ---------------------------------------------------------------------------
  const consumer = await connect('js-mqtt-queue-consumer');
  console.log('[consumer] connected');

  const received = new Promise<void>((resolve) => {
    consumer.on('message', (topic, payload) => {
      console.log(`[consumer] received from ${topic}: ${payload.toString()}`);
      resolve();
    });
  });

  const validCode = await subscribeRawCode(consumer, consumeTopic, 1);
  if (validCode >= 0x80) {
    throw new Error(`Valid shared subscribe rejected: 0x${validCode.toString(16)}`);
  }
  console.log(`[consumer] subscribed to ${consumeTopic} qos=${validCode}`);

  await new Promise<void>((r) => setTimeout(r, 300));

  // Producer
  const producer: MqttClient = mqtt.connect(url, {
    clientId: 'js-mqtt-queue-producer',
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
  });

  await new Promise<void>((resolve, reject) => {
    producer.once('connect', () => resolve());
    producer.once('error', reject);
  });

  console.log('[producer] connected');

  await new Promise<void>((resolve, reject) => {
    producer.publish(
      produceTopic,
      JSON.stringify({ orderId: 'ORD-001', item: 'widget', qty: 3 }),
      { qos: 1 },
      (err) => {
        if (err) { reject(err); return; }
        console.log(`[producer] enqueued message to ${produceTopic}`);
        resolve();
      },
    );
  });

  const timeout = new Promise<void>((_, reject) =>
    setTimeout(() => reject(new Error('Timed out waiting for queue message')), 10_000),
  );
  await Promise.race([received, timeout]);

  await producer.endAsync();
  await consumer.endAsync();
  console.log('[done]');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output:
// [probe] connected (negative-path checks)
// [probe] testing QoS 0 shared-queue subscribe (expect 0x83)...
// [probe] QoS 0 correctly rejected with SUBACK 0x83 (impl-specific error)
// [probe] testing plain queues/demo/q subscribe (expect 0x83)...
// [probe] plain queues/ correctly rejected with SUBACK 0x83 (impl-specific error)
// [probe] disconnected
// [consumer] connected
// [consumer] subscribed to $share/g1/queues/demo/q qos=1
// [producer] connected
// [producer] enqueued message to queues/demo/q
// [consumer] received from $share/g1/queues/demo/q: {"orderId":"ORD-001","item":"widget","qty":3}
// [done]
