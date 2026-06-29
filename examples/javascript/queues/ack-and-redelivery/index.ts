/**
 * Example: queues/ack-and-redelivery
 *
 * Demonstrates the KubeMQ Queues ack model over MQTT:
 *   - Ack is triggered by sending PUBACK (automatic in MQTT.js for QoS 1).
 *   - No PUBACK within QueueAckTimeoutSeconds (default 30s) -> NAck -> redeliver.
 *   - Disconnect with unacked message -> immediate NAck -> requeue (no loss).
 *
 * Flow (mirrors tests #6 QueueRedeliveryOnDisconnect + #7 QueueAckTimeout):
 *   1. Producer enqueues one message to queues/demo/rq.
 *   2. Consumer A subscribes but IMMEDIATELY disconnects without sending PUBACK
 *      (simulate by closing connection before the message handler fires).
 *      -> message is requeued.
 *   3. Consumer B subscribes and receives + acks (PUBACK) the redelivered message.
 *
 * Canonical (S6.1.5): consume $share/g1/queues/demo/rq with manual-ack;
 * disconnect w/o ack -> redeliver to survivor; QoS 1.
 *
 * NOTE: MQTT.js sends PUBACK automatically for QoS 1 messages. To simulate
 * "no ack", we destroy the underlying socket before the message event fires.
 *
 * Run: npx tsx queues/ack-and-redelivery/index.ts
 */
import mqtt, { type MqttClient } from 'mqtt';

function brokerUrl(): string {
  return process.env['KUBEMQ_MQTT_URL'] ?? 'tcp://localhost:1883';
}

function connect(clientId: string): Promise<MqttClient> {
  return new Promise<MqttClient>((resolve, reject) => {
    const client = mqtt.connect(brokerUrl(), {
      clientId,
      protocolVersion: 5,
      clean: true,
      keepalive: 30,
      // Disable auto-reconnect: when we deliberately destroy consumerA's socket
      // to simulate "crashed before ack", MQTT.js must NOT silently reconnect
      // and re-receive the redelivery (which would steal it from consumerB).
      reconnectPeriod: 0,
    });
    client.once('connect', () => resolve(client));
    client.once('error', reject);
  });
}

async function publish(client: MqttClient, topic: string, payload: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    client.publish(topic, payload, { qos: 1 }, (err) => {
      if (err) { reject(err); return; }
      resolve();
    });
  });
}

async function main(): Promise<void> {
  const produceTopic = 'queues/demo/rq';
  const consumeTopic = '$share/g1/queues/demo/rq';

  // Step 1: produce one message.
  const producer = await connect('js-mqtt-ack-producer');
  console.log('[producer] connected');
  await publish(producer, produceTopic, JSON.stringify({ job: 'requeue-demo', attempt: 1 }));
  console.log(`[producer] enqueued message to ${produceTopic}`);
  await producer.endAsync();

  // Allow the message to settle in the queue.
  await new Promise<void>((r) => setTimeout(r, 300));

  // Step 2: Consumer A subscribes, RECEIVES the message, then disconnects
  // abruptly WITHOUT acking (no PUBACK).
  //
  // TIMING IS CRITICAL: consumerA must actually receive the delivery before
  // destroying the socket. Only then does the broker have a tracked in-flight
  // (unacked) delivery for consumerA, which it NAcks/requeues IMMEDIATELY on
  // disconnect (the §3.4 disconnect path). If we destroyed the socket right
  // after subscribe() — before any delivery — the broker would have nothing
  // in-flight to requeue, and the message would instead wait the full 30s
  // ack-timeout, exceeding consumerB's wait window.
  const consumerA = await connect('js-mqtt-ack-consumer-a');
  console.log('[consumerA] connected');

  // Arm the receive handler that destroys the socket the instant the message
  // arrives — synchronously, BEFORE MQTT.js flushes the PUBACK to the socket.
  const consumerAReceived = new Promise<void>((resolve) => {
    consumerA.on('message', (_topic, payload) => {
      console.log(`[consumerA] received (will NOT ack): ${payload.toString()}`);
      // Destroy the socket before the PUBACK is written -> broker sees an
      // unacked in-flight delivery on disconnect and requeues it.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (consumerA.stream as any).destroy();
      console.log('[consumerA] socket destroyed (no PUBACK sent) -> broker requeues');
      resolve();
    });
  });

  await new Promise<void>((resolve, reject) => {
    consumerA.subscribe(consumeTopic, { qos: 1 }, (err) => {
      if (err) { reject(err); return; }
      console.log('[consumerA] subscribed, awaiting delivery before disconnecting...');
      resolve();
    });
  });

  // Wait until consumerA has received the message and destroyed its socket.
  const consumerATimeout = new Promise<void>((_, reject) =>
    setTimeout(() => reject(new Error('consumerA never received the message to requeue')), 10_000),
  );
  await Promise.race([consumerAReceived, consumerATimeout]);

  // Brief delay for the broker to process the disconnect and requeue.
  await new Promise<void>((r) => setTimeout(r, 1_500));

  // Step 3: Consumer B connects and receives the redelivered message.
  const consumerB = await connect('js-mqtt-ack-consumer-b');
  console.log('[consumerB] connected');

  const redelivered = new Promise<void>((resolve) => {
    consumerB.on('message', (topic, payload) => {
      console.log(`[consumerB] received (redelivered): ${payload.toString()}`);
      console.log('[consumerB] PUBACK sent automatically (ack-on-PUBACK)');
      resolve();
    });
  });

  await new Promise<void>((resolve, reject) => {
    consumerB.subscribe(consumeTopic, { qos: 1 }, (err) => {
      if (err) { reject(err); return; }
      console.log('[consumerB] subscribed, waiting for redelivered message...');
      resolve();
    });
  });

  const timeout = new Promise<void>((_, reject) =>
    setTimeout(() => reject(new Error('Timed out waiting for redelivered message')), 15_000),
  );
  await Promise.race([redelivered, timeout]);

  await consumerB.endAsync();
  console.log('[done] message redelivered and acked successfully');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output:
// [producer] connected
// [producer] enqueued message to queues/demo/rq
// [consumerA] connected
// [consumerA] subscribed, awaiting delivery before disconnecting...
// [consumerA] received (will NOT ack): {"job":"requeue-demo","attempt":1}
// [consumerA] socket destroyed (no PUBACK sent) -> broker requeues
// [consumerB] connected
// [consumerB] subscribed, waiting for redelivered message...
// [consumerB] received (redelivered): {"job":"requeue-demo","attempt":1}
// [consumerB] PUBACK sent automatically (ack-on-PUBACK)
// [done] message redelivered and acked successfully
