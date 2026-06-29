/**
 * Example: connectivity/websocket
 *
 * Demonstrates Events pub/sub over WebSocket (ws://host:8083/) using the KubeMQ
 * MQTT connector. Per decision D7, examples default to MQTT 5.0.
 *
 * Integration test #19 (TestMqttIntegration_WebSocketListener) uses a v3.1.1 client
 * for convenience — v5-over-WebSocket is equally valid and is what this example uses.
 *
 * Canonical (S6.1.9): sub events/demo/ws, pub events/demo/ws over ws://host:8083/; QoS 1.
 *
 * Run: npx tsx connectivity/websocket/index.ts
 * With custom WS URL: KUBEMQ_MQTT_URL=ws://my-kubemq:8083/ npx tsx connectivity/websocket/index.ts
 */
import mqtt, { type MqttClient } from 'mqtt';

function brokerUrl(): string {
  const envUrl = process.env['KUBEMQ_MQTT_URL'];
  if (envUrl) return envUrl;
  // Default to WebSocket for this example.
  return 'ws://localhost:8083/';
}

async function main(): Promise<void> {
  const url = brokerUrl();
  const topic = 'events/demo/ws';

  console.log(`[example] connecting over WebSocket to ${url}`);

  const subscriber: MqttClient = mqtt.connect(url, {
    clientId: 'js-mqtt-ws-sub',
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
  });

  subscriber.on('error', (err) => {
    console.error('[subscriber] connection error:', err.message);
    process.exit(1);
  });

  await new Promise<void>((resolve, reject) => {
    subscriber.once('connect', () => resolve());
    subscriber.once('error', reject);
  });

  console.log('[subscriber] connected over WebSocket');

  const received = new Promise<void>((resolve) => {
    subscriber.on('message', (t, payload) => {
      console.log(`[subscriber] topic: ${t} | payload: ${payload.toString()}`);
      resolve();
    });
  });

  await new Promise<void>((resolve, reject) => {
    subscriber.subscribe(topic, { qos: 1 }, (err) => {
      if (err) { reject(err); return; }
      console.log(`[subscriber] subscribed to ${topic}`);
      resolve();
    });
  });

  await new Promise<void>((r) => setTimeout(r, 300));

  const publisher: MqttClient = mqtt.connect(url, {
    clientId: 'js-mqtt-ws-pub',
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
  });

  publisher.on('error', (err) => {
    console.error('[publisher] connection error:', err.message);
    process.exit(1);
  });

  await new Promise<void>((resolve, reject) => {
    publisher.once('connect', () => resolve());
    publisher.once('error', reject);
  });

  console.log('[publisher] connected over WebSocket');

  await new Promise<void>((resolve, reject) => {
    publisher.publish(
      topic,
      JSON.stringify({ transport: 'websocket', message: 'Hello over WebSocket!' }),
      { qos: 1 },
      (err) => {
        if (err) { reject(err); return; }
        console.log(`[publisher] published to ${topic} over WebSocket`);
        resolve();
      },
    );
  });

  const timeout = new Promise<void>((_, reject) =>
    setTimeout(() => reject(new Error('Timed out waiting for WebSocket event')), 10_000),
  );
  await Promise.race([received, timeout]);

  await publisher.endAsync();
  await subscriber.endAsync();
  console.log('[done]');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output:
// [example] connecting over WebSocket to ws://localhost:8083/
// [subscriber] connected over WebSocket
// [subscriber] subscribed to events/demo/ws
// [publisher] connected over WebSocket
// [publisher] published to events/demo/ws over WebSocket
// [subscriber] topic: events/demo/ws | payload: {"transport":"websocket","message":"Hello over WebSocket!"}
// [done]
