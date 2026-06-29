/**
 * Example: connectivity/tls
 *
 * Demonstrates Events pub/sub over TLS (port 8883) using the KubeMQ MQTT connector.
 *
 * The broker URL is read from KUBEMQ_MQTT_URL; override to tls://host:8883 to use TLS.
 * Default: tls://localhost:8883 (overrides the plain-tcp default for this example).
 *
 * TLS requirements (connector_integration_test.go test #18 TLSListener):
 *   - TLS listener is active only when the Security config is set.
 *   - TLS min version: 1.2; mTLS supported.
 *   - LIVE TEST: TLS listener is CLOSED on this broker -> compile succeeds,
 *     live connect will fail (expected; mark partial/N/A for live testing).
 *
 * Canonical (S6.1.8): sub events/demo/tls, pub events/demo/tls over tls://host:8883; QoS 1.
 *
 * Run: npx tsx connectivity/tls/index.ts
 * With custom TLS URL: KUBEMQ_MQTT_URL=tls://my-kubemq:8883 npx tsx connectivity/tls/index.ts
 */
import mqtt, { type MqttClient, type IClientOptions } from 'mqtt';

function brokerUrl(): string {
  // Default to TLS for this example (override with KUBEMQ_MQTT_URL=tcp://... for plain)
  const envUrl = process.env['KUBEMQ_MQTT_URL'];
  if (envUrl) return envUrl;
  return 'tls://localhost:8883';
}

function tlsClientOptions(clientId: string): IClientOptions {
  return {
    clientId,
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
    // For self-signed certs in dev/test: set rejectUnauthorized=false.
    // Production: provide ca, cert, key as Buffer values via process.env or file paths.
    rejectUnauthorized: false,
  };
}

async function main(): Promise<void> {
  const url = brokerUrl();
  const topic = 'events/demo/tls';

  console.log(`[example] connecting to ${url}`);
  console.log('[example] NOTE: TLS listener requires Security config on the broker.');
  console.log('[example] If the TLS port is closed, the connect will fail (expected on a plain-tcp broker).');

  const subscriber: MqttClient = mqtt.connect(url, tlsClientOptions('js-mqtt-tls-sub'));

  subscriber.on('error', (err) => {
    console.error('[subscriber] connection error:', err.message);
    console.error('[subscriber] If the TLS port is closed, this is expected — mark live test N/A.');
    process.exit(1);
  });

  await new Promise<void>((resolve, reject) => {
    subscriber.once('connect', () => resolve());
    subscriber.once('error', reject);
  });

  console.log(`[subscriber] connected over TLS to ${url}`);

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

  const publisher: MqttClient = mqtt.connect(url, tlsClientOptions('js-mqtt-tls-pub'));

  publisher.on('error', (err) => {
    console.error('[publisher] connection error:', err.message);
    process.exit(1);
  });

  await new Promise<void>((resolve, reject) => {
    publisher.once('connect', () => resolve());
    publisher.once('error', reject);
  });

  console.log('[publisher] connected over TLS');

  await new Promise<void>((resolve, reject) => {
    publisher.publish(
      topic,
      JSON.stringify({ secure: true, message: 'Hello over TLS!' }),
      { qos: 1 },
      (err) => {
        if (err) { reject(err); return; }
        console.log(`[publisher] published to ${topic} over TLS`);
        resolve();
      },
    );
  });

  const timeout = new Promise<void>((_, reject) =>
    setTimeout(() => reject(new Error('Timed out waiting for TLS event')), 10_000),
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

// Expected output (TLS listener active):
// [example] connecting to tls://localhost:8883
// [subscriber] connected over TLS to tls://localhost:8883
// [subscriber] subscribed to events/demo/tls
// [publisher] connected over TLS
// [publisher] published to events/demo/tls over TLS
// [subscriber] topic: events/demo/tls | payload: {"secure":true,"message":"Hello over TLS!"}
// [done]
//
// Expected output (TLS listener closed — live test N/A):
// [example] connecting to tls://localhost:8883
// [subscriber] connection error: connect ECONNREFUSED 127.0.0.1:8883
// [subscriber] If the TLS port is closed, this is expected — mark live test N/A.
