/**
 * Example: events/basic-pubsub
 *
 * Demonstrates fire-and-forget event pub/sub via the KubeMQ MQTT connector
 * using MQTT 5.0.
 *
 * Wire contract:
 *   - Subscribe: events/demo/+  QoS 1  (single-level '+' wildcard)
 *   - Publish:   events/demo/x  QoS 1  payload "hello"  (matches events/demo/+)
 *   - MQTT topic '/' becomes '.' in the KubeMQ channel:
 *       events/demo/x  ->  KubeMQ channel  demo.x
 *   - One MQTT 5.0 User Property (k1=v1) is sent and received,
 *     demonstrating the User-Properties <-> KubeMQ Tags round-trip.
 *     Caps: 32 user-properties / 4096 bytes total per message.
 *
 * Run: npx tsx events/basic-pubsub/index.ts
 */
import mqtt, { type MqttClient } from 'mqtt';

// ---------------------------------------------------------------------------
// Broker URL: read from env, default to tcp://localhost:1883.
// Scheme may be tcp://, tls://, or ws://.
// ---------------------------------------------------------------------------
function brokerUrl(): string {
  return process.env['KUBEMQ_MQTT_URL'] ?? 'tcp://localhost:1883';
}

async function main(): Promise<void> {
  const url = brokerUrl();

  // KubeMQ MQTT topic -> KubeMQ channel:  events/<ch>  where '/' -> '.'
  //   subscribe filter events/demo/+ (single-level wildcard) matches the
  //   publish topic events/demo/x  ->  channel  demo.x
  const subscribeFilter = 'events/demo/+';
  const publishTopic = 'events/demo/x';
  const payload = 'hello';

  // -------------------------------------------------------------------------
  // Step 1: Connect subscriber (MQTT 5.0)
  // -------------------------------------------------------------------------
  const subscriber: MqttClient = mqtt.connect(url, {
    clientId: 'js-mqtt-events-basic-sub',
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
  });

  await new Promise<void>((resolve, reject) => {
    subscriber.once('connect', () => resolve());
    subscriber.once('error', reject);
  });
  console.log('[subscriber] connected to', url);

  // -------------------------------------------------------------------------
  // Step 2: Subscribe to events/demo/+ at QoS 1
  //   The '+' single-level wildcard matches one topic level (events/demo/x,
  //   events/demo/y, ...).  Wildcards ('+' -> '*', '#' -> '>') are accepted on
  //   the Events pattern ONLY; a non-events wildcard subscribe -> SUBACK 0xA2.
  //   SUBACK reason code 0x01 = granted QoS 1
  //   SUBACK reason code 0xA2 = wildcard-subscriptions-not-supported (non-events)
  //   SUBACK reason code 0x83 = QoS-0 queue / plain-queue-sub / foreign $reply
  // -------------------------------------------------------------------------
  const receivedPromise = new Promise<void>((resolve) => {
    subscriber.on('message', (receivedTopic, receivedPayload, packet) => {
      console.log(`[subscriber] message received on topic: ${receivedTopic}`);
      console.log(`[subscriber]   payload: ${receivedPayload.toString()}`);
      console.log(`[subscriber]   KubeMQ channel (topic '/' -> '.'): demo.x`);

      // MQTT 5.0 User Properties arrive as KubeMQ Tags echoed back.
      const userProps = (
        packet.properties as { userProperties?: Record<string, string> } | undefined
      )?.userProperties;
      if (userProps && Object.keys(userProps).length > 0) {
        console.log(
          '[subscriber]   user-properties (KubeMQ tags round-trip):',
          JSON.stringify(userProps),
        );
      } else {
        console.log('[subscriber]   user-properties: (none)');
      }

      resolve();
    });
  });

  await new Promise<void>((resolve, reject) => {
    subscriber.subscribe(subscribeFilter, { qos: 1 }, (err, granted) => {
      if (err) {
        reject(new Error(`Subscribe failed: ${String(err)}`));
        return;
      }
      // Check SUBACK reason codes from the broker.
      for (const g of granted ?? []) {
        const rc = (g as { qos: number }).qos;
        if (rc >= 0x80) {
          reject(
            new Error(
              `SUBACK returned error reason code 0x${rc.toString(16).padStart(2, '0')} ` +
                `(0x83=impl-specific, 0xA2=wildcard-not-supported, 0x8F=invalid-filter)`,
            ),
          );
          return;
        }
      }
      console.log(`[subscriber] subscribed: ${subscribeFilter}  QoS 1`);
      resolve();
    });
  });

  // Allow the subscription to be registered by the broker before publishing.
  await new Promise<void>((r) => setTimeout(r, 300));

  // -------------------------------------------------------------------------
  // Step 3: Connect publisher (MQTT 5.0)
  // -------------------------------------------------------------------------
  const publisher: MqttClient = mqtt.connect(url, {
    clientId: 'js-mqtt-events-basic-pub',
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
  });

  await new Promise<void>((resolve, reject) => {
    publisher.once('connect', () => resolve());
    publisher.once('error', reject);
  });
  console.log('[publisher]  connected to', url);

  // -------------------------------------------------------------------------
  // Step 4: Publish to events/demo/x  QoS 1, payload "hello"  (matches events/demo/+)
  //   - PUBACK reason code 0x00 = success
  //   - PUBACK reason code 0x80 = broker-not-ready (publish gated)
  //   - PUBACK reason code 0x97 = quota exceeded (user-prop cap)
  //   NOTE: do NOT set the retain flag — a retained PUBLISH is silently
  //         stripped (PUBACK 0x00 succeeds but the message is dropped).
  //         (0x9A retain-not-supported applies only to CONNECT will-retain,
  //          NOT to a runtime retained publish, which gets PUBACK 0x00.)
  // -------------------------------------------------------------------------
  await new Promise<void>((resolve, reject) => {
    publisher.publish(
      publishTopic,
      payload,
      {
        qos: 1,
        retain: false,          // IMPORTANT: retain is NOT supported; setting it
                                // causes silent message drop (PUBACK 0x00 but
                                // message never delivered).
        properties: {
          // MQTT 5.0 User Property -> KubeMQ Tag (bidirectional).
          // Cap: 32 properties / 4096 bytes total.  Exceeding -> PUBACK 0x97.
          userProperties: { k1: 'v1' },
        },
      },
      (err) => {
        if (err) {
          reject(new Error(`Publish failed: ${String(err)}`));
          return;
        }
        console.log(`[publisher]  published "${payload}" -> ${publishTopic}  QoS 1`);
        console.log('[publisher]  user-property sent: k1=v1 (-> KubeMQ tag)');
        resolve();
      },
    );
  });

  // -------------------------------------------------------------------------
  // Step 5: Wait for the subscriber to receive the message.
  // -------------------------------------------------------------------------
  await receivedPromise;

  // -------------------------------------------------------------------------
  // Cleanup
  // -------------------------------------------------------------------------
  await publisher.endAsync();
  await subscriber.endAsync();
  console.log('[done]');
}

main().catch((err) => {
  console.error('[error]', err instanceof Error ? err.message : String(err));
  process.exit(1);
});

// Expected output:
// [subscriber] connected to tcp://localhost:1883
// [subscriber] subscribed: events/demo/+  QoS 1
// [publisher]  connected to tcp://localhost:1883
// [publisher]  published "hello" -> events/demo/x  QoS 1
// [publisher]  user-property sent: k1=v1 (-> KubeMQ tag)
// [subscriber] message received on topic: events/demo/x
// [subscriber]   payload: hello
// [subscriber]   KubeMQ channel (topic '/' -> '.'): demo.x
// [subscriber]   user-properties (KubeMQ tags round-trip): {"k1":"v1"}
// [done]
