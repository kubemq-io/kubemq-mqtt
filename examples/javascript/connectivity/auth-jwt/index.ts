/**
 * Example: connectivity/auth-jwt
 *
 * Demonstrates password-as-JWT authentication for the KubeMQ MQTT connector.
 *
 * Auth model (§2.5):
 *   - CONNECT Password field = KubeMQ JWT token (PasswordFlag=true).
 *   - Username is DISPLAY-ONLY; not used for authentication.
 *   - Empty password when auth is enabled -> CONNACK 0x86.
 *   - Auth is checked at connect-time only (no mid-connection recheck).
 *   - Auth not configured -> open (no password needed).
 *
 * Live-test status on reference broker:
 *   Auth appears DISABLED -> connecting with a JWT succeeds but JWT rejection
 *   is NOT enforced. Mark partial/N/A; full JWT enforcement requires an
 *   auth-enabled broker.
 *
 * Canonical (S6.1.10): CONNECT with Password=<JWT>, PasswordFlag=true;
 * pub/sub events/demo/auth; QoS 1; tests #16 AuthRequired (wrong -> 0x86) + #17 ACLDenied (->0x87).
 *
 * Run: npx tsx connectivity/auth-jwt/index.ts
 * With auth: KUBEMQ_JWT=<your-token> npx tsx connectivity/auth-jwt/index.ts
 */
import mqtt, { type MqttClient, type IConnackPacket, type Packet } from 'mqtt';

function brokerUrl(): string {
  return process.env['KUBEMQ_MQTT_URL'] ?? 'tcp://localhost:1883';
}

function jwtToken(): string {
  return process.env['KUBEMQ_JWT'] ?? '';
}

type ConnectOutcome =
  | { outcome: 'connected' }
  | { outcome: 'auth-rejected'; code: number }
  | { outcome: 'protocol-error'; code: number; reason: string }
  | { outcome: 'rejected'; code: number };

async function tryConnect(
  clientId: string,
  password: string,
  label: string,
  username?: string,
): Promise<ConnectOutcome> {
  return new Promise((resolve) => {
    // IMPORTANT: only set username/password when explicitly provided.
    // MQTT.js sets the CONNECT password FLAG whenever a username is present,
    // even with an empty-string password — and a flag-set-but-no-value CONNECT
    // is a wire-protocol violation (CONNACK 0x82 "protocol error"), NOT an auth
    // failure. To exercise the real auth path with an empty password we must
    // send the CONNECT with NO username so the password flag is not forced.
    const opts: Parameters<typeof mqtt.connect>[1] = {
      clientId,
      protocolVersion: 5,
      clean: true,
      keepalive: 30,
    };
    if (username !== undefined) opts.username = username;
    if (password.length > 0) opts.password = password;

    const client = mqtt.connect(brokerUrl(), opts);

    // Capture the raw CONNACK reason code so we can tell an AUTH rejection
    // (0x86 bad-auth / 0x87 not-authorized) apart from a PROTOCOL error (0x82).
    let connackCode = -1;
    let connackReason = '';
    client.on('packetreceive', (packet: Packet) => {
      if (packet.cmd === 'connack') {
        const connack = packet as IConnackPacket;
        connackCode = connack.reasonCode ?? 0;
        connackReason = connack.properties?.reasonString ?? '';
      }
    });

    const timer = setTimeout(() => {
      client.end(true);
      resolve({ outcome: 'rejected', code: connackCode });
    }, 5_000);

    client.once('connect', () => {
      clearTimeout(timer);
      console.log(`[${label}] connected (CONNACK 0x00)`);
      client.end(true);
      resolve({ outcome: 'connected' });
    });

    client.on('error', (err) => {
      clearTimeout(timer);
      client.end(true);
      // 0x86 bad auth / 0x87 not authorized = genuine auth rejection.
      // 0x82 protocol error = malformed CONNECT (e.g. password flag w/o value).
      if (connackCode === 0x86 || connackCode === 0x87) {
        console.log(`[${label}] auth-rejected: CONNACK 0x${connackCode.toString(16)} (${err.message})`);
        resolve({ outcome: 'auth-rejected', code: connackCode });
      } else if (connackCode === 0x82) {
        console.log(`[${label}] protocol-error: CONNACK 0x82 — ${connackReason}`);
        resolve({ outcome: 'protocol-error', code: connackCode, reason: connackReason });
      } else {
        console.log(`[${label}] rejected: ${err.message} (CONNACK 0x${(connackCode >>> 0).toString(16)})`);
        resolve({ outcome: 'rejected', code: connackCode });
      }
    });
  });
}

async function pubSubWithAuth(token: string): Promise<void> {
  const url = brokerUrl();
  const topic = 'events/demo/auth';

  const subscriber: MqttClient = mqtt.connect(url, {
    clientId: 'js-mqtt-auth-sub',
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
    username: 'kubemq-js-example',
    password: token,
  });

  await new Promise<void>((resolve, reject) => {
    subscriber.once('connect', () => resolve());
    subscriber.on('error', (err) => {
      reject(new Error(`Subscriber auth failed: ${err.message}`));
    });
  });

  console.log('[subscriber] authenticated and connected');

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
    clientId: 'js-mqtt-auth-pub',
    protocolVersion: 5,
    clean: true,
    keepalive: 30,
    username: 'kubemq-js-example',
    password: token,
  });

  await new Promise<void>((resolve, reject) => {
    publisher.once('connect', () => resolve());
    publisher.on('error', (err) => {
      reject(new Error(`Publisher auth failed: ${err.message}`));
    });
  });

  console.log('[publisher] authenticated and connected');

  await new Promise<void>((resolve, reject) => {
    publisher.publish(
      topic,
      JSON.stringify({ authenticated: true, message: 'Hello with JWT auth!' }),
      { qos: 1 },
      (err) => {
        if (err) { reject(err); return; }
        console.log(`[publisher] published to ${topic}`);
        resolve();
      },
    );
  });

  const timeout = new Promise<void>((_, reject) =>
    setTimeout(() => reject(new Error('Timed out waiting for auth event')), 10_000),
  );
  await Promise.race([received, timeout]);

  await publisher.endAsync();
  await subscriber.endAsync();
}

async function main(): Promise<void> {
  const token = jwtToken();

  console.log('[auth-jwt] KubeMQ MQTT password-as-JWT authentication example');
  console.log('[auth-jwt] Auth model: CONNECT.Password = JWT token (PasswordFlag=true)');
  console.log('[auth-jwt] Username field is display-only and ignored for auth decisions');
  console.log('');

  // Demonstrate the empty-password path.
  //   - Auth ENABLED  -> CONNACK 0x86 (bad auth): empty password rejected.
  //   - Auth DISABLED -> CONNACK 0x00: empty password accepted (open broker).
  // NOTE: we deliberately send NO username here. With a username present,
  // MQTT.js sets the password FLAG even for an empty password, and a
  // flag-set-but-no-value CONNECT is a protocol violation (CONNACK 0x82) that
  // would mask the real auth outcome.
  console.log('[test] empty password -> expect CONNACK 0x86 if auth is enabled, 0x00 if disabled...');
  const emptyResult = await tryConnect('js-mqtt-auth-nopass', '', 'empty-password');
  switch (emptyResult.outcome) {
    case 'auth-rejected':
      console.log('[test] empty password correctly rejected with auth CONNACK '
        + `0x${emptyResult.code.toString(16)} -> auth is ENABLED`);
      break;
    case 'connected':
      console.log('[test] empty password accepted -> auth is DISABLED on this broker (expected on open broker)');
      break;
    case 'protocol-error':
      console.log('[test] empty password produced a PROTOCOL error (0x82), not an auth result '
        + `-> ${emptyResult.reason}`);
      break;
    default:
      console.log(`[test] empty password rejected with CONNACK 0x${(emptyResult.code >>> 0).toString(16)} `
        + '(not an auth/protocol code)');
  }

  console.log('');

  if (token) {
    console.log(`[test] connecting with JWT token (length ${token.length})...`);
    await pubSubWithAuth(token);
    console.log('[done] authenticated pub/sub complete');
  } else {
    console.log('[info] KUBEMQ_JWT not set -> skipping authenticated pub/sub demo');
    console.log('[info] Set KUBEMQ_JWT=<your-token> to test JWT authentication');
    console.log('[info] On an auth-disabled broker, any non-empty password is accepted');
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output (auth-disabled broker, no KUBEMQ_JWT):
// [auth-jwt] KubeMQ MQTT password-as-JWT authentication example
// [auth-jwt] Auth model: CONNECT.Password = JWT token (PasswordFlag=true)
// [auth-jwt] Username field is display-only and ignored for auth decisions
//
// [test] empty password -> expect CONNACK 0x86 if auth is enabled, 0x00 if disabled...
// [empty-password] connected (CONNACK 0x00)
// [test] empty password accepted -> auth is DISABLED on this broker (expected on open broker)
//
// [info] KUBEMQ_JWT not set -> skipping authenticated pub/sub demo
// [info] Set KUBEMQ_JWT=<your-token> to test JWT authentication
// [info] On an auth-disabled broker, any non-empty password is accepted
