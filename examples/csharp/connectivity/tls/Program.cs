// Example: connectivity/tls
//
// Demonstrates a TLS connection to the KubeMQ MQTT connector (MQTT 5.0).
// The tls:// scheme in KUBEMQ_MQTT_URL selects encrypted TCP transport.
//
// Wire contract (from connector spec):
//   - Topic "events/demo/tls" -> KubeMQ Events channel "demo.tls"
//   - QoS 1 (AtLeastOnce)
//   - MQTT 5.0 (v500); TLS port 8883
//   - InsecureSkipVerify is acceptable for examples; use a proper CA in production.
//
// NOTE: The reference broker at tls://127.0.0.1:8883 has its TLS listener
// CLOSED on this deployment. This example compiles and runs correctly when TLS
// is enabled on the broker; live testing is N/A for this broker instance.
//
// Reason codes to watch:
//   0x00 Success
//   0x80 Broker not ready (publish gated — retry after a moment)
//   0x86 Bad credentials (empty/invalid password when auth enabled)
//   0x87 Not authorized (ACL deny)
//   0x90 Topic-name-invalid (empty channel or DefaultPattern=none configured)
//   0x97 Quota exceeded (user-property cap or RpcMaxPending)
//
// Run:
//   dotnet run
//   KUBEMQ_MQTT_URL=tls://127.0.0.1:8883 dotnet run

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using System.Text;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

static string BrokerUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_MQTT_URL") ?? "tls://localhost:8883";

// Parse host, port, and scheme from the URL.
// Supports tcp://, tls://, and ws:// prefixes.
static (string host, int port, bool isTls) ParseEndpoint(string url)
{
    bool tls = url.StartsWith("tls://", StringComparison.OrdinalIgnoreCase);
    foreach (var prefix in new[] { "tcp://", "tls://", "ws://", "wss://" })
        if (url.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            url = url[prefix.Length..];
    var parts = url.TrimEnd('/').Split(':');
    int port = parts.Length > 1 ? int.Parse(parts[1]) : (tls ? 8883 : 1883);
    return (parts[0], port, tls);
}

var (host, port, isTls) = ParseEndpoint(BrokerUrl());

// Spec-mandated topics for this variant (§6.1.8):
//   subscribe: events/demo/tls  -> KubeMQ Events channel "demo.tls"
//   publish:   events/demo/tls  -> KubeMQ Events channel "demo.tls"
const string subTopic     = "events/demo/tls";
const string pubTopic     = "events/demo/tls";
const string kubemqCh     = "demo.tls"; // shown in logs

Console.WriteLine($"Broker:          {BrokerUrl()}");
Console.WriteLine($"Subscribe topic: {subTopic}  (KubeMQ channel {kubemqCh})");
Console.WriteLine($"Publish topic:   {pubTopic}  (KubeMQ channel {kubemqCh})");
Console.WriteLine($"Transport:       {(isTls ? "TLS" : "plain TCP (set KUBEMQ_MQTT_URL=tls://... for TLS)")}");
Console.WriteLine();

var factory = new MqttFactory();

// ---------------------------------------------------------------------------
// 1. Build TLS options — shared by both subscriber and publisher.
// ---------------------------------------------------------------------------

MqttClientOptions BuildOptions(string clientId)
{
    var b = new MqttClientOptionsBuilder()
        .WithClientId(clientId)
        .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
        .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
        .WithCleanSession(true);

    if (isTls)
    {
        // InsecureSkipVerify (CertificateValidationHandler returns true) is
        // acceptable for development / self-signed certs. In production,
        // validate against your CA certificate instead:
        //   tls.WithCertificateValidationHandler(ctx =>
        //       ctx.Chain.Build(ctx.Certificate));
        b = b
            .WithTlsOptions(tls => tls.WithCertificateValidationHandler(_ => true))
            .WithTcpServer(host, port);
    }
    else
    {
        b = b.WithTcpServer(host, port);
    }

    return b.Build();
}

// ---------------------------------------------------------------------------
// 2. Subscriber — connect and subscribe BEFORE the publisher sends.
// ---------------------------------------------------------------------------

var received = new TaskCompletionSource<string>(
    TaskCreationOptions.RunContinuationsAsynchronously);

using var subClient = factory.CreateMqttClient();
subClient.ApplicationMessageReceivedAsync += e =>
{
    var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
    received.TrySetResult(payload);
    return Task.CompletedTask;
};

Console.WriteLine("[1] Connecting subscriber...");
try
{
    var subConn = await subClient.ConnectAsync(BuildOptions(
        $"csharp-tls-sub-{Guid.NewGuid():N}"[..22]));

    if (subConn.ResultCode != MqttClientConnectResultCode.Success)
        throw new Exception($"Subscriber CONNACK failed: {subConn.ResultCode}");

    Console.WriteLine($"    Subscriber connected  (TLS={isTls}, ResultCode={subConn.ResultCode})");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"    Subscriber connection failed: {ex.Message}");
    Console.Error.WriteLine("    (TLS listener may be disabled on this broker — live test is N/A)");
    Environment.Exit(1);
}

var subResult = await subClient.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f =>
            f.WithTopic(subTopic)
             .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

var subItem = subResult.Items.First();
if (subItem.ResultCode > MqttClientSubscribeResultCode.GrantedQoS2)
    throw new Exception($"SUBACK rejected for '{subTopic}': {subItem.ResultCode} " +
                        $"(0x{(int)subItem.ResultCode:X2})");

Console.WriteLine($"    Subscribed to '{subTopic}'  (SUBACK={subItem.ResultCode})");

// Allow the broker to register the subscription before publishing.
await Task.Delay(300);

// ---------------------------------------------------------------------------
// 3. Publisher — connect and publish over TLS.
// ---------------------------------------------------------------------------

using var pubClient = factory.CreateMqttClient();

Console.WriteLine();
Console.WriteLine("[2] Connecting publisher...");
try
{
    var pubConn = await pubClient.ConnectAsync(BuildOptions(
        $"csharp-tls-pub-{Guid.NewGuid():N}"[..22]));

    if (pubConn.ResultCode != MqttClientConnectResultCode.Success)
        throw new Exception($"Publisher CONNACK failed: {pubConn.ResultCode}");

    Console.WriteLine($"    Publisher connected  (TLS={isTls}, ResultCode={pubConn.ResultCode})");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"    Publisher connection failed: {ex.Message}");
    Console.Error.WriteLine("    (TLS listener may be disabled on this broker — live test is N/A)");
    await subClient.DisconnectAsync();
    Environment.Exit(1);
}

var msgPayload = $"{{\"message\":\"TLS connectivity test\",\"channel\":\"{kubemqCh}\"," +
                 $"\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";

var pubResult = await pubClient.PublishAsync(
    new MqttApplicationMessageBuilder()
        .WithTopic(pubTopic)
        .WithPayload(Encoding.UTF8.GetBytes(msgPayload))
        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        // MQTT 5.0 User Property -> KubeMQ Tag (bidirectional, cap 32 / 4096 bytes)
        .WithUserProperty("source", "csharp-tls-example")
        .Build());

if (pubResult.ReasonCode != MqttClientPublishReasonCode.Success)
    throw new Exception($"PUBACK reason code: {pubResult.ReasonCode} (0x{(int)pubResult.ReasonCode:X2})");

Console.WriteLine($"    Published to '{pubTopic}' with UserProperty source=csharp-tls-example  (PUBACK=Success)");

// ---------------------------------------------------------------------------
// 4. Wait for the event to arrive at the subscriber.
// ---------------------------------------------------------------------------

Console.WriteLine();
Console.WriteLine("[3] Waiting for event delivery (10 s timeout)...");
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
try
{
    var body = await received.Task.WaitAsync(cts.Token);
    Console.WriteLine($"    Received payload: {body}");
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("ERROR: Timed out after 10 s waiting for event delivery.");
    Environment.Exit(1);
}

// ---------------------------------------------------------------------------
// 5. Clean disconnect.
// ---------------------------------------------------------------------------

Console.WriteLine();
Console.WriteLine("[4] Disconnecting...");
await subClient.DisconnectAsync();
await pubClient.DisconnectAsync();
Console.WriteLine("    Done.");

// ---------------------------------------------------------------------------
// Expected output (TLS-enabled broker):
//
// Broker:          tls://127.0.0.1:8883
// Subscribe topic: events/demo/tls  (KubeMQ channel demo.tls)
// Publish topic:   events/demo/tls  (KubeMQ channel demo.tls)
// Transport:       TLS
//
// [1] Connecting subscriber...
//     Subscriber connected  (TLS=True, ResultCode=Success)
//     Subscribed to 'events/demo/tls'  (SUBACK=GrantedQoS1)
//
// [2] Connecting publisher...
//     Publisher connected  (TLS=True, ResultCode=Success)
//     Published to 'events/demo/tls' with UserProperty source=csharp-tls-example  (PUBACK=Success)
//
// [3] Waiting for event delivery (10 s timeout)...
//     Received payload: {"message":"TLS connectivity test","channel":"demo.tls","ts":"..."}
//
// [4] Disconnecting...
//     Done.
// ---------------------------------------------------------------------------
