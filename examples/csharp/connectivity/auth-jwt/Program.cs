// Example: connectivity/auth-jwt
//
// Demonstrates JWT authentication when connecting to the KubeMQ MQTT connector.
// KubeMQ uses the MQTT CONNECT Password field to carry the JWT token (PasswordFlag=true).
// The Username field is DISPLAY-ONLY and is NOT used for auth checks.
//
// Auth flow:
//   1. Read JWT from KUBEMQ_MQTT_TOKEN (or KUBEMQ_MQTT_JWT) env var.
//   2. Open MQTT 5.0 connection with WithCredentials(clientId, jwt).
//   3. Broker returns CONNACK 0x00 (success) or 0x86 (bad/missing JWT when auth enabled).
//   4. Publish a test event to events/demo/auth to prove the session works.
//   5. Disconnect cleanly.
//
// Live-test note: The reference broker has auth DISABLED.
// - The example connects and publishes successfully even without a JWT.
// - JWT rejection (CONNACK 0x86) and ACL denial (0x87) require an auth-enabled broker.
// - Mark live test as PARTIAL / N/A on auth-disabled deployments.
//
// Set KUBEMQ_MQTT_TOKEN (or KUBEMQ_MQTT_JWT) to a valid JWT for an auth-enabled broker.
//
// Run:
//   dotnet run
//   KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 KUBEMQ_MQTT_TOKEN=<token> dotnet run

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using System.Text;

// ── Configuration ────────────────────────────────────────────────────────────

static string BrokerUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_MQTT_URL") ?? "tcp://localhost:1883";

// Accept both KUBEMQ_MQTT_TOKEN (preferred) and KUBEMQ_MQTT_JWT (legacy alias)
static string? JwtToken() =>
    Environment.GetEnvironmentVariable("KUBEMQ_MQTT_TOKEN")
    ?? Environment.GetEnvironmentVariable("KUBEMQ_MQTT_JWT");

static (string host, int port) ParseTcpEndpoint(string url)
{
    foreach (var prefix in new[] { "tcp://", "tls://", "ws://" })
        if (url.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            url = url[prefix.Length..];
    var parts = url.TrimEnd('/').Split(':');
    return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 1883);
}

// ── Main ─────────────────────────────────────────────────────────────────────

var brokerUrl = BrokerUrl();
var (host, port) = ParseTcpEndpoint(brokerUrl);
var jwt = JwtToken();

Console.WriteLine("=== KubeMQ MQTT — connectivity/auth-jwt ===");
Console.WriteLine($"[1] Broker URL : {brokerUrl}");

if (string.IsNullOrWhiteSpace(jwt))
{
    Console.WriteLine("[1] KUBEMQ_MQTT_TOKEN not set — connecting WITHOUT credentials.");
    Console.WriteLine("    To test JWT auth: export KUBEMQ_MQTT_TOKEN=<your-token>");
    jwt = null;
}
else
{
    Console.WriteLine($"[1] JWT found  : {jwt[..Math.Min(20, jwt.Length)]}... (truncated for log)");
}

// Client ID must be unique per connection
var clientId = $"csharp-auth-{Guid.NewGuid():N}"[..24];
Console.WriteLine($"[2] Client ID  : {clientId}");

var factory = new MqttFactory();
using var client = factory.CreateMqttClient();

// Build MQTT 5.0 connect options
// KubeMQ MQTT connector: Password field = JWT token, Username = clientId (display-only)
var optionsBuilder = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId(clientId)
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true);

if (jwt is not null)
{
    // Username is display-only; Password carries the JWT token
    optionsBuilder = optionsBuilder.WithCredentials(clientId, jwt);
    Console.WriteLine("[3] Credentials: Username=<clientId> (display-only), Password=<JWT>");
}
else
{
    Console.WriteLine("[3] Credentials: none (auth-disabled broker accepts open connections)");
}

// ── Step 4: Connect ───────────────────────────────────────────────────────────

Console.WriteLine($"[4] Connecting to {host}:{port} via MQTT 5.0 ...");

MqttClientConnectResult connResult;
try
{
    connResult = await client.ConnectAsync(optionsBuilder.Build());
}
catch (Exception ex)
{
    Console.Error.WriteLine($"[4] TCP connection failed: {ex.Message}");
    Environment.Exit(1);
    return;
}

switch (connResult.ResultCode)
{
    case MqttClientConnectResultCode.Success:
        Console.WriteLine($"[4] CONNACK 0x00: Connected successfully.");
        if (jwt is null)
            Console.WriteLine("    (Auth disabled on broker — JWT enforcement not exercised)");
        else
            Console.WriteLine("    (JWT accepted — full enforcement requires auth-enabled broker)");
        break;

    case MqttClientConnectResultCode.BadUserNameOrPassword:
        Console.Error.WriteLine("[4] CONNACK 0x86: Bad credentials — empty or invalid JWT.");
        Console.Error.WriteLine("    Auth is ENABLED on this broker. Provide a valid KUBEMQ_MQTT_TOKEN.");
        Environment.Exit(1);
        return;

    case MqttClientConnectResultCode.NotAuthorized:
        Console.Error.WriteLine("[4] CONNACK 0x87: Not authorized — ACL denied this client.");
        Environment.Exit(1);
        return;

    case MqttClientConnectResultCode.ServerUnavailable:
        Console.Error.WriteLine("[4] CONNACK 0x80: Broker not ready (server unavailable).");
        Environment.Exit(1);
        return;

    default:
        Console.Error.WriteLine($"[4] CONNACK {(int)connResult.ResultCode}: Unexpected result: {connResult.ResultCode}");
        Environment.Exit(1);
        return;
}

// ── Step 5: Subscribe to events/demo/auth ────────────────────────────────────

const string topic = "events/demo/auth";
Console.WriteLine($"[5] Subscribing to '{topic}' at QoS 1 ...");

var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

client.ApplicationMessageReceivedAsync += args =>
{
    var body = Encoding.UTF8.GetString(args.ApplicationMessage.PayloadSegment);
    Console.WriteLine($"[recv] topic={args.ApplicationMessage.Topic} payload={body}");
    received.TrySetResult(body);
    return Task.CompletedTask;
};

var subResult = await client.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(topic, MqttQualityOfServiceLevel.AtLeastOnce)
        .Build());

foreach (var item in subResult.Items)
    Console.WriteLine($"[5] SUBACK: topic={item.TopicFilter.Topic} granted={item.ResultCode}");

// ── Step 6: Publish a test event ──────────────────────────────────────────────

var payload = $"{{\"msg\":\"auth connectivity test\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";
Console.WriteLine($"[6] Publishing to '{topic}' ...");

var pubResult = await client.PublishAsync(
    new MqttApplicationMessageBuilder()
        .WithTopic(topic)
        .WithPayload(Encoding.UTF8.GetBytes(payload))
        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        .Build());

Console.WriteLine($"[6] PUBACK reason: {pubResult.ReasonCode}");

if (pubResult.ReasonCode != MqttClientPublishReasonCode.Success)
{
    Console.Error.WriteLine($"[6] Publish rejected: {pubResult.ReasonCode}");
    await client.DisconnectAsync();
    Environment.Exit(1);
    return;
}

// ── Step 7: Wait for echo (up to 3 s) ────────────────────────────────────────

Console.WriteLine("[7] Waiting for echo ...");
var timeout = Task.Delay(TimeSpan.FromSeconds(3));
var completed = await Task.WhenAny(received.Task, timeout);

if (completed == timeout)
    Console.WriteLine("[7] No echo within 3 s (expected if no other subscriber — publish succeeded).");
else
    Console.WriteLine($"[7] Echo received: {received.Task.Result}");

// ── Step 8: Disconnect ────────────────────────────────────────────────────────

await client.DisconnectAsync();
Console.WriteLine("[8] Disconnected cleanly.");
Console.WriteLine();
Console.WriteLine("Summary:");
Console.WriteLine($"  Broker   : {host}:{port}");
Console.WriteLine($"  ClientID : {clientId}");
Console.WriteLine($"  JWT used : {(jwt is not null ? "yes" : "no (auth-disabled broker)")}");
Console.WriteLine($"  Publish  : PUBACK={pubResult.ReasonCode}");

// ── Expected output (auth-disabled broker, no JWT) ───────────────────────────
// === KubeMQ MQTT — connectivity/auth-jwt ===
// [1] Broker URL : tcp://localhost:1883
// [1] KUBEMQ_MQTT_TOKEN not set — connecting WITHOUT credentials.
// ...
// [4] CONNACK 0x00: Connected successfully.
//     (Auth disabled on broker — JWT enforcement not exercised)
// [5] SUBACK: topic=events/demo/auth granted=GrantedQoS1
// [6] PUBACK reason: Success
// [7] No echo within 3 s (expected if no other subscriber — publish succeeded).
// [8] Disconnected cleanly.
