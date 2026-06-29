// Example: rpc/command-round-trip
//
// Demonstrates KubeMQ Commands RPC over MQTT 5.0.
// MQTT 5.0 ONLY — v3.1.1 publish to commands/ is silently dropped.
//
// Flow:
//   1. Client subscribes to its OWN reply topic $reply/<clientID>/inbox (must be own namespace).
//   2. Client publishes to commands/demo/cmd (KubeMQ channel demo.cmd) with
//      Properties.ResponseTopic and Properties.CorrelationData set.
//   3. PUBACK is sent IMMEDIATELY on receipt — before the gRPC responder executes.
//   4. gRPC responder handles the command and replies; connector delivers
//      the response to the ResponseTopic.
//   5. Command response: NO body + user-props kubemq-executed("true"/"false") + optional kubemq-error.
//      CorrelationData is echoed on the response.
//
// NOTE: A running gRPC responder is required on channel demo.cmd (type=command).
//       Start it with: responder -channel demo.cmd -type command
//       (see .work/test-harness/responder/)
//
// Run: dotnet run
//      KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using System.Text;

static string BrokerUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_MQTT_URL") ?? "tcp://localhost:1883";

static (string host, int port) ParseTcpEndpoint(string url)
{
    foreach (var prefix in new[] { "tcp://", "tls://", "ws://" })
        if (url.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            url = url[prefix.Length..];
    var parts = url.TrimEnd('/').Split(':');
    return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 1883);
}

var (host, port) = ParseTcpEndpoint(BrokerUrl());

// ClientID is kept short enough to stay well within MQTT 23-char v3.1.1 limit.
// The $reply/<clientID>/... namespace is mochi-local: no routing to KubeMQ array.
var clientId      = $"csharp-cmd-{Guid.NewGuid():N}"[..22];
var requestId     = Guid.NewGuid().ToString("N");
var replyTopic    = $"$reply/{clientId}/inbox";
// commands/demo/cmd  ->  KubeMQ channel demo.cmd  (topic '/' becomes '.' in channel name)
const string commandTopic = "commands/demo/cmd";

var factory = new MqttFactory();
var responseReceived = new TaskCompletionSource<(bool executed, string? errorMsg, string correlationId)>(
    TaskCreationOptions.RunContinuationsAsynchronously);

using var client = factory.CreateMqttClient();

// Handler fires when the command response arrives on the reply topic.
// Command response: NO body; result conveyed entirely via MQTT 5.0 User Properties.
client.ApplicationMessageReceivedAsync += e =>
{
    var msg = e.ApplicationMessage;
    var correlationId = msg.CorrelationData != null
        ? Encoding.UTF8.GetString(msg.CorrelationData)
        : string.Empty;

    string? executed = null;
    string? kubemqError = null;
    if (msg.UserProperties != null)
    {
        foreach (var prop in msg.UserProperties)
        {
            if (prop.Name == "kubemq-executed") executed = prop.Value;
            if (prop.Name == "kubemq-error")    kubemqError = prop.Value;
        }
    }
    responseReceived.TrySetResult((executed == "true", kubemqError, correlationId));
    return Task.CompletedTask;
};

var options = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId(clientId)
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build();

Console.WriteLine($"[1] Connecting to {host}:{port} as MQTT 5.0...");
await client.ConnectAsync(options);
Console.WriteLine($"    Connected as '{clientId}'");

// Step 1: subscribe to own reply topic BEFORE publishing.
// The ResponseTopic on the publish MUST be in own $reply/<clientID>/... namespace —
// a foreign namespace returns PUBACK 0x83.
Console.WriteLine($"[2] Subscribing to reply topic '{replyTopic}'...");
var subResult = await client.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f => f.WithTopic(replyTopic).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

var subCode = subResult.Items.First().ResultCode;
if (subCode > MqttClientSubscribeResultCode.GrantedQoS2)
    throw new Exception($"Reply-topic SUBACK rejected: {subCode} (0x{(int)subCode:X2}) — check $reply namespace and broker readiness");
Console.WriteLine($"    Subscribed (result={subCode})");

// Brief pause ensures the subscription is registered before the publish.
await Task.Delay(200);

// Step 2: publish command with ResponseTopic + CorrelationData.
// PUBACK arrives immediately on receipt (before execution) — this is by design.
// The client MUST implement its own response-wait timeout.
var payload = $"{{\"action\":\"ping\",\"requestId\":\"{requestId}\"}}";

Console.WriteLine($"[3] Publishing command to '{commandTopic}'...");
var pubResult = await client.PublishAsync(
    new MqttApplicationMessageBuilder()
        .WithTopic(commandTopic)
        .WithPayload(Encoding.UTF8.GetBytes(payload))
        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        .WithResponseTopic(replyTopic)
        .WithCorrelationData(Encoding.UTF8.GetBytes(requestId))
        .Build());

if (pubResult.ReasonCode != MqttClientPublishReasonCode.Success)
    throw new Exception($"PUBACK reason: {pubResult.ReasonCode} (0x{(int)pubResult.ReasonCode:X2}) — " +
        "0x80=broker not ready, 0x83=missing/foreign ResponseTopic, 0x97=RpcMaxPending exceeded");
Console.WriteLine($"    PUBACK received immediately (before execution) — waiting for command response...");

// Step 3: wait for command response (implement own timeout — PUBACK is NOT the response).
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
try
{
    var (executed, error, corrId) = await responseReceived.Task.WaitAsync(cts.Token);
    Console.WriteLine("[4] Command response received:");
    Console.WriteLine($"    correlation-id   : {corrId}");
    Console.WriteLine($"    kubemq-executed  : {executed}");
    if (!string.IsNullOrEmpty(error))
        Console.WriteLine($"    kubemq-error     : {error}");
    Console.WriteLine(executed
        ? "    Result: SUCCESS"
        : $"    Result: FAILURE ({error ?? "no error detail"})");
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("[ERROR] Timed out waiting for command response.");
    Console.Error.WriteLine("        Is the gRPC responder running on channel demo.cmd?");
    Console.Error.WriteLine("        Start it with: responder -channel demo.cmd -type command");
    Environment.Exit(1);
}

Console.WriteLine("[5] Disconnecting.");
await client.DisconnectAsync();
Console.WriteLine("    Done.");

// Expected output (with responder running on channel demo.cmd):
// [1] Connecting to 127.0.0.1:1883 as MQTT 5.0...
//     Connected as 'csharp-cmd-<suffix>'
// [2] Subscribing to reply topic '$reply/csharp-cmd-<suffix>/inbox'...
//     Subscribed (result=GrantedQoS1)
// [3] Publishing command to 'commands/demo/cmd'...
//     PUBACK received immediately (before execution) — waiting for command response...
// [4] Command response received:
//     correlation-id   : <requestId>
//     kubemq-executed  : True
//     Result: SUCCESS
// [5] Disconnecting.
//     Done.
