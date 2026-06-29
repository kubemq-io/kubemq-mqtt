// Example: rpc/query-round-trip
//
// Demonstrates KubeMQ Queries RPC over MQTT 5.0.
// MQTT 5.0 ONLY — v3.1.1 publish to queries/ is silently dropped.
//
// Flow:
//   1. Client subscribes to its OWN reply topic $reply/<clientID>/inbox (QoS 1) FIRST.
//   2. Client publishes to queries/demo/rpc with:
//        Properties.ResponseTopic = $reply/<clientID>/inbox    (must be own namespace)
//        Properties.CorrelationData = <request-id bytes>
//   3. PUBACK is sent IMMEDIATELY (before the response) — client implements its own timeout.
//   4. gRPC responder (channel demo.rpc, type query) answers; connector forwards response
//      to ResponseTopic.
//   5. Query response: body + user-props kubemq-metadata + tags, CorrelationData echoed.
//
// NOTE: A running gRPC responder is required (see .work/test-harness/responder/):
//       responder -channel demo.rpc -type query
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

// clientId trimmed to 23 chars so $reply/<23-char-id>/inbox stays well inside limits
var clientId   = $"csharp-mqtt-rpc-q-{Guid.NewGuid():N}"[..23];
var requestId  = Guid.NewGuid().ToString("N");
var replyTopic = $"$reply/{clientId}/inbox";
const string queryTopic = "queries/demo/rpc";

var factory = new MqttFactory();
var responseReceived = new TaskCompletionSource<(string body, string correlationId, string metadata, string tags)>(
    TaskCreationOptions.RunContinuationsAsynchronously);

using var client = factory.CreateMqttClient();

client.ApplicationMessageReceivedAsync += e =>
{
    var msg = e.ApplicationMessage;
    var body = Encoding.UTF8.GetString(msg.PayloadSegment);
    var correlationId = msg.CorrelationData != null
        ? Encoding.UTF8.GetString(msg.CorrelationData)
        : string.Empty;

    // Query response includes user-props: kubemq-metadata + any tags set by responder
    var metadataValue = string.Empty;
    var tagParts = new List<string>();
    if (msg.UserProperties != null)
    {
        foreach (var prop in msg.UserProperties)
        {
            if (prop.Name == "kubemq-metadata")
                metadataValue = prop.Value;
            else
                tagParts.Add($"{prop.Name}={prop.Value}");
        }
    }

    responseReceived.TrySetResult((body, correlationId, metadataValue, string.Join(", ", tagParts)));
    return Task.CompletedTask;
};

var options = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId(clientId)
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build();

Console.WriteLine($"Connecting to {BrokerUrl()} as '{clientId}'...");
await client.ConnectAsync(options);
Console.WriteLine("Connected");

// Step 1: subscribe to own reply inbox BEFORE publishing
var subResult = await client.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f => f.WithTopic(replyTopic).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

var subCode = subResult.Items.First().ResultCode;
if (subCode > MqttClientSubscribeResultCode.GrantedQoS2)
    throw new Exception($"Reply-topic subscribe rejected: SUBACK {(int)subCode:X2} ({subCode}). " +
                        "Ensure ResponseTopic is in own $reply/<clientID>/ namespace.");
Console.WriteLine($"Subscribed to reply inbox '{replyTopic}' (QoS {subCode})");

// Brief pause to ensure subscription is registered before publishing
await Task.Delay(200);

// Step 2: publish query with ResponseTopic + CorrelationData
var payload = $"{{\"query\":\"hello\",\"requestId\":\"{requestId}\"}}";

Console.WriteLine($"Publishing query to '{queryTopic}'...");
var pubResult = await client.PublishAsync(
    new MqttApplicationMessageBuilder()
        .WithTopic(queryTopic)
        .WithPayload(Encoding.UTF8.GetBytes(payload))
        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        .WithResponseTopic(replyTopic)
        .WithCorrelationData(Encoding.UTF8.GetBytes(requestId))
        .Build());

// PUBACK arrives immediately (before execution) — design of the MQTT connector
if (pubResult.ReasonCode != MqttClientPublishReasonCode.Success)
    throw new Exception($"Publish failed: PUBACK reason code {(int)pubResult.ReasonCode:X2} ({pubResult.ReasonCode})");
Console.WriteLine($"PUBACK received (immediate, before response) — waiting for query response...");

// Step 3: wait for response on reply inbox (client MUST implement own timeout)
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
try
{
    var (body, corrId, metadata, tags) = await responseReceived.Task.WaitAsync(cts.Token);
    Console.WriteLine("Query response received:");
    Console.WriteLine($"  correlation-id   : {corrId}");
    Console.WriteLine($"  body             : {body}");
    if (!string.IsNullOrEmpty(metadata))
        Console.WriteLine($"  kubemq-metadata  : {metadata}");
    if (!string.IsNullOrEmpty(tags))
        Console.WriteLine($"  tags             : {tags}");
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("ERROR: Timed out waiting for query response.");
    Console.Error.WriteLine("       Is the gRPC responder running?");
    Console.Error.WriteLine("       Start it with: responder -channel demo.rpc -type query");
    Environment.Exit(1);
}

await client.DisconnectAsync();
Console.WriteLine("Disconnected. Done.");

// Expected output (with responder running on channel demo.rpc):
// Connecting to tcp://127.0.0.1:1883 as 'csharp-mqtt-rpc-q-...'...
// Connected
// Subscribed to reply inbox '$reply/csharp-mqtt-rpc-q-.../inbox' (QoS GrantedQoS1)
// Publishing query to 'queries/demo/rpc'...
// PUBACK received (immediate, before response) — waiting for query response...
// Query response received:
//   correlation-id   : <requestId>
//   body             : pong:{"query":"hello","requestId":"..."}
//   kubemq-metadata  : responder/query
//   tags             : responder=mqtt-test, rpc-type=query
// Disconnected. Done.
