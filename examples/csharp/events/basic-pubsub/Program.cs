// Example: events/basic-pubsub
//
// Demonstrates basic MQTT 5.0 pub/sub over KubeMQ Events using MQTTnet.
//
// Topic grammar
//   - Prefix "events/" routes to the KubeMQ Events pattern.
//   - '/' in the remaining path segments maps to '.' in the KubeMQ channel name.
//     e.g. "events/demo/x" -> KubeMQ channel "demo.x"
//   - '+' is a single-level wildcard (MQTT) -> '*' in KubeMQ.
//
// This example also demonstrates MQTT 5.0 User Properties round-tripping as
// KubeMQ Tags:
//   Publisher sets:    UserProperty("k1", "v1")
//   KubeMQ stores it as a Tag; subscriber receives it in UserProperties.
//   Cap: 32 properties / 4096 bytes total; exceeding the cap -> PUBACK 0x97.
//
// GOTCHA — Retain is NOT supported:
//   A publish with Retain=true is silently dropped — PUBACK returns 0x00
//   (success), but the message is never delivered and an error is audited.
//   The broker only rejects Retain in a Will message at CONNECT (CONNACK 0x9A).
//   Do NOT set Retain=true on any publish.
//
// GOTCHA — Literal '.' in a topic segment conflates with '/':
//   "events/demo.temp" and "events/demo/temp" both map to channel "demo.temp".
//   Avoid dots in topic segments to prevent unintended channel aliasing.
//
// Run:
//   dotnet run
//   KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using System.Text;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

static string BrokerUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_MQTT_URL") ?? "tcp://localhost:1883";

// Parse host and port from URL (supports tcp:// ws:// tls:// schemes).
static (string host, int port) ParseTcpEndpoint(string url)
{
    foreach (var prefix in new[] { "tcp://", "tls://", "ws://" })
        if (url.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            url = url[prefix.Length..];
    var parts = url.TrimEnd('/').Split(':');
    return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 1883);
}

var (host, port) = ParseTcpEndpoint(BrokerUrl());

// Subscription uses a single-level wildcard '+' that matches any one-segment subtopic.
// "events/demo/+" -> KubeMQ Events channel "demo.*"
const string subTopic = "events/demo/+";

// Publisher sends to the concrete topic that matches the wildcard above.
// "events/demo/x" -> KubeMQ Events channel "demo.x"
const string pubTopic = "events/demo/x";
const string kubemqChannel = "demo.x"; // shown in logs for clarity

Console.WriteLine($"Broker:          {BrokerUrl()}");
Console.WriteLine($"Subscribe topic: {subTopic}  (KubeMQ wildcard channel demo.*)");
Console.WriteLine($"Publish topic:   {pubTopic}  (KubeMQ channel {kubemqChannel})");
Console.WriteLine();

var factory = new MqttFactory();

// ---------------------------------------------------------------------------
// 1. Subscriber — connect and subscribe BEFORE publishing.
// ---------------------------------------------------------------------------

// Capture received payload + echoed user-property (KubeMQ tag).
var received = new TaskCompletionSource<(string payload, string? tag)>(
    TaskCreationOptions.RunContinuationsAsynchronously);

using var subClient = factory.CreateMqttClient();
subClient.ApplicationMessageReceivedAsync += e =>
{
    var msg     = e.ApplicationMessage;
    var payload = Encoding.UTF8.GetString(msg.PayloadSegment);
    // KubeMQ forwards Tags back to the MQTT subscriber as User Properties (v5 only).
    string? tag = msg.UserProperties?.FirstOrDefault(p => p.Name == "k1")?.Value;
    received.TrySetResult((payload, tag));
    return Task.CompletedTask;
};

var subOptions = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"csharp-events-sub-{Guid.NewGuid():N}"[..26])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build();

Console.WriteLine("[1] Connecting subscriber...");
var subConnResult = await subClient.ConnectAsync(subOptions);
if (subConnResult.ResultCode != MqttClientConnectResultCode.Success)
    throw new Exception($"Subscriber CONNACK failed: {subConnResult.ResultCode}");
Console.WriteLine($"    Subscriber connected  (ResultCode={subConnResult.ResultCode})");

var subResult = await subClient.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f =>
            f.WithTopic(subTopic)
             .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

var subItem = subResult.Items.First();
if (subItem.ResultCode > MqttClientSubscribeResultCode.GrantedQoS2)
    throw new Exception($"SUBACK rejected for '{subTopic}': {subItem.ResultCode}");
Console.WriteLine($"    Subscribed to '{subTopic}'  (SUBACK={subItem.ResultCode})");

// Allow the broker to register the subscription before publishing.
await Task.Delay(300);

// ---------------------------------------------------------------------------
// 2. Publisher — connect and publish a message with a v5 User Property.
// ---------------------------------------------------------------------------

using var pubClient = factory.CreateMqttClient();
var pubOptions = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"csharp-events-pub-{Guid.NewGuid():N}"[..26])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build();

Console.WriteLine();
Console.WriteLine("[2] Connecting publisher...");
var pubConnResult = await pubClient.ConnectAsync(pubOptions);
if (pubConnResult.ResultCode != MqttClientConnectResultCode.Success)
    throw new Exception($"Publisher CONNACK failed: {pubConnResult.ResultCode}");
Console.WriteLine($"    Publisher connected  (ResultCode={pubConnResult.ResultCode})");

var payload = $"{{\"message\":\"hello\",\"channel\":\"{kubemqChannel}\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";

var pubResult = await pubClient.PublishAsync(
    new MqttApplicationMessageBuilder()
        .WithTopic(pubTopic)
        .WithPayload(Encoding.UTF8.GetBytes(payload))
        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        // MQTT 5.0 User Property "k1=v1" is forwarded to KubeMQ as a Tag.
        // The subscriber receives it back in UserProperties — proving the
        // bidirectional User-Props <-> Tags mapping (connector spec §2.9).
        // Cap: 32 properties / 4096 bytes total; exceeding -> PUBACK 0x97.
        .WithUserProperty("k1", "v1")
        .Build());

// Relevant PUBACK reason codes:
//   0x00 Success
//   0x80 Broker not ready (publish gated; retry after a moment)
//   0x87 Not authorized (ACL deny)
//   0x90 Topic-name-invalid (empty channel or DefaultPattern=none configured)
//   0x97 Quota exceeded (user-property cap exceeded or RpcMaxPending)
if (pubResult.ReasonCode != MqttClientPublishReasonCode.Success)
    throw new Exception($"PUBACK reason code: {pubResult.ReasonCode} (0x{(int)pubResult.ReasonCode:X2})");
Console.WriteLine($"    Published to '{pubTopic}' with UserProperty k1=v1  (PUBACK=Success)");

// ---------------------------------------------------------------------------
// 3. Wait for the event to arrive at the subscriber.
// ---------------------------------------------------------------------------

Console.WriteLine();
Console.WriteLine("[3] Waiting for event delivery...");
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
try
{
    var (body, tag) = await received.Task.WaitAsync(cts.Token);
    Console.WriteLine($"    Received payload:       {body}");
    Console.WriteLine($"    Received UserProperty:  k1={tag ?? "(not present)"}");
    if (tag == "v1")
        Console.WriteLine("    User-property 'k1=v1' round-tripped as a KubeMQ Tag.");
    else
        Console.WriteLine("    NOTE: user-property not echoed back — " +
                          "verify KubeMQ tagsToUserProps is enabled.");
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("ERROR: Timed out after 10 s waiting for event delivery.");
    Environment.Exit(1);
}

// ---------------------------------------------------------------------------
// 4. Clean disconnect.
// ---------------------------------------------------------------------------

Console.WriteLine();
Console.WriteLine("[4] Disconnecting...");
await subClient.DisconnectAsync();
await pubClient.DisconnectAsync();
Console.WriteLine("    Done.");

// ---------------------------------------------------------------------------
// Expected output (approximate):
//
// Broker:          tcp://127.0.0.1:1883
// Subscribe topic: events/demo/+  (KubeMQ wildcard channel demo.*)
// Publish topic:   events/demo/x  (KubeMQ channel demo.x)
//
// [1] Connecting subscriber...
//     Subscriber connected  (ResultCode=Success)
//     Subscribed to 'events/demo/+'  (SUBACK=GrantedQoS1)
//
// [2] Connecting publisher...
//     Publisher connected  (ResultCode=Success)
//     Published to 'events/demo/x' with UserProperty k1=v1  (PUBACK=Success)
//
// [3] Waiting for event delivery...
//     Received payload:       {"message":"hello","channel":"demo.x","ts":"..."}
//     Received UserProperty:  k1=v1
//     User-property 'k1=v1' round-tripped as a KubeMQ Tag.
//
// [4] Disconnecting...
//     Done.
// ---------------------------------------------------------------------------
