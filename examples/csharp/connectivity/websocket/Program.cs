// Example: connectivity/websocket
//
// Demonstrates MQTT 5.0 over WebSocket transport connecting to the KubeMQ
// MQTT connector, then exercises a basic Events pub/sub round-trip.
//
// Transport:    WebSocket — ws://host:8083/  (path "/" is required by KubeMQ)
// MQTT version: MQTT 5.0
// Pattern:      KubeMQ Events  (topic prefix "events/")
// Channel:      events/demo/ws  ->  KubeMQ channel "demo.ws"
// QoS:          1 (AtLeastOnce) — QoS 0 is fine for Events; QoS 1 gives PUBACK
//
// The WS listener (port 8083) accepts both MQTT 3.1.1 and MQTT 5.0 clients.
// Integration test #19 (TestMqttIntegration_WebSocketListener) uses a v3.1.1
// client as a convenience — this example uses MQTT v5, which is equally valid.
//
// GOTCHA — Retain is NOT supported:
//   A PUBLISH with Retain=true is silently stripped: PUBACK returns 0x00 (success)
//   but the message is never delivered.  Do NOT set Retain on any publish.
//
// GOTCHA — Literal '.' in a topic segment conflates with '/':
//   "events/demo.ws" and "events/demo/ws" both map to KubeMQ channel "demo.ws".
//   Avoid dots in topic segments to prevent unintended channel aliasing.
//
// Run:
//   dotnet run
//   KUBEMQ_MQTT_URL=ws://127.0.0.1:8083/ dotnet run

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using System.Text;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

static string BrokerUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_MQTT_URL") ?? "ws://localhost:8083/";

static string ParseWsUri(string url)
{
    bool isWs = url.StartsWith("ws://", StringComparison.OrdinalIgnoreCase)
             || url.StartsWith("wss://", StringComparison.OrdinalIgnoreCase);
    if (!isWs)
    {
        Console.Error.WriteLine($"Expected a ws:// or wss:// URL, got: {url}");
        Console.Error.WriteLine("Set KUBEMQ_MQTT_URL=ws://127.0.0.1:8083/");
        Environment.Exit(1);
    }
    return url;
}

var wsUri = ParseWsUri(BrokerUrl());

// Spec §6.1.9: sub and pub on "events/demo/ws" (-> KubeMQ channel "demo.ws")
const string subTopic     = "events/demo/ws";
const string pubTopic     = "events/demo/ws";
const string kubemqChannel = "demo.ws";

Console.WriteLine($"Broker (WebSocket): {wsUri}");
Console.WriteLine($"Subscribe topic:    {subTopic}  (KubeMQ channel {kubemqChannel})");
Console.WriteLine($"Publish topic:      {pubTopic}  (KubeMQ channel {kubemqChannel})");
Console.WriteLine();

var factory = new MqttFactory();

// ---------------------------------------------------------------------------
// 1. Subscriber — connect over WebSocket and subscribe BEFORE publishing.
// ---------------------------------------------------------------------------

var received = new TaskCompletionSource<(string payload, string? tag)>(
    TaskCreationOptions.RunContinuationsAsynchronously);

using var subClient = factory.CreateMqttClient();
subClient.ApplicationMessageReceivedAsync += e =>
{
    var msg  = e.ApplicationMessage;
    var body = Encoding.UTF8.GetString(msg.PayloadSegment);
    // MQTT 5.0 User Properties are forwarded as KubeMQ Tags (bidirectional).
    string? tag = msg.UserProperties?.FirstOrDefault(p => p.Name == "source")?.Value;
    received.TrySetResult((body, tag));
    return Task.CompletedTask;
};

var subOpts = new MqttClientOptionsBuilder()
    .WithClientId($"csharp-ws-sub-{Guid.NewGuid():N}"[..24])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .WithWebSocketServer(o => o.WithUri(wsUri))
    .Build();

Console.WriteLine("[1] Connecting subscriber over WebSocket...");
MqttClientConnectResult subConn;
try
{
    subConn = await subClient.ConnectAsync(subOpts);
}
catch (Exception ex)
{
    Console.Error.WriteLine($"ERROR: Cannot connect to WebSocket endpoint {wsUri}");
    Console.Error.WriteLine($"       {ex.Message}");
    Console.Error.WriteLine("       Ensure KubeMQ is running with the WebSocket listener enabled (port 8083).");
    Environment.Exit(1);
    return;
}
if (subConn.ResultCode != MqttClientConnectResultCode.Success)
    throw new Exception($"Subscriber CONNACK failed: {subConn.ResultCode}");
Console.WriteLine($"    Subscriber connected  (ResultCode={subConn.ResultCode})");

var subResult = await subClient.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f =>
            f.WithTopic(subTopic)
             .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

var subItem = subResult.Items.First();
// Relevant SUBACK reason codes:
//   0x00-0x02  GrantedQoS0/1/2 — success
//   0x8F       TopicFilterInvalid (empty segment)
//   0xA2       WildcardSubscriptionsNotSupported (wildcard on non-Events topics)
if (subItem.ResultCode > MqttClientSubscribeResultCode.GrantedQoS2)
    throw new Exception($"SUBACK rejected for '{subTopic}': {subItem.ResultCode}");
Console.WriteLine($"    Subscribed to '{subTopic}'  (SUBACK={subItem.ResultCode})");

// Allow the broker to register the subscription before publishing.
await Task.Delay(300);

// ---------------------------------------------------------------------------
// 2. Publisher — connect over WebSocket and publish with a v5 User Property.
// ---------------------------------------------------------------------------

using var pubClient = factory.CreateMqttClient();
var pubOpts = new MqttClientOptionsBuilder()
    .WithClientId($"csharp-ws-pub-{Guid.NewGuid():N}"[..24])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .WithWebSocketServer(o => o.WithUri(wsUri))
    .Build();

Console.WriteLine();
Console.WriteLine("[2] Connecting publisher over WebSocket...");
MqttClientConnectResult pubConn;
try
{
    pubConn = await pubClient.ConnectAsync(pubOpts);
}
catch (Exception ex)
{
    Console.Error.WriteLine($"ERROR: Publisher cannot connect: {ex.Message}");
    Environment.Exit(1);
    return;
}
if (pubConn.ResultCode != MqttClientConnectResultCode.Success)
    throw new Exception($"Publisher CONNACK failed: {pubConn.ResultCode}");
Console.WriteLine($"    Publisher connected  (ResultCode={pubConn.ResultCode})");

var msgPayload = $"{{\"message\":\"hello via WebSocket\",\"channel\":\"{kubemqChannel}\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";

var pubResult = await pubClient.PublishAsync(
    new MqttApplicationMessageBuilder()
        .WithTopic(pubTopic)
        .WithPayload(Encoding.UTF8.GetBytes(msgPayload))
        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        // MQTT 5.0 User Property forwarded to KubeMQ as a Tag.
        // Cap: 32 properties / 4096 bytes total; exceeding -> PUBACK 0x97.
        // NOTE: Do NOT set Retain=true — retain is silently stripped (PUBACK 0x00,
        //       message never delivered, error audited; RetainAvailable=0 forced).
        .WithUserProperty("source", "csharp-ws-example")
        .Build());

// Relevant PUBACK reason codes:
//   0x00  Success
//   0x80  Broker not ready (publish gated; retry)
//   0x87  Not authorized (ACL deny)
//   0x90  TopicNameInvalid (empty channel / DefaultPattern=none)
//   0x97  QuotaExceeded (user-property cap or RpcMaxPending)
if (pubResult.ReasonCode != MqttClientPublishReasonCode.Success)
    throw new Exception($"PUBACK reason code: {pubResult.ReasonCode} (0x{(int)pubResult.ReasonCode:X2})");
Console.WriteLine($"    Published to '{pubTopic}' with UserProperty source=csharp-ws-example  (PUBACK=Success)");

// ---------------------------------------------------------------------------
// 3. Wait for the event to be delivered back to the subscriber.
// ---------------------------------------------------------------------------

Console.WriteLine();
Console.WriteLine("[3] Waiting for event delivery...");
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
try
{
    var (body, tag) = await received.Task.WaitAsync(cts.Token);
    Console.WriteLine($"    Received payload:       {body}");
    Console.WriteLine($"    Received UserProperty:  source={tag ?? "(not present)"}");
    if (tag == "csharp-ws-example")
        Console.WriteLine("    User-property 'source' round-tripped as a KubeMQ Tag.");
    else
        Console.WriteLine("    NOTE: user-property not echoed — check KubeMQ tagsToUserProps.");
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
// Expected output:
//
// Broker (WebSocket): ws://127.0.0.1:8083/
// Subscribe topic:    events/demo/ws  (KubeMQ channel demo.ws)
// Publish topic:      events/demo/ws  (KubeMQ channel demo.ws)
//
// [1] Connecting subscriber over WebSocket...
//     Subscriber connected  (ResultCode=Success)
//     Subscribed to 'events/demo/ws'  (SUBACK=GrantedQoS1)
//
// [2] Connecting publisher over WebSocket...
//     Publisher connected  (ResultCode=Success)
//     Published to 'events/demo/ws' with UserProperty source=csharp-ws-example  (PUBACK=Success)
//
// [3] Waiting for event delivery...
//     Received payload:       {"message":"hello via WebSocket","channel":"demo.ws","ts":"..."}
//     Received UserProperty:  source=csharp-ws-example
//     User-property 'source' round-tripped as a KubeMQ Tag.
//
// [4] Disconnecting...
//     Done.
// ---------------------------------------------------------------------------
