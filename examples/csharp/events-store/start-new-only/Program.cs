// Example: events-store/start-new-only
//
// Demonstrates KubeMQ Events-Store over MQTT 5.0.
// Topic prefix "store/<channel>" routes to KubeMQ Events-Store.
//
// KEY CONSTRAINT: Events-Store over MQTT is ALWAYS StartNewOnly.
// Messages published BEFORE a subscription exists are NOT delivered to that
// subscriber — there is no historical replay over MQTT (unlike the gRPC API
// which supports SeqFirst, SeqRange, TimeRange, etc.).
//
// Sequence this example uses to prove StartNewOnly:
//   Step 1 — Publish 3 messages BEFORE subscribing (pre-sub, not replayed)
//   Step 2 — Subscribe to store/demo/s
//   Step 3 — Publish 1 message AFTER subscribing (post-sub, WILL arrive)
//   Step 4 — Assert (a) NONE of this run's pre-subscribe messages were replayed
//            and (b) this run's post-subscribe message arrived.
//
// Note on the shared channel:
//   store/demo/s is a canonical demo channel that may carry concurrent traffic
//   from other clients (e.g. sibling examples in other languages running at the
//   same time). To stay correct under that traffic, every message this run
//   publishes is tagged with a unique per-run id plus a phase marker
//   ("pre"/"post"). On receive, each message is classified by run id into
//   own-pre / own-post / foreign; foreign messages (including non-matching
//   payloads) are ignored. The assertions only consider THIS run's tagged
//   messages, so the StartNewOnly guarantee stays provable without a brittle
//   exact-count assertion against the shared channel.
//
// Channel: demo.s
// Topic:   store/demo/s
//
// Run: dotnet run
//      KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using System.Text;

static string BrokerUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_MQTT_URL") ?? "tcp://localhost:1883";

// Parse host and port from URL — supports tcp:// ws:// tls:// schemes.
static (string host, int port) ParseTcpEndpoint(string url)
{
    foreach (var prefix in new[] { "tcp://", "tls://", "ws://" })
        if (url.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            url = url[prefix.Length..];
    var parts = url.TrimEnd('/').Split(':');
    return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 1883);
}

var (host, port) = ParseTcpEndpoint(BrokerUrl());
const string topic   = "store/demo/s";
const string channel = "demo.s";

// Unique per-run id, embedded in every payload so the subscriber can tell THIS
// run's traffic apart from concurrent foreign traffic on the shared channel.
var runId = Guid.NewGuid().ToString("N");
Console.WriteLine($"Run ID: {runId}");

var factory = new MqttFactory();

// ── publisher client ────────────────────────────────────────────────────────
using var pubClient = factory.CreateMqttClient();
var pubOptions = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"csharp-mqtt-store-pub-{Guid.NewGuid():N}"[..27])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build();

await pubClient.ConnectAsync(pubOptions);
Console.WriteLine($"Publisher  connected to {host}:{port}");

// Helper: publish one message; throw on non-success PUBACK reason code.
async Task Publish(string label, string body)
{
    var result = await pubClient.PublishAsync(
        new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(Encoding.UTF8.GetBytes(body))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build());

    if (result.ReasonCode != MqttClientPublishReasonCode.Success)
        throw new Exception($"PUBACK reason: {result.ReasonCode}");

    Console.WriteLine($"  Published [{label}]: {body[..Math.Min(body.Length, 80)]}");
}

// ── STEP 1: Publish 3 messages BEFORE any subscriber exists ─────────────────
Console.WriteLine();
Console.WriteLine("=== STEP 1: Publish 3 messages BEFORE subscribing (these will NOT be replayed) ===");
for (int i = 1; i <= 3; i++)
{
    var body = $"{{\"seq\":{i},\"label\":\"pre-sub-{i}\",\"channel\":\"{channel}\",\"run_id\":\"{runId}\",\"phase\":\"pre\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";
    await Publish($"pre-sub-{i}", body);
    await Task.Delay(50);
}
Console.WriteLine("  -> Pre-subscription messages published and stored in KubeMQ Events-Store.");
Console.WriteLine("     A new subscriber will NOT receive these — MQTT always uses StartNewOnly.");

// ── STEP 2: Subscribe ────────────────────────────────────────────────────────
Console.WriteLine();
Console.WriteLine("=== STEP 2: Subscribe to the store channel ===");

// Classify only THIS run's messages (matched by run id) by phase. Foreign
// messages on the shared channel are counted separately and ignored by the
// assertions.
var ownPre  = new List<string>();
var ownPost = new List<string>();
var foreign = new List<string>();
var firstPostSubMessage = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

// A payload belongs to THIS run only if it carries this run's tag.
string runTag   = $"\"run_id\":\"{runId}\"";
string preTag   = "\"phase\":\"pre\"";
string postTag  = "\"phase\":\"post\"";

using var subClient = factory.CreateMqttClient();
subClient.ApplicationMessageReceivedAsync += e =>
{
    var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
    if (!payload.Contains(runTag))
    {
        lock (foreign) foreign.Add(payload);
        Console.WriteLine($"  Ignoring foreign message: {payload[..Math.Min(payload.Length, 80)]}");
        return Task.CompletedTask;
    }
    if (payload.Contains(preTag))
    {
        lock (ownPre) ownPre.Add(payload);
    }
    else if (payload.Contains(postTag))
    {
        lock (ownPost) ownPost.Add(payload);
        firstPostSubMessage.TrySetResult(payload);
    }
    Console.WriteLine($"  Received: {payload[..Math.Min(payload.Length, 80)]}");
    return Task.CompletedTask;
};

var subOptions = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"csharp-mqtt-store-sub-{Guid.NewGuid():N}"[..27])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build();

await subClient.ConnectAsync(subOptions);

var subResult = await subClient.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f =>
            f.WithTopic(topic).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

var rc = subResult.Items.First().ResultCode;
if (rc > MqttClientSubscribeResultCode.GrantedQoS2)
    throw new Exception($"SUBACK rejected with reason code: {rc}");

Console.WriteLine($"  Subscribed to '{topic}' (QoS {rc})");
Console.WriteLine("  StartNewOnly in effect — only messages published from NOW on will arrive.");

// Small delay to ensure the subscription is fully registered server-side.
await Task.Delay(300);

// ── STEP 3: Publish 1 message AFTER subscribing ──────────────────────────────
Console.WriteLine();
Console.WriteLine("=== STEP 3: Publish 1 message AFTER subscribing (this WILL be delivered) ===");
var postBody = $"{{\"seq\":4,\"label\":\"post-sub\",\"channel\":\"{channel}\",\"run_id\":\"{runId}\",\"phase\":\"post\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";
await Publish("post-sub", postBody);

// ── STEP 4: Wait and verify ───────────────────────────────────────────────────
Console.WriteLine();
Console.WriteLine("=== STEP 4: Wait for delivery and verify StartNewOnly ===");

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
try
{
    await firstPostSubMessage.Task.WaitAsync(cts.Token);
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("ERROR: Timed out — post-subscribe message was not delivered within 10 s.");
    Environment.Exit(1);
}

// Brief extra wait to ensure no replayed pre-sub messages arrive late.
await Task.Delay(500);

Console.WriteLine();
int preCount, postCount, foreignCount;
lock (ownPre)  preCount     = ownPre.Count;
lock (ownPost) postCount    = ownPost.Count;
lock (foreign) foreignCount = foreign.Count;
Console.WriteLine($"  Pre-subscribe messages published       : 3");
Console.WriteLine($"  This run's pre-subscribe msgs replayed : {preCount}  (StartNewOnly — expect 0)");
Console.WriteLine($"  This run's post-subscribe msgs received: {postCount}  (expect >= 1)");
Console.WriteLine($"  Foreign messages ignored               : {foreignCount}");

// (a) The StartNewOnly guarantee: NONE of this run's pre-subscribe messages are
//     replayed to a subscription that started after they were published.
if (preCount != 0)
{
    Console.Error.WriteLine($"ERROR: StartNewOnly violation — {preCount} of this run's " +
                            "pre-subscribe message(s) were replayed.");
    Environment.Exit(1);
}

// (b) This run's uniquely-tagged post-subscribe message IS delivered.
if (postCount < 1)
{
    Console.Error.WriteLine("ERROR: this run's post-subscribe message was not delivered.");
    Environment.Exit(1);
}

Console.WriteLine();
Console.WriteLine("PASS: StartNewOnly confirmed — this run's pre-subscribe messages were NOT replayed.");
Console.WriteLine("      This run's post-subscribe message was delivered (foreign traffic ignored).");

await subClient.DisconnectAsync();
await pubClient.DisconnectAsync();
Console.WriteLine("Done.");

// Expected output (run id and foreign count vary per run):
// Publisher  connected to 127.0.0.1:1883
// Run ID: 3f2a1c9e...
//
// === STEP 1: Publish 3 messages BEFORE subscribing (these will NOT be replayed) ===
//   Published [pre-sub-1]: {"seq":1,"label":"pre-sub-1","channel":"demo.s","run_id":"...","phase":"pre","ts":"..."}
//   Published [pre-sub-2]: {"seq":2,"label":"pre-sub-2","channel":"demo.s","run_id":"...","phase":"pre","ts":"..."}
//   Published [pre-sub-3]: {"seq":3,"label":"pre-sub-3","channel":"demo.s","run_id":"...","phase":"pre","ts":"..."}
//   -> Pre-subscription messages published and stored in KubeMQ Events-Store.
//      A new subscriber will NOT receive these — MQTT always uses StartNewOnly.
//
// === STEP 2: Subscribe to the store channel ===
//   Subscribed to 'store/demo/s' (QoS GrantedQoS1)
//   StartNewOnly in effect — only messages published from NOW on will arrive.
//
// === STEP 3: Publish 1 message AFTER subscribing (this WILL be delivered) ===
//   Published [post-sub]: {"seq":4,"label":"post-sub","channel":"demo.s","run_id":"...","phase":"post","ts":"..."}
//
// === STEP 4: Wait for delivery and verify StartNewOnly ===
//   Received: {"seq":4,"label":"post-sub","channel":"demo.s","run_id":"...","phase":"post","ts":"..."}
//
//   Pre-subscribe messages published       : 3
//   This run's pre-subscribe msgs replayed : 0  (StartNewOnly — expect 0)
//   This run's post-subscribe msgs received: 1  (expect >= 1)
//   Foreign messages ignored               : 0
//
// PASS: StartNewOnly confirmed — this run's pre-subscribe messages were NOT replayed.
//       This run's post-subscribe message was delivered (foreign traffic ignored).
// Done.
