// Example: events/wildcard-subscribe
//
// Mirrors integration test #25 (TestMqttIntegration_QueueNoWildcardLeak).
//
// Demonstrates that overlapping MQTT wildcard subscriptions over KubeMQ Events
// each create an independent bridge registry entry. A single publish that
// matches N entries delivers exactly N copies — no cross-entry dedup (V1,
// gotcha #6).
//
// Per-run UNIQUE channel: every run mints a fresh 8-char id so the three
// overlapping filters target a freshly minted KubeMQ channel that no prior/other
// run reuses. This keeps the copy count deterministic on a busy/shared broker
// (a literal fixed "events/leak/x" would over-count when another process holds a
// broad wildcard).
//
// Setup (one subscriber client, three overlapping filters):
//   "events/leak/<id>/x"  -> KubeMQ channel "leak.<id>.x"  (exact)
//   "events/leak/<id>/#"  -> KubeMQ channel "leak.<id>.>"  (multi-level, scoped)
//   "#"                   -> KubeMQ channel ">"            (bare, INFORMATIONAL)
//
// Publishing "events/leak/<id>/x" ONCE matches all three bridge entries:
//   - the exact filter (one entry),
//   - the scoped multi-level filter (one entry),
//   - the bare "#" (one entry, plus any foreign broad-wildcard entries).
// On a quiet broker that is exactly 3 copies. The bare "#" is INFORMATIONAL:
// on a BUSY broker it (and any foreign "#"/"events/#" held by another process,
// all mapping to the global ">" channel) adds extra matching bridge entries that
// ALSO fan into our unique channel — that is gotcha #6 itself, so the assertion
// is ">= wantCopies" (deterministic): we count ONLY copies of our own unique
// publish topic and pass as soon as the three entries we registered each
// delivered. Extra copies are reported, not failed.
//
// Wildcards are ONLY allowed on Events subscriptions.
// Attempting "store/#" returns SUBACK 0xA2 (wildcard-subscriptions-not-supported).
//
// '+' maps to KubeMQ '*' (single-level wildcard)
// '#' maps to KubeMQ '>' (multi-level wildcard)
//
// Run: dotnet run
//      KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using System.Collections.Concurrent;
using System.Text;

static string BrokerUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_MQTT_URL") ?? "tcp://localhost:1883";

// Parse host and port from URL (supports tcp:// ws:// tls:// schemes)
static (string host, int port) ParseTcpEndpoint(string url)
{
    foreach (var prefix in new[] { "tcp://", "tls://", "ws://" })
        if (url.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            url = url[prefix.Length..];
    var parts = url.TrimEnd('/').Split(':');
    return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 1883);
}

var (host, port) = ParseTcpEndpoint(BrokerUrl());

// Per-run unique id so this example's overlapping filters target a freshly
// minted KubeMQ channel that no prior/other run reuses.
var runId = Guid.NewGuid().ToString("N")[..8];

// Three overlapping event filters — each creates its own bridge registry entry.
// A publish to "events/leak/<id>/x" matches all three.
var filterExact       = $"events/leak/{runId}/x";  // KubeMQ channel "leak.<id>.x" (exact)
var filterScopedHash  = $"events/leak/{runId}/#";  // KubeMQ channel "leak.<id>.>" (scoped multi-level)
const string filterBareHash = "#";                 // KubeMQ channel ">"  (bare — INFORMATIONAL only)
var publishTopic      = $"events/leak/{runId}/x";
const int    wantCopies = 3;                        // exact + scoped "#" + bare "#" (quiet broker)

var factory = new MqttFactory();

// ── Subscriber ──────────────────────────────────────────────────────────────
// Count ONLY copies of OUR own unique publish topic. Other Events traffic on the
// broker is irrelevant to the per-entry-fanout semantic we demonstrate.
var received = new ConcurrentBag<(string topic, string payload)>();
var gotAll   = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
int counter  = 0;

using var subClient = factory.CreateMqttClient();
subClient.ApplicationMessageReceivedAsync += e =>
{
    var msg = e.ApplicationMessage;
    // Ignore unrelated background Events traffic — only count our unique topic.
    if (msg.Topic != publishTopic)
        return Task.CompletedTask;

    var payload = Encoding.UTF8.GetString(msg.PayloadSegment);
    // MQTTnet does not surface which subscription filter matched, so we record
    // the delivered topic (always "events/leak/<id>/x" here) alongside the payload.
    received.Add((msg.Topic, payload));
    if (Interlocked.Increment(ref counter) >= wantCopies)
        gotAll.TrySetResult(true);
    return Task.CompletedTask;
};

var subOpts = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"csharp-mqtt-wildcard-sub-{Guid.NewGuid():N}"[..30])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build();

await subClient.ConnectAsync(subOpts);
Console.WriteLine($"[sub] Connected to {host}:{port}");
Console.WriteLine($"[sub] Per-run unique channel: {publishTopic}");

// Subscribe with three overlapping filters in one SUBSCRIBE packet:
//   exact + scoped "#" are the two UNIQUE filters we assert on;
//   bare "#" is INFORMATIONAL (also matches; varies with ambient traffic).
var subResult = await subClient.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f => f.WithTopic(filterExact)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .WithTopicFilter(f => f.WithTopic(filterScopedHash)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .WithTopicFilter(f => f.WithTopic(filterBareHash)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

Console.WriteLine("[sub] SUBACK results:");
foreach (var item in subResult.Items)
{
    if (item.ResultCode > MqttClientSubscribeResultCode.GrantedQoS2)
        throw new Exception($"  Filter '{item.TopicFilter.Topic}' REJECTED: SUBACK 0x{(int)item.ResultCode:X2} ({item.ResultCode})");
    Console.WriteLine($"  Filter '{item.TopicFilter.Topic}' -> {item.ResultCode}");
}
Console.WriteLine("[sub] Three overlapping filters registered — each creates an independent bridge entry.");
Console.WriteLine("      (bare '#' is INFORMATIONAL: on a busy broker it adds extra matching entries.)");

// Give the broker a moment to settle all bridge subscriptions
await Task.Delay(500);

// ── Non-events wildcard rejection demo ──────────────────────────────────────
Console.WriteLine();
Console.WriteLine("[demo] Attempting non-events wildcard 'store/#' (expect SUBACK 0xA2)...");
var badResult = await subClient.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f => f.WithTopic("store/#")
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

foreach (var item in badResult.Items)
{
    // 0xA2 = WildcardSubscriptionsNotSupported (non-events wildcard rejected)
    Console.WriteLine($"  Filter 'store/#' -> SUBACK 0x{(int)item.ResultCode:X2} ({item.ResultCode}) — wildcard-subscriptions-not-supported as expected");
    if ((int)item.ResultCode != 0xA2)
        Console.Error.WriteLine($"  WARNING: expected 0xA2, got {item.ResultCode}");
}

Console.WriteLine();

// ── Publisher ────────────────────────────────────────────────────────────────
using var pubClient = factory.CreateMqttClient();
var pubOpts = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"csharp-mqtt-wildcard-pub-{Guid.NewGuid():N}"[..30])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build();

await pubClient.ConnectAsync(pubOpts);
Console.WriteLine($"[pub] Connected to {host}:{port}");

var payload = Encoding.UTF8.GetBytes(
    $"{{\"message\":\"overlap-test\",\"topic\":\"{publishTopic}\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}");

var pubResult = await pubClient.PublishAsync(
    new MqttApplicationMessageBuilder()
        .WithTopic(publishTopic)
        .WithPayload(payload)
        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        .Build());

if (pubResult.ReasonCode != MqttClientPublishReasonCode.Success)
    throw new Exception($"PUBACK reason: 0x{(int)pubResult.ReasonCode:X2} ({pubResult.ReasonCode})");

Console.WriteLine($"[pub] Published ONE message to '{publishTopic}'");
Console.WriteLine($"[pub] Expecting >= {wantCopies} copies (one per matching bridge registry entry)");

// ── Wait for at least 3 copies ─────────────────────────────────────────────
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
try
{
    await gotAll.Task.WaitAsync(cts.Token);
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine($"[sub] Timed out — received {received.Count}/{wantCopies} copies");
}

// Brief window to surface any extra copies (e.g. ambient ">" entries on a busy broker).
await Task.Delay(700);

Console.WriteLine();
int copies = received.Count;
Console.WriteLine($"[sub] Received {copies} copy/copies of the published message:");
int idx = 1;
foreach (var (topic, body) in received)
    Console.WriteLine($"  copy {idx++}: topic='{topic}'  payload={body}");

Console.WriteLine();
if (copies >= wantCopies)
{
    Console.WriteLine($"[ok] Received {copies} copies (>= {wantCopies}) — one per matching bridge registry entry " +
                      "(no cross-entry dedup, V1 / gotcha #6).");
    if (copies > wantCopies)
        Console.WriteLine($"     ({copies - wantCopies} extra copy/copies — a foreign broad-wildcard subscriber " +
                          "added matching bridge entries on the global '>' channel; that is gotcha #6, not an error.)");
}
else
{
    Console.Error.WriteLine($"[warn] Expected >= {wantCopies} copies, got {copies}.");
}

Console.WriteLine();
Console.WriteLine("Wildcard mapping summary:");
Console.WriteLine("  MQTT '+'   -> KubeMQ '*'  (single-level wildcard)");
Console.WriteLine("  MQTT '#'   -> KubeMQ '>'  (multi-level wildcard)");
Console.WriteLine("  Wildcards on non-events patterns -> SUBACK 0xA2");

await subClient.DisconnectAsync();
await pubClient.DisconnectAsync();

// Expected output (regenerated from a real run in the LiveTest stage):
// [sub] Connected to 127.0.0.1:1883
// [sub] Per-run unique channel: events/leak/<id>/x
// [sub] SUBACK results:
//   Filter 'events/leak/<id>/x' -> GrantedQoS1
//   Filter 'events/leak/<id>/#' -> GrantedQoS1
//   Filter '#' -> GrantedQoS1
// [sub] Three overlapping filters registered — each creates an independent bridge entry.
//       (bare '#' is INFORMATIONAL: on a busy broker it adds extra matching entries.)
//
// [demo] Attempting non-events wildcard 'store/#' (expect SUBACK 0xA2)...
//   Filter 'store/#' -> SUBACK 0xA2 (WildcardSubscriptionsNotSupported) — wildcard-subscriptions-not-supported as expected
//
// [pub] Connected to 127.0.0.1:1883
// [pub] Published ONE message to 'events/leak/<id>/x'
// [pub] Expecting >= 3 copies (one per matching bridge registry entry)
//
// [sub] Received 3 copy/copies of the published message:
//   copy 1: topic='events/leak/<id>/x'  payload={"message":"overlap-test","topic":"events/leak/<id>/x","ts":"..."}
//   copy 2: topic='events/leak/<id>/x'  payload={"message":"overlap-test","topic":"events/leak/<id>/x","ts":"..."}
//   copy 3: topic='events/leak/<id>/x'  payload={"message":"overlap-test","topic":"events/leak/<id>/x","ts":"..."}
//
// [ok] Received 3 copies (>= 3) — one per matching bridge registry entry (no cross-entry dedup, V1 / gotcha #6).
//
// Wildcard mapping summary:
//   MQTT '+'   -> KubeMQ '*'  (single-level wildcard)
//   MQTT '#'   -> KubeMQ '>'  (multi-level wildcard)
//   Wildcards on non-events patterns -> SUBACK 0xA2
