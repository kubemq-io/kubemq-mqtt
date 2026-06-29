// Example: queues/ack-and-redelivery
//
// Demonstrates at-least-once delivery semantics via two redelivery triggers:
//
//   Fast path (disconnect-without-ack):
//     Consumer 1 receives the message but does NOT send PUBACK (AutoAcknowledge=false),
//     then disconnects. KubeMQ immediately NAcks and requeues the message so Consumer 2
//     receives it.
//
//   Slow path (ack-timeout, shown in comments):
//     If the consumer never disconnects and never calls AcknowledgeAsync(), the broker
//     waits QueueAckTimeoutSeconds (default 30 s) before NAcking and redelivering.
//
// Produce topic:  queues/demo/rq
// Consume filter: $share/g1/queues/demo/rq
//
// MQTT 5.0 is required. $share group name is audit-only; all groups compete in ONE pool.
// QoS 0 shared subscribe -> SUBACK 0x83. Plain queues/... subscribe -> SUBACK 0x83.
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
const string publishTopic  = "queues/demo/rq";
const string consumeFilter = "$share/g1/queues/demo/rq";

var factory = new MqttFactory();

// ── Step 1: Produce one queue message ────────────────────────────────────────
Console.WriteLine("Step 1: Producing one queue message...");
using (var producer = factory.CreateMqttClient())
{
    await producer.ConnectAsync(new MqttClientOptionsBuilder()
        .WithTcpServer(host, port)
        .WithClientId($"csharp-ackprod-{Guid.NewGuid():N}"[..22])
        .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
        .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
        .WithCleanSession(true)
        .Build());

    var payload = $"{{\"msg\":\"ack-test\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";
    var result = await producer.PublishAsync(
        new MqttApplicationMessageBuilder()
            .WithTopic(publishTopic)
            .WithPayload(Encoding.UTF8.GetBytes(payload))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build());

    if (result.ReasonCode != MqttClientPublishReasonCode.Success)
        throw new Exception($"Produce PUBACK reason: {result.ReasonCode}");

    Console.WriteLine($"  Produced: {payload}");
    await producer.DisconnectAsync();
}

await Task.Delay(300); // let the broker ingest the message

// ── Step 2: Consumer 1 — receive but DO NOT ack, then disconnect ──────────────
//
// Setting e.AutoAcknowledge = false prevents MQTTnet from sending PUBACK.
// Disconnecting with the message unacked triggers immediate requeue (fast path).
// The slow path is: stay connected, never call AcknowledgeAsync() -> broker waits
// QueueAckTimeoutSeconds (default 30 s) then NAcks and redelivers.
Console.WriteLine("\nStep 2: Consumer 1 — receives message WITHOUT acking, then disconnects...");
var firstReceived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

using var consumer1 = factory.CreateMqttClient();
consumer1.ApplicationMessageReceivedAsync += e =>
{
    // Suppress the automatic PUBACK so the message is NOT acked.
    e.AutoAcknowledge = false;

    var msg = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
    Console.WriteLine($"  Consumer 1 received (NOT acked): {msg}");
    firstReceived.TrySetResult(msg);
    return Task.CompletedTask;
};

await consumer1.ConnectAsync(new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"csharp-ackcon1-{Guid.NewGuid():N}"[..22])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build());

var sub1 = await consumer1.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f => f
            .WithTopic(consumeFilter)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

if (sub1.Items.First().ResultCode > MqttClientSubscribeResultCode.GrantedQoS2)
    throw new Exception($"Consumer 1 SUBACK rejected: {sub1.Items.First().ResultCode}");

Console.WriteLine($"  Consumer 1 subscribed: {consumeFilter}");

using var cts1 = new CancellationTokenSource(TimeSpan.FromSeconds(15));
try { await firstReceived.Task.WaitAsync(cts1.Token); }
catch (OperationCanceledException)
{
    Console.Error.WriteLine("Timed out waiting for Consumer 1 to receive the message.");
    Environment.Exit(1);
}

// Disconnect WITHOUT acking -> KubeMQ immediately NAcks and requeues the message.
Console.WriteLine("  Consumer 1 disconnecting (no PUBACK) -> message requeued immediately...");
await consumer1.DisconnectAsync();

await Task.Delay(500); // let the broker requeue before Consumer 2 subscribes

// ── Step 3: Consumer 2 — receives the redelivered message and acks it ────────
Console.WriteLine("\nStep 3: Consumer 2 — receives redelivered message and acks it (PUBACK sent)...");
var secondReceived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

using var consumer2 = factory.CreateMqttClient();
consumer2.ApplicationMessageReceivedAsync += async e =>
{
    var msg = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
    Console.WriteLine($"  Consumer 2 received (will ack): {msg}");

    // Explicitly acknowledge: sends PUBACK, completing the at-least-once delivery.
    await e.AcknowledgeAsync(CancellationToken.None);
    Console.WriteLine("  Consumer 2 sent PUBACK — message fully acked and removed from queue.");
    secondReceived.TrySetResult(msg);
};

await consumer2.ConnectAsync(new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"csharp-ackcon2-{Guid.NewGuid():N}"[..22])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build());

var sub2 = await consumer2.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f => f
            .WithTopic(consumeFilter)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

if (sub2.Items.First().ResultCode > MqttClientSubscribeResultCode.GrantedQoS2)
    throw new Exception($"Consumer 2 SUBACK rejected: {sub2.Items.First().ResultCode}");

Console.WriteLine($"  Consumer 2 subscribed: {consumeFilter}");

using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(15));
try { await secondReceived.Task.WaitAsync(cts2.Token); }
catch (OperationCanceledException)
{
    Console.Error.WriteLine("Timed out waiting for Consumer 2 to receive the redelivered message.");
    Environment.Exit(1);
}

await consumer2.DisconnectAsync();

Console.WriteLine("\nDone. At-least-once delivery demonstrated:");
Console.WriteLine("  - Consumer 1 received the message but did not ack.");
Console.WriteLine("  - Disconnect without PUBACK -> immediate requeue (fast path).");
Console.WriteLine("  - Consumer 2 received the redelivered message and acked it.");
Console.WriteLine("  - Slow path: no PUBACK within QueueAckTimeoutSeconds (30s) also triggers redelivery.");
