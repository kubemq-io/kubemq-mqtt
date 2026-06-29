// Example: queues/shared-consume
//
// Demonstrates KubeMQ Queues over MQTT 5.0 using shared subscriptions.
// MQTT 5.0 shared subscription syntax: $share/<group>/queues/<channel>
// at QoS >= 1 is required — plain "queues/..." subscribe -> SUBACK 0x83.
// The group name is audit/metrics-only: all groups compete in ONE shared KubeMQ pool.
//
// Produce topic:  queues/demo/q
// Consume filter: $share/g1/queues/demo/q
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
const string publishTopic  = "queues/demo/q";
const string consumeFilter = "$share/g1/queues/demo/q";
const int messageCount = 3;

var factory = new MqttFactory();
int received = 0;
var done = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

// --- consumer ---
using var consumer = factory.CreateMqttClient();
consumer.ApplicationMessageReceivedAsync += e =>
{
    var msg = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
    Console.WriteLine($"Consumed: {msg}");
    if (System.Threading.Interlocked.Increment(ref received) >= messageCount)
        done.TrySetResult(true);
    return Task.CompletedTask;
};

var consumerOptions = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"csharp-mqtt-queue-con-{Guid.NewGuid():N}"[..27])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build();

await consumer.ConnectAsync(consumerOptions);
Console.WriteLine($"Consumer connected ({host}:{port})");

var subResult = await consumer.SubscribeAsync(
    new MqttClientSubscribeOptionsBuilder()
        .WithTopicFilter(f => f.WithTopic(consumeFilter).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
        .Build());

if (subResult.Items.First().ResultCode > MqttClientSubscribeResultCode.GrantedQoS2)
    throw new Exception($"SUBACK rejected: {subResult.Items.First().ResultCode} — ensure QoS>=1 and $share syntax");
Console.WriteLine($"Subscribed with shared filter '{consumeFilter}'");

await Task.Delay(300);

// --- producer ---
using var producer = factory.CreateMqttClient();
var producerOptions = new MqttClientOptionsBuilder()
    .WithTcpServer(host, port)
    .WithClientId($"csharp-mqtt-queue-prod-{Guid.NewGuid():N}"[..28])
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithCleanSession(true)
    .Build();
await producer.ConnectAsync(producerOptions);

for (int i = 1; i <= messageCount; i++)
{
    var payload = $"{{\"seq\":{i},\"msg\":\"queue message {i}\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";
    var pubResult = await producer.PublishAsync(
        new MqttApplicationMessageBuilder()
            .WithTopic(publishTopic)
            .WithPayload(Encoding.UTF8.GetBytes(payload))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build());
    if (pubResult.ReasonCode != MqttClientPublishReasonCode.Success)
        throw new Exception($"PUBACK reason: {pubResult.ReasonCode}");
    Console.WriteLine($"Produced message {i}");
}

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
try
{
    await done.Task.WaitAsync(cts.Token);
    Console.WriteLine($"All {messageCount} messages consumed.");
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine($"Timed out — received {received}/{messageCount}");
    Environment.Exit(1);
}

await consumer.DisconnectAsync();
await producer.DisconnectAsync();
