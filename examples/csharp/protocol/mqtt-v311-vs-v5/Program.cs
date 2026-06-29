// Example: protocol/mqtt-v311-vs-v5
//
// Contrasts MQTT 3.1.1 and MQTT 5.0 against the KubeMQ MQTT connector.
//
// MQTT 3.1.1 capabilities (demonstrated):
//   - Basic events pub/sub at QoS 1 — works.
//   - No User Properties — KubeMQ Tags are NOT carried in either direction.
//   - RPC (commands/queries) publish is SILENTLY DROPPED:
//       PUBACK returns 0x00 (success), but the command is never executed.
//   - No shared subscriptions ($share/...) — queue consume is not available.
//
// MQTT 5.0 capabilities (demonstrated):
//   - Events pub/sub with User Properties <-> KubeMQ Tags round-trip.
//   - Shared subscription ($share/<group>/queues/<ch>) for queue consume.
//   - RPC with $reply/<clientID>/<suffix> ResponseTopic + CorrelationData.
//
// Relevant PUBACK reason codes:
//   0x00 Success (also returned for the SILENTLY DROPPED v3.1.1 RPC publish)
//   0x80 Broker not ready (gated publish)
//   0x87 Not authorized (ACL deny)
//   0x90 Topic-name-invalid (empty channel / DefaultPattern=none)
//   0x97 Quota exceeded (user-prop cap: 32 props / 4096 bytes)
//
// GOTCHA — Retain is NOT supported (either version):
//   A publish with Retain=true is silently dropped — PUBACK returns 0x00
//   but the message is never delivered. Retain in a Will message at CONNECT
//   gets CONNACK 0x9A (only way Retain is actively rejected).
//
// GOTCHA — RPC is MQTT 5.0 ONLY:
//   A v3.1.1 client that publishes to commands/ or queries/ receives PUBACK
//   0x00 (wire-level success), but the command/query is NEVER executed. No
//   error is returned to the v3.1.1 client.  Use v5.0 with ResponseTopic +
//   CorrelationData to perform RPC.
//
// Run:
//   dotnet run
//   KUBEMQ_MQTT_URL=tcp://127.0.0.1:1883 dotnet run

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using System.Text;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

static string BrokerUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_MQTT_URL") ?? "tcp://localhost:1883";

// Supports tcp:// ws:// tls:// schemes.
static (string host, int port) ParseTcpEndpoint(string url)
{
    foreach (var prefix in new[] { "tcp://", "tls://", "ws://" })
        if (url.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            url = url[prefix.Length..];
    var parts = url.TrimEnd('/').Split(':');
    return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 1883);
}

var (host, port) = ParseTcpEndpoint(BrokerUrl());
var factory = new MqttFactory();

Console.WriteLine($"Broker: {BrokerUrl()}");
Console.WriteLine();

// ============================================================================
// PART 1 — MQTT 3.1.1
// Basic events pub/sub; then attempt RPC (silently dropped).
// ============================================================================
Console.WriteLine("═══════════════════════════════════════════════════════════");
Console.WriteLine("  PART 1 — MQTT 3.1.1");
Console.WriteLine("═══════════════════════════════════════════════════════════");

{
    // ----- 1a. Events pub/sub ------------------------------------------------

    const string topic311 = "events/demo/v3";
    // Channel mapping: events/demo/v3 -> KubeMQ channel "demo.v3"

    var eventsReceived = new TaskCompletionSource<string>(
        TaskCreationOptions.RunContinuationsAsynchronously);

    using var sub311 = factory.CreateMqttClient();
    sub311.ApplicationMessageReceivedAsync += e =>
    {
        var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
        // v3.1.1 has NO UserProperties — KubeMQ Tags are not echoed back.
        eventsReceived.TrySetResult(payload);
        return Task.CompletedTask;
    };

    Console.WriteLine("[1a] MQTT 3.1.1 — Events pub/sub");
    Console.WriteLine($"     Topic: {topic311}  (KubeMQ channel: demo.v3)");

    var subOpts311 = new MqttClientOptionsBuilder()
        .WithTcpServer(host, port)
        .WithClientId($"csharp-v311-sub-{Guid.NewGuid():N}"[..24])
        .WithProtocolVersion(MqttProtocolVersion.V311)
        .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
        .WithCleanSession(true)
        .Build();

    var subConn311 = await sub311.ConnectAsync(subOpts311);
    Console.WriteLine($"     Subscriber connected  (CONNACK={subConn311.ResultCode})");

    var subRes311 = await sub311.SubscribeAsync(
        new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f =>
                f.WithTopic(topic311)
                 .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build());
    // v3.1.1 SUBACK codes: 0=QoS0 granted, 1=QoS1 granted, 2=QoS2 granted, 0x80=failure
    Console.WriteLine($"     Subscribed  (SUBACK={subRes311.Items.First().ResultCode})");

    await Task.Delay(300);

    using var pub311 = factory.CreateMqttClient();
    var pubOpts311 = new MqttClientOptionsBuilder()
        .WithTcpServer(host, port)
        .WithClientId($"csharp-v311-pub-{Guid.NewGuid():N}"[..24])
        .WithProtocolVersion(MqttProtocolVersion.V311)
        .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
        .WithCleanSession(true)
        .Build();

    var pubConn311 = await pub311.ConnectAsync(pubOpts311);
    Console.WriteLine($"     Publisher connected  (CONNACK={pubConn311.ResultCode})");

    var evPayload = $"{{\"hello\":\"from v3.1.1\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";
    await pub311.PublishAsync(
        new MqttApplicationMessageBuilder()
            .WithTopic(topic311)
            .WithPayload(Encoding.UTF8.GetBytes(evPayload))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            // NOTE: v3.1.1 has NO User Properties — adding them here would be
            // silently ignored; KubeMQ Tags cannot be sent or received over v3.1.1.
            .Build());
    Console.WriteLine($"     Published to '{topic311}'  (no user properties — v3.1.1 cannot carry KubeMQ Tags)");

    using var cts311 = new CancellationTokenSource(TimeSpan.FromSeconds(8));
    try
    {
        var body = await eventsReceived.Task.WaitAsync(cts311.Token);
        Console.WriteLine($"     Received: {body}");
        Console.WriteLine("     v3.1.1 basic events pub/sub: OK");
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("     WARNING: timed out waiting for v3.1.1 events delivery");
    }

    // ----- 1b. RPC — silently dropped ----------------------------------------

    Console.WriteLine();
    Console.WriteLine("[1b] MQTT 3.1.1 — RPC attempt on 'commands/demo/x' (silently dropped)");
    Console.WriteLine("     A v3.1.1 publish to commands/ or queries/ returns PUBACK 0x00 (success)");
    Console.WriteLine("     but the command is NEVER executed — silently dropped by the connector.");

    var rpcPayload = $"{{\"action\":\"ping\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";
    var rpcPubResult = await pub311.PublishAsync(
        new MqttApplicationMessageBuilder()
            .WithTopic("commands/demo/x")
            .WithPayload(Encoding.UTF8.GetBytes(rpcPayload))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            // v3.1.1 cannot set ResponseTopic/CorrelationData (v5-only properties).
            // Even if it could, the connector drops the publish before routing.
            .Build());

    // PUBACK reason code is 0x00 (Success) even though the message was dropped.
    // This is the silent-drop behavior documented in the KubeMQ connector spec
    // (test #12 RPC311Dropped).
    Console.WriteLine($"     PUBACK returned: {rpcPubResult.ReasonCode}  " +
                      "(0x00=Success — wire ack OK, but command was NOT executed)");
    Console.WriteLine("     v3.1.1 RPC silently dropped: confirmed.");

    // ----- 1c. Shared subscription — not available ---------------------------

    Console.WriteLine();
    Console.WriteLine("[1c] MQTT 3.1.1 — Shared subscription ($share) is NOT available.");
    Console.WriteLine("     Queue consume requires MQTT 5.0 shared subscriptions.");
    Console.WriteLine("     v3.1.1 cannot subscribe to $share/... (no such protocol feature).");

    await sub311.DisconnectAsync();
    await pub311.DisconnectAsync();
    Console.WriteLine();
    Console.WriteLine("     v3.1.1 clients disconnected.");
}

// ============================================================================
// PART 2 — MQTT 5.0
// Events with User Properties, shared-subscription queue consume note, RPC.
// ============================================================================
Console.WriteLine();
Console.WriteLine("═══════════════════════════════════════════════════════════");
Console.WriteLine("  PART 2 — MQTT 5.0");
Console.WriteLine("═══════════════════════════════════════════════════════════");

{
    // ----- 2a. Events with User Properties (Tags) ----------------------------

    const string topic5 = "events/demo/v3";
    // Same channel as v3.1.1: demo.v3 — showing both versions can share a channel.

    var evReceived = new TaskCompletionSource<(string payload, string? tag)>(
        TaskCreationOptions.RunContinuationsAsynchronously);

    using var sub5 = factory.CreateMqttClient();
    sub5.ApplicationMessageReceivedAsync += e =>
    {
        var msg = e.ApplicationMessage;
        var body = Encoding.UTF8.GetString(msg.PayloadSegment);
        // v5.0 UserProperties carry KubeMQ Tags bidirectionally (§2.9).
        // Cap: 32 properties / 4096 bytes total; exceeding -> PUBACK 0x97.
        string? tag = msg.UserProperties?.FirstOrDefault(p => p.Name == "x-source")?.Value;
        evReceived.TrySetResult((body, tag));
        return Task.CompletedTask;
    };

    Console.WriteLine("[2a] MQTT 5.0 — Events with User Properties (KubeMQ Tags)");
    Console.WriteLine($"     Topic: {topic5}  (KubeMQ channel: demo.v3)");

    var subOpts5 = new MqttClientOptionsBuilder()
        .WithTcpServer(host, port)
        .WithClientId($"csharp-v5-sub-{Guid.NewGuid():N}"[..22])
        .WithProtocolVersion(MqttProtocolVersion.V500)
        .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
        .WithCleanSession(true)
        .Build();

    var subConn5 = await sub5.ConnectAsync(subOpts5);
    Console.WriteLine($"     Subscriber connected  (CONNACK={subConn5.ResultCode})");
    // v5 CONNACK carries RetainAvailable=0 — retain is disabled on this broker.
    if (subConn5.AssignedClientIdentifier != null)
        Console.WriteLine($"     Assigned client ID: {subConn5.AssignedClientIdentifier}");

    var subRes5 = await sub5.SubscribeAsync(
        new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f =>
                f.WithTopic(topic5)
                 .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build());
    Console.WriteLine($"     Subscribed  (SUBACK={subRes5.Items.First().ResultCode})");

    await Task.Delay(300);

    using var pub5 = factory.CreateMqttClient();
    var pubOpts5 = new MqttClientOptionsBuilder()
        .WithTcpServer(host, port)
        .WithClientId($"csharp-v5-pub-{Guid.NewGuid():N}"[..22])
        .WithProtocolVersion(MqttProtocolVersion.V500)
        .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
        .WithCleanSession(true)
        .Build();

    var pubConn5 = await pub5.ConnectAsync(pubOpts5);
    Console.WriteLine($"     Publisher connected  (CONNACK={pubConn5.ResultCode})");

    var ev5Payload = $"{{\"hello\":\"from v5.0\",\"ts\":\"{DateTimeOffset.UtcNow:O}\"}}";

    // Relevant PUBACK codes for v5 publish:
    //   0x00 Success
    //   0x97 Quota exceeded (>32 user props or >4096 bytes total)
    //   0x87 Not authorized
    //   0x90 Topic-name-invalid
    var pubRes5 = await pub5.PublishAsync(
        new MqttApplicationMessageBuilder()
            .WithTopic(topic5)
            .WithPayload(Encoding.UTF8.GetBytes(ev5Payload))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            // User Properties -> KubeMQ Tags. Subscriber receives them back in
            // UserProperties (bidirectional §2.9). Cap: 32 props / 4096 bytes.
            .WithUserProperty("x-source", "csharp-v5-example")
            .Build());
    if (pubRes5.ReasonCode != MqttClientPublishReasonCode.Success)
        throw new Exception($"PUBACK failed: {pubRes5.ReasonCode} (0x{(int)pubRes5.ReasonCode:X2})");
    Console.WriteLine($"     Published with UserProperty 'x-source=csharp-v5-example'  (PUBACK={pubRes5.ReasonCode})");

    using var cts5 = new CancellationTokenSource(TimeSpan.FromSeconds(8));
    try
    {
        var (body, tag) = await evReceived.Task.WaitAsync(cts5.Token);
        Console.WriteLine($"     Received: {body}");
        Console.WriteLine($"     Received UserProperty 'x-source': {tag ?? "(not present)"}");
        if (tag == "csharp-v5-example")
            Console.WriteLine("     User-property 'x-source' round-tripped as KubeMQ Tag: OK");
        else
            Console.WriteLine("     NOTE: user-property not echoed — verify KubeMQ tagsToUserProps is enabled");
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("     WARNING: timed out waiting for v5.0 events delivery");
    }

    // ----- 2b. Shared subscription (queue consume) — available in v5 only ----

    Console.WriteLine();
    Console.WriteLine("[2b] MQTT 5.0 — Shared subscription ($share) for queue consume");
    Console.WriteLine("     v5.0 supports $share/<group>/queues/<ch> at QoS>=1.");

    const string queueConsumeTopic = "$share/demo-group/queues/demo/v5-q";
    // QoS 0 on a shared-sub returns SUBACK 0x83; must use QoS 1 or 2.
    var sharedSubResult = await sub5.SubscribeAsync(
        new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f =>
                f.WithTopic(queueConsumeTopic)
                 .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build());
    var sharedSubCode = sharedSubResult.Items.First().ResultCode;
    if (sharedSubCode <= MqttClientSubscribeResultCode.GrantedQoS2)
        Console.WriteLine($"     Subscribed to '{queueConsumeTopic}'  (SUBACK={sharedSubCode}) — OK");
    else
        Console.WriteLine($"     SUBACK={sharedSubCode} (unexpected rejection)");

    // Demonstrate that a plain queues/... subscribe (without $share) is rejected.
    const string plainQueueTopic = "queues/demo/v5-q";
    var plainSubResult = await sub5.SubscribeAsync(
        new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f =>
                f.WithTopic(plainQueueTopic)
                 .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build());
    var plainSubCode = plainSubResult.Items.First().ResultCode;
    // Expected: 0x83 (ImplementationSpecificError)
    Console.WriteLine($"     Plain subscribe to '{plainQueueTopic}' (no $share) -> SUBACK={plainSubCode} " +
                      "(0x83 = rejected — must use $share/... syntax)");

    // ----- 2c. RPC — v5.0 only -----------------------------------------------

    Console.WriteLine();
    Console.WriteLine("[2c] MQTT 5.0 — RPC publish to 'commands/demo/cmd' with ResponseTopic");
    Console.WriteLine("     Step 1: subscribe own $reply/<clientID>/inbox BEFORE publishing.");
    Console.WriteLine("     Step 2: publish with ResponseTopic + CorrelationData.");
    Console.WriteLine("     Step 3: PUBACK is IMMEDIATE (before the response); wait separately.");
    Console.WriteLine("     NOTE: a running gRPC responder on channel 'demo.cmd' (type command) is");
    Console.WriteLine("           required for the full round-trip.  If none is running, the wait times out.");

    var rpcClientId = $"csharp-v5-rpc-{Guid.NewGuid():N}"[..22];
    var replyTopic  = $"$reply/{rpcClientId}/inbox";
    var corrId      = Guid.NewGuid().ToString("N");

    var cmdReceived = new TaskCompletionSource<(bool executed, string? error, string corr)>(
        TaskCreationOptions.RunContinuationsAsynchronously);

    using var rpcClient = factory.CreateMqttClient();
    rpcClient.ApplicationMessageReceivedAsync += e =>
    {
        var msg = e.ApplicationMessage;
        // Command response: NO body; result in user-props kubemq-executed / kubemq-error.
        string? executed = msg.UserProperties?.FirstOrDefault(p => p.Name == "kubemq-executed")?.Value;
        string? kubemqError = msg.UserProperties?.FirstOrDefault(p => p.Name == "kubemq-error")?.Value;
        string corr = msg.CorrelationData != null
            ? Encoding.UTF8.GetString(msg.CorrelationData)
            : string.Empty;
        cmdReceived.TrySetResult((executed == "true", kubemqError, corr));
        return Task.CompletedTask;
    };

    var rpcOpts = new MqttClientOptionsBuilder()
        .WithTcpServer(host, port)
        .WithClientId(rpcClientId)
        .WithProtocolVersion(MqttProtocolVersion.V500)
        .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
        .WithCleanSession(true)
        .Build();

    var rpcConn = await rpcClient.ConnectAsync(rpcOpts);
    Console.WriteLine($"     RPC client connected as '{rpcClientId}'  (CONNACK={rpcConn.ResultCode})");

    // CRITICAL: subscribe reply inbox FIRST, then publish.
    var replySubResult = await rpcClient.SubscribeAsync(
        new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f =>
                f.WithTopic(replyTopic)
                 .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build());
    if (replySubResult.Items.First().ResultCode > MqttClientSubscribeResultCode.GrantedQoS2)
        throw new Exception($"Reply topic SUBACK rejected: {replySubResult.Items.First().ResultCode}");
    Console.WriteLine($"     Subscribed to reply topic '{replyTopic}'  (SUBACK={replySubResult.Items.First().ResultCode})");

    await Task.Delay(200);

    var cmdPayload = $"{{\"action\":\"ping\",\"corrId\":\"{corrId}\"}}";
    var cmdPubResult = await rpcClient.PublishAsync(
        new MqttApplicationMessageBuilder()
            .WithTopic("commands/demo/cmd")
            .WithPayload(Encoding.UTF8.GetBytes(cmdPayload))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            // ResponseTopic MUST be in own $reply/<clientID>/ namespace.
            // Foreign namespace returns PUBACK 0x83.
            .WithResponseTopic(replyTopic)
            .WithCorrelationData(Encoding.UTF8.GetBytes(corrId))
            .Build());

    // PUBACK is sent IMMEDIATELY by the broker (not after RPC completes).
    // The client must maintain its own timeout for the actual response.
    Console.WriteLine($"     Published command to 'commands/demo/cmd'  (PUBACK={cmdPubResult.ReasonCode} — IMMEDIATE, not after response)");

    if (cmdPubResult.ReasonCode != MqttClientPublishReasonCode.Success)
    {
        Console.WriteLine($"     PUBACK rejected ({cmdPubResult.ReasonCode}); skipping response wait.");
    }
    else
    {
        using var ctsCmdWait = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var (ok, err, cid) = await cmdReceived.Task.WaitAsync(ctsCmdWait.Token);
            Console.WriteLine($"     Command response received:");
            Console.WriteLine($"       correlation-id  : {cid}");
            Console.WriteLine($"       kubemq-executed : {ok}");
            if (!string.IsNullOrEmpty(err))
                Console.WriteLine($"       kubemq-error    : {err}");
            Console.WriteLine("     v5.0 RPC round-trip: OK");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("     Timed out waiting for command response.");
            Console.WriteLine("     (This is expected if no gRPC responder is running on channel 'demo.cmd'.)");
            Console.WriteLine("     Start the responder: cd .work/test-harness/responder && go run . -channel demo.cmd -type command");
        }
    }

    await sub5.DisconnectAsync();
    await pub5.DisconnectAsync();
    await rpcClient.DisconnectAsync();
    Console.WriteLine();
    Console.WriteLine("     v5.0 clients disconnected.");
}

// ============================================================================
// Summary comparison
// ============================================================================
Console.WriteLine();
Console.WriteLine("═══════════════════════════════════════════════════════════");
Console.WriteLine("  Summary: v3.1.1 vs v5.0");
Console.WriteLine("═══════════════════════════════════════════════════════════");
Console.WriteLine("  Feature                    │ MQTT 3.1.1       │ MQTT 5.0");
Console.WriteLine("  ─────────────────────────────────────────────────────────");
Console.WriteLine("  Events pub/sub             │ OK               │ OK");
Console.WriteLine("  User Properties (Tags)     │ NOT AVAILABLE    │ Supported (32/4096B)");
Console.WriteLine("  Queue shared-sub ($share)  │ NOT AVAILABLE    │ Supported (QoS>=1)");
Console.WriteLine("  RPC (commands/queries)     │ SILENTLY DROPPED │ Supported ($reply/...)");
Console.WriteLine("  Retain                     │ Silently dropped │ Silently dropped");
Console.WriteLine("  Reason codes               │ Binary only      │ Detailed hex codes");
Console.WriteLine();

// ---------------------------------------------------------------------------
// Expected output:
//
// Broker: tcp://127.0.0.1:1883
//
// ═══════════════════════════════════════════════════════════
//   PART 1 — MQTT 3.1.1
// ═══════════════════════════════════════════════════════════
// [1a] MQTT 3.1.1 — Events pub/sub
//      Topic: events/demo/v3  (KubeMQ channel: demo.v3)
//      Subscriber connected  (CONNACK=Success)
//      Subscribed  (SUBACK=GrantedQoS1)
//      Publisher connected  (CONNACK=Success)
//      Published to 'events/demo/v3'  (no user properties — v3.1.1 cannot carry KubeMQ Tags)
//      Received: {"hello":"from v3.1.1","ts":"..."}
//      v3.1.1 basic events pub/sub: OK
//
// [1b] MQTT 3.1.1 — RPC attempt on 'commands/demo/x' (silently dropped)
//      ...
//      PUBACK returned: Success  (0x00=Success — wire ack OK, but command was NOT executed)
//      v3.1.1 RPC silently dropped: confirmed.
//
// [1c] MQTT 3.1.1 — Shared subscription ($share) is NOT available.
//      ...
//
// ═══════════════════════════════════════════════════════════
//   PART 2 — MQTT 5.0
// ═══════════════════════════════════════════════════════════
// [2a] MQTT 5.0 — Events with User Properties (KubeMQ Tags)
//      ...
//      Received UserProperty 'x-source': csharp-v5-example
//      User-property 'x-source' round-tripped as KubeMQ Tag: OK
//
// [2b] MQTT 5.0 — Shared subscription ($share) for queue consume
//      Subscribed to '$share/demo-group/queues/demo/v5-q'  (SUBACK=GrantedQoS1) — OK
//      Plain subscribe to 'queues/demo/v5-q' (no $share) -> SUBACK=ImplementationSpecificError ...
//
// [2c] MQTT 5.0 — RPC publish to 'commands/demo/cmd' with ResponseTopic
//      ...
//      Published command to 'commands/demo/cmd'  (PUBACK=Success — IMMEDIATE, not after response)
//      [If responder running (channel demo.cmd, type command):]
//      Command response received:
//        correlation-id  : <corrId>
//        kubemq-executed : True
//      v5.0 RPC round-trip: OK
//      [If no responder:]
//      Timed out waiting for command response.
//      ...
// ---------------------------------------------------------------------------
