# Example: protocol/mqtt_v311_vs_v5
#
# NOTE: The Ruby mqtt gem (njh/ruby-mqtt) is MQTT 3.1.1 ONLY.
# It does NOT support MQTT 5.0. This means:
#   - No MQTT 5.0 User Properties -> KubeMQ Tags mapping.
#   - No shared-subscription queue consume ($share/...).
#   - No RPC (commands/queries patterns are v5-only;
#     a v3.1.1 publish to commands/ is silently dropped, PUBACK 0x00 — gotcha #4).
#
# This file demonstrates what IS possible with MQTT 3.1.1 using this gem:
#   - Basic events pub/sub (works fine over v3.1.1).
#
# It also documents (does NOT demonstrate) what requires MQTT 5.0.
#
# For a full v3.1.1 vs v5 comparison, see the Go, Python, JavaScript,
# Java, C#, or Rust examples in other language directories.
#
# Canonical tests referenced:
#   #13 V311PubSub (events/it/v3) — v3.1.1 pub/sub works.
#   #12 RPC311Dropped (commands/it/cmd311) — v3.1.1 RPC silently dropped.
#
# Run: ruby protocol/mqtt_v311_vs_v5/main.rb
# Env: KUBEMQ_MQTT_URL=tcp://localhost:1883 (default)
#
require "mqtt"
require "uri"
require "securerandom"
require "timeout"

BROKER_URL = ENV.fetch("KUBEMQ_MQTT_URL", "tcp://localhost:1883")

def parse_url(url)
  uri = URI.parse(url)
  { host: uri.host, port: uri.port, ssl: uri.scheme == "tls" }
end

conn  = parse_url(BROKER_URL)
topic = "events/demo/v3"

puts "=== Ruby mqtt gem: MQTT 3.1.1 only ==="
puts ""
puts "What works (v3.1.1):"
puts "  - events pub/sub"
puts "  - events-store pub/sub (StartNewOnly)"
puts "  - queues PRODUCE (plain publish to queues/<ch>)"
puts "  - connectivity/tls"
puts "  - connectivity/auth_jwt"
puts ""
puts "What requires MQTT 5.0 (NOT supported by this gem):"
puts "  - User Properties <-> KubeMQ Tags mapping"
puts "  - Shared-subscription queue consume ($share/<g>/queues/<ch>)"
puts "  - RPC: commands and queries patterns (v3.1.1 publish silently dropped)"
puts "  - WebSocket transport (gem has no WebSocket)"
puts ""
puts "Running events pub/sub over MQTT 3.1.1..."

received  = Queue.new
sub_ready = Queue.new

subscriber = Thread.new do
  MQTT::Client.connect(
    host:          conn[:host],
    port:          conn[:port],
    ssl:           conn[:ssl],
    client_id:     "ruby-v311-sub-#{SecureRandom.hex(4)}",
    clean_session: true,
    keep_alive:    30
  ) do |client|
    client.subscribe([topic, 1])
    sub_ready.push(:ready)
    t, p = client.get
    received.push([t, p])
  end
rescue => e
  # Surface a connect/subscribe failure instead of deadlocking the main
  # thread on sub_ready.pop (e.g. the broker is not reachable).
  sub_ready.push([:error, e])
end

ready = sub_ready.pop
if ready.is_a?(Array) && ready[0] == :error
  err = ready[1]
  abort "[error] Subscriber connect/subscribe failed: #{err.class}: #{err.message}"
end

MQTT::Client.connect(
  host:          conn[:host],
  port:          conn[:port],
  ssl:           conn[:ssl],
  client_id:     "ruby-v311-pub-#{SecureRandom.hex(4)}",
  clean_session: true,
  keep_alive:    30
) do |client|
  client.publish(topic, "hello from MQTT 3.1.1", false, 1)
  puts "Published to #{topic} (MQTT 3.1.1)"
end

t, p = nil, nil
Timeout.timeout(10) { t, p = received.pop }
puts "Received: topic=#{t} payload=#{p}"
puts ""
puts "Note: v5-only features (User Properties, shared-queue consume, RPC)"
puts "      are not available with the Ruby mqtt gem. Use a v5-capable MQTT"
puts "      client library for those patterns."

subscriber.kill

# Expected output:
# === Ruby mqtt gem: MQTT 3.1.1 only ===
# ...
# Published to events/demo/v3 (MQTT 3.1.1)
# Received: topic=events/demo/v3 payload=hello from MQTT 3.1.1
# Note: v5-only features ...
