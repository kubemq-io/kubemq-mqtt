# Example: events_store/start_new_only
#
# Demonstrates Events-Store over MQTT, which is ALWAYS StartNewOnly.
# Messages published BEFORE a subscription is established are NEVER delivered.
# There is NO historical replay over MQTT (gotcha #2).
#
# Flow:
#   1. Publish 3 messages to store/demo/s BEFORE subscribing.
#   2. Subscribe to store/demo/s.
#   3. Publish 1 message AFTER subscribing.
#   4. Assert: (a) NONE of this run's pre-subscribe messages were replayed and
#      (b) this run's post-subscribe message arrived.
#
# Key teaching: MQTT Events-Store subscriptions are ALWAYS StartNewOnly.
# There is NO historical replay — unlike KubeMQ gRPC/REST which supports 6 replay types.
#
# Note on the shared channel:
#   store/demo/s is a canonical demo channel that may carry concurrent traffic
#   from other clients (e.g. sibling examples in other languages running at the
#   same time). To stay correct under that traffic, every message this run
#   publishes is tagged with a unique per-run id plus a phase marker
#   ("pre"/"post"). On receive, each message is classified by run id into
#   own-pre / own-post / foreign; foreign messages (including non-matching
#   payloads) are ignored. The assertions only consider THIS run's tagged
#   messages, so the StartNewOnly guarantee stays provable without a brittle
#   exact-count assertion against the shared channel.
#
# Canonical test: integration test #4 EventsStoreStartNewOnly (store/it/s)
#
# Run: ruby events_store/start_new_only/main.rb
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

# Per-run tagged payload: "<run_id>|<phase>|<body>". The run id lets the
# subscriber tell THIS run's traffic apart from concurrent foreign traffic on
# the shared channel; the phase ("pre"/"post") distinguishes pre-subscribe
# messages (which must NOT be replayed) from the post-subscribe message.
def tag_payload(run_id, phase, body)
  "#{run_id}|#{phase}|#{body}"
end

# Classify a received payload by run id and phase. Anything that does not carry
# this run's id (including malformed payloads) is :foreign and ignored.
def classify(run_id, payload)
  parts = payload.to_s.split("|", 3)
  return :foreign if parts.length < 2 || parts[0] != run_id

  case parts[1]
  when "pre"  then :own_pre
  when "post" then :own_post
  else :foreign
  end
end

conn      = parse_url(BROKER_URL)
topic     = "store/demo/s"   # KubeMQ channel: demo.s (Events-Store)
run_id    = SecureRandom.uuid
pub_id    = "ruby-mqtt-evstore-pub-#{SecureRandom.hex(4)}"
sub_id    = "ruby-mqtt-evstore-sub-#{SecureRandom.hex(4)}"

puts "=== Events-Store: Start-New-Only (no replay) ==="
puts "Topic: #{topic}  (KubeMQ channel: demo.s)"
puts "MQTT version: 3.1.1  |  MQTT gem: njh/ruby-mqtt"
puts "Run ID: #{run_id}"
puts ""
puts "Teaching: MQTT Events-Store subscriptions ALWAYS start at StartNewOnly."
puts "          There is NO historical replay over MQTT (unlike gRPC/REST)."
puts ""

# Step 1: publish 3 messages BEFORE any subscription exists
puts "Step 1: Publishing 3 messages BEFORE subscription is established..."
MQTT::Client.connect(
  host:          conn[:host],
  port:          conn[:port],
  ssl:           conn[:ssl],
  client_id:     pub_id,
  clean_session: true,
  keep_alive:    30
) do |client|
  3.times do |i|
    payload = tag_payload(run_id, "pre", "pre-sub-message-#{i + 1}")
    client.publish(topic, payload, false, 1)
    puts "  Published (pre-sub): #{payload}"
  end
end
puts "  -> These messages are stored in KubeMQ Events-Store,"
puts "     but will NOT be delivered to any subscriber that connects AFTER them."
puts ""

sleep 0.3

received = Queue.new
sub_ready = Queue.new

# Step 2: subscribe — StartNewOnly is the ONLY mode over MQTT
puts "Step 2: Subscribing to #{topic} at QoS 1..."
puts "  Note: MQTT forces StartNewOnly — no way to request historical replay."
subscriber = Thread.new do
  MQTT::Client.connect(
    host:          conn[:host],
    port:          conn[:port],
    ssl:           conn[:ssl],
    client_id:     sub_id,
    clean_session: true,
    keep_alive:    30
  ) do |client|
    client.subscribe([topic, 1])
    sub_ready.push(:ready)
    loop do
      t, p = client.get
      received.push([t, p])
    end
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
puts "  Subscribed successfully."

# The MQTT SUBACK confirms the broker accepted the subscription, but when the
# Events-Store channel already holds messages, the store-side StartNewOnly
# subscription needs a brief moment to become active. Without this settle the
# post-sub publish can race ahead of the live subscription and be missed.
sleep 0.6
puts ""

# Step 3: publish 1 message AFTER subscription
puts "Step 3: Publishing 1 message AFTER subscription is established..."
MQTT::Client.connect(
  host:          conn[:host],
  port:          conn[:port],
  ssl:           conn[:ssl],
  client_id:     "#{pub_id}-b",
  clean_session: true,
  keep_alive:    30
) do |client|
  post_payload = tag_payload(run_id, "post", "post-sub-message-1")
  client.publish(topic, post_payload, false, 1)
  puts "  Published (post-sub): #{post_payload}"
end
puts "  -> This message was published AFTER the subscription; it WILL be delivered."
puts ""

# Step 4: collect — classify by run id, tolerating concurrent foreign traffic.
# Keep popping until THIS run's post-subscribe message arrives or we time out.
puts "Step 4: Collecting received messages (timeout: 5s)..."
own_pre  = []
own_post = []
foreign  = []
begin
  Timeout.timeout(5) do
    loop do
      t, p = received.pop
      case classify(run_id, p)
      when :own_pre
        own_pre << [t, p]
      when :own_post
        own_post << [t, p]
      else
        foreign << [t, p]
      end
      break unless own_post.empty?
    end
  end
rescue Timeout::Error
  puts "  Timeout waiting for this run's post-subscribe message"
end
# Brief drain window to catch any late replay of THIS run's pre-sub messages.
sleep 0.5
until received.empty?
  t, p = received.pop
  case classify(run_id, p)
  when :own_pre  then own_pre  << [t, p]
  when :own_post then own_post << [t, p]
  else foreign << [t, p]
  end
end

puts ""
puts "=== Result ==="
puts "This run's pre-subscribe msgs replayed : #{own_pre.size}  (StartNewOnly — expect 0)"
puts "This run's post-subscribe msgs received: #{own_post.size}  (expect >= 1)"
puts "Foreign messages ignored               : #{foreign.size}"
own_post.each_with_index { |(t, p), i| puts "  post[#{i + 1}] topic=#{t}  payload=#{p}" }

# (a) The StartNewOnly guarantee: NONE of this run's pre-subscribe messages are
#     replayed to a subscription that started after they were published.
# (b) This run's uniquely-tagged post-subscribe message IS delivered.
if own_pre.empty? && !own_post.empty?
  puts ""
  puts "OK — StartNewOnly confirmed:"
  puts "     3 pre-subscription messages: NOT replayed for this run (no replay)"
  puts "     1 post-subscription message: DELIVERED"
  puts "     MQTT events-store subscriptions are ALWAYS StartNewOnly — no historical replay."
else
  puts ""
  puts "UNEXPECTED result:"
  puts "  this run's pre-sub message was replayed (StartNewOnly violation)" unless own_pre.empty?
  puts "  this run's post-sub message was NOT delivered" if own_post.empty?
  raise "Assertion failed: pre-sub replayed=#{own_pre.size}, post-sub received=#{own_post.size}"
end

subscriber.kill

# Expected output (run id and foreign count vary per run):
# === Events-Store: Start-New-Only (no replay) ===
# Topic: store/demo/s  (KubeMQ channel: demo.s)
# MQTT version: 3.1.1  |  MQTT gem: njh/ruby-mqtt
# Run ID: 3f2a1c9e-...
#
# Teaching: MQTT Events-Store subscriptions ALWAYS start at StartNewOnly.
#           There is NO historical replay over MQTT (unlike gRPC/REST).
#
# Step 1: Publishing 3 messages BEFORE subscription is established...
#   Published (pre-sub): 3f2a1c9e-...|pre|pre-sub-message-1
#   Published (pre-sub): 3f2a1c9e-...|pre|pre-sub-message-2
#   Published (pre-sub): 3f2a1c9e-...|pre|pre-sub-message-3
#   -> These messages are stored in KubeMQ Events-Store,
#      but will NOT be delivered to any subscriber that connects AFTER them.
# Step 2: Subscribing to store/demo/s at QoS 1...
#   Note: MQTT forces StartNewOnly — no way to request historical replay.
#   Subscribed successfully.
# Step 3: Publishing 1 message AFTER subscription is established...
#   Published (post-sub): 3f2a1c9e-...|post|post-sub-message-1
#   -> This message was published AFTER the subscription; it WILL be delivered.
# Step 4: Collecting received messages (timeout: 5s)...
# === Result ===
# This run's pre-subscribe msgs replayed : 0  (StartNewOnly — expect 0)
# This run's post-subscribe msgs received: 1  (expect >= 1)
# Foreign messages ignored               : 0
#   post[1] topic=store/demo/s  payload=3f2a1c9e-...|post|post-sub-message-1
# OK — StartNewOnly confirmed:
#      3 pre-subscription messages: NOT replayed for this run (no replay)
#      1 post-subscription message: DELIVERED
#      MQTT events-store subscriptions are ALWAYS StartNewOnly — no historical replay.
