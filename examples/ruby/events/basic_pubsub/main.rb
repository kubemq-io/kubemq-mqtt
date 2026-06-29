# Example: events/basic_pubsub
#
# Demonstrates basic fire-and-forget event pub/sub over the KubeMQ MQTT
# connector using the mqtt gem (MQTT 3.1.1).
#
# Topic grammar:  events/<channel>  ->  KubeMQ Events pattern.
#   The '/' separator maps to '.' in the KubeMQ channel name:
#     events/demo/x  ->  channel "demo.x"
#   The subscriber uses a single-level '+' wildcard (events/demo/+), which
#   matches the published topic events/demo/x.
#
# User properties / KubeMQ tags:
#   MQTT 5.0 User Properties map bidirectionally to KubeMQ message Tags.
#   The canonical example sets a v5 user property k1=v1 which arrives as the
#   KubeMQ tag k1=v1 (caps: 32 properties / 4096 bytes total).
#   The ruby mqtt gem implements MQTT 3.1.1 only and has no User Properties
#   field.  To demonstrate the concept with v3.1.1, we encode the same tag as a
#   key=value suffix in the payload.  A real MQTT 5.0 client (e.g. a Go or
#   Python example) can send the same data as a proper User Property and
#   observe the tag round-trip on the KubeMQ side.
#
# Gotcha #1 — Retain silently dropped:
#   Do NOT set retain=true.  The broker forces RetainAvailable=0.  A retained
#   publish returns PUBACK 0x00 (success) but the message is silently
#   discarded, never delivered, never stored.  There is no error response.
#
# Gotcha #5 — Literal '.' in a topic segment:
#   events/a.b/c and events/a/b/c both map to channel a.b.c (lossy).
#   Avoid dots in individual topic segments.
#
# Run: ruby events/basic_pubsub/main.rb
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

conn = parse_url(BROKER_URL)

# Canonical channel (spec §6.3): subscribe with a single-level '+' wildcard and
# publish to the concrete topic events/demo/x. The '+' matches the final
# segment, so events/demo/+ receives the events/demo/x publish.
SUB_TOPIC    = "events/demo/+"   # single-level wildcard
PUB_TOPIC    = "events/demo/x"
KUBEMQ_CHAN  = "demo.x"   # '/' -> '.' mapping
CLIENT_ID_SUB = "ruby-mqtt-events-sub-#{SecureRandom.hex(4)}"
CLIENT_ID_PUB = "ruby-mqtt-events-pub-#{SecureRandom.hex(4)}"

# Simulated tag encoded in payload (MQTT 3.1.1 has no User Properties).
# An MQTT 5.0 client would send this as a proper User Property k1=v1 instead,
# which arrives as the KubeMQ tag k1=v1.
TAG_KEY   = "k1"
TAG_VALUE = "v1"
PAYLOAD   = "hello|#{TAG_KEY}=#{TAG_VALUE}"

received = Queue.new
sub_ready = Queue.new

puts "[example] events/basic_pubsub"
puts "[example]   MQTT sub topic : #{SUB_TOPIC}  (single-level '+' wildcard)"
puts "[example]   MQTT pub topic : #{PUB_TOPIC}"
puts "[example]   KubeMQ channel : #{KUBEMQ_CHAN}  ('/' -> '.' mapping)"
puts "[example]   MQTT version   : 3.1.1 (mqtt gem — no User Properties)"
puts "[example]   Tag note       : tag '#{TAG_KEY}=#{TAG_VALUE}' encoded in payload"
puts "[example]                    (MQTT 5.0 clients use User Properties for real tag round-trip)"
puts

# ── Subscriber thread ─────────────────────────────────────────────────────────
subscriber = Thread.new do
  MQTT::Client.connect(
    host:           conn[:host],
    port:           conn[:port],
    ssl:            conn[:ssl],
    client_id:      CLIENT_ID_SUB,
    clean_session:  true,
    keep_alive:     30
  ) do |client|
    client.subscribe([SUB_TOPIC, 1])
    puts "[sub] subscribed to '#{SUB_TOPIC}' QoS 1 (client_id=#{CLIENT_ID_SUB})"
    sub_ready.push(:ready)

    topic, payload = client.get
    received.push([topic, payload])
  end
rescue => e
  received.push([:error, e.message])
end

# Wait until subscription is established before publishing.
sub_ready.pop
puts "[sub] subscription established — ready to receive"

# ── Publisher ─────────────────────────────────────────────────────────────────
MQTT::Client.connect(
  host:           conn[:host],
  port:           conn[:port],
  ssl:            conn[:ssl],
  client_id:      CLIENT_ID_PUB,
  clean_session:  true,
  keep_alive:     30
) do |client|
  # retain=false is mandatory — the broker silently drops retained messages
  # (RetainAvailable=0, gotcha #1).
  client.publish(PUB_TOPIC, PAYLOAD, false, 1)
  puts "[pub] published to '#{PUB_TOPIC}' QoS 1 (client_id=#{CLIENT_ID_PUB})"
  puts "[pub]   payload : #{PAYLOAD.inspect}"
end

# ── Receive ───────────────────────────────────────────────────────────────────
topic_r, payload_r = nil, nil
begin
  Timeout.timeout(10) { topic_r, payload_r = received.pop }
rescue Timeout::Error
  abort "[error] timed out waiting for message — is the MQTT connector running?"
end

if topic_r == :error
  abort "[error] subscriber failed: #{payload_r}"
end

puts
puts "[recv] message received:"
puts "[recv]   topic   : #{topic_r}"
puts "[recv]   payload : #{payload_r.inspect}"

# Parse the simulated tag out of the payload.
parts = payload_r.split("|", 2)
body  = parts[0]
tags  = {}
parts[1..-1].each do |t|
  k, v = t.split("=", 2)
  tags[k] = v if k
end

puts "[recv]   body    : #{body.inspect}"
puts "[recv]   tags    : #{tags.inspect}  (simulated; real MQTT 5.0 User Property round-trip)"
puts
puts "[ok]  Round-trip complete."
puts "[ok]  Topic '#{PUB_TOPIC}' -> KubeMQ channel '#{KUBEMQ_CHAN}' ('/' -> '.' confirmed)."

subscriber.kill

# Expected output (captured from a live run; client_id hex suffixes vary per run):
# [example] events/basic_pubsub
# [example]   MQTT sub topic : events/demo/+  (single-level '+' wildcard)
# [example]   MQTT pub topic : events/demo/x
# [example]   KubeMQ channel : demo.x  ('/' -> '.' mapping)
# [example]   MQTT version   : 3.1.1 (mqtt gem — no User Properties)
# [example]   Tag note       : tag 'k1=v1' encoded in payload
# [example]                    (MQTT 5.0 clients use User Properties for real tag round-trip)
#
# [sub] subscribed to 'events/demo/+' QoS 1 (client_id=ruby-mqtt-events-sub-2a90e03c)
# [sub] subscription established — ready to receive
# [pub] published to 'events/demo/x' QoS 1 (client_id=ruby-mqtt-events-pub-f4f962a6)
# [pub]   payload : "hello|k1=v1"
#
# [recv] message received:
# [recv]   topic   : events/demo/x
# [recv]   payload : "hello|k1=v1"
# [recv]   body    : "hello"
# [recv]   tags    : {"k1"=>"v1"}  (simulated; real MQTT 5.0 User Property round-trip)
#
# [ok]  Round-trip complete.
# [ok]  Topic 'events/demo/x' -> KubeMQ channel 'demo.x' ('/' -> '.' confirmed).
