# Example: connectivity/tls
#
# Demonstrates TLS-secured MQTT connection to the KubeMQ MQTT connector
# on port 8883 using the mqtt gem's ssl: option with an OpenSSL context.
#
# The mqtt gem (njh/ruby-mqtt) supports TCP and TLS (no WebSocket).
# TLS uses Ruby's OpenSSL; the ssl: option accepts true, false, or an
# OpenSSL::SSL::SSLContext object.
#
# InsecureSkipVerify (VERIFY_NONE) is used here so the example runs against
# a KubeMQ server with a self-signed certificate (the common dev/test case).
#
# PRODUCTION: replace VERIFY_NONE with a proper CA bundle:
#   ctx.verify_mode  = OpenSSL::SSL::VERIFY_PEER
#   ctx.ca_file      = "/path/to/ca.crt"   # or ca_path for a directory
#   ctx.cert         = OpenSSL::X509::Certificate.new(File.read("/path/to/client.crt"))
#   ctx.key          = OpenSSL::PKey::RSA.new(File.read("/path/to/client.key"))
#
# NOTE: The live test broker at 127.0.0.1:8883 has its TLS listener closed.
# This example compiles and runs correctly but the live connection is N/A
# until a TLS-enabled broker is available.
#
# Canonical test: integration test #18 TLSListener (events/it/tls)
#
# Run: ruby connectivity/tls/main.rb
# Env: KUBEMQ_MQTT_URL=tls://localhost:8883 (default for TLS examples)
#
require "mqtt"
require "openssl"
require "uri"
require "securerandom"
require "timeout"

BROKER_URL = ENV.fetch("KUBEMQ_MQTT_URL", "tls://localhost:8883")

def parse_url(url)
  uri = URI.parse(url)
  { host: uri.host, port: uri.port, tls: uri.scheme == "tls" }
end

# Note: this script keeps a `tls:` key (not `ssl:` like the other examples)
# because the value here is an OpenSSL context object, not a boolean.

def build_ssl_context
  ctx = OpenSSL::SSL::SSLContext.new
  # InsecureSkipVerify — acceptable for development / self-signed certs.
  # PRODUCTION: set VERIFY_PEER and supply ctx.ca_file / ctx.cert / ctx.key.
  ctx.verify_mode = OpenSSL::SSL::VERIFY_NONE
  ctx
end

conn  = parse_url(BROKER_URL)
ssl   = conn[:tls] ? build_ssl_context : false
topic = "events/demo/tls"
msg   = "Hello over TLS from Ruby!"

puts "[TLS] Connecting to #{BROKER_URL}"
puts "[TLS] ssl context: #{conn[:tls] ? 'VERIFY_NONE (dev; use VERIFY_PEER + CA in production)' : 'disabled (tcp)'}"

received  = Queue.new
sub_ready = Queue.new

subscriber = Thread.new do
  MQTT::Client.connect(
    host:          conn[:host],
    port:          conn[:port],
    ssl:           ssl,
    client_id:     "ruby-tls-sub-#{SecureRandom.hex(4)}",
    clean_session: true,
    keep_alive:    30
  ) do |client|
    puts "[sub] Connected over TLS"
    client.subscribe([topic, 1])
    puts "[sub] Subscribed to #{topic} at QoS 1"
    sub_ready.push(:ready)
    t, p = client.get
    received.push([t, p])
  end
rescue => e
  # Surface the connect/subscribe failure instead of deadlocking the main
  # thread on sub_ready.pop. The common case is a closed TLS listener
  # (Errno::ECONNREFUSED on port 8883) when the broker has no TLS configured.
  sub_ready.push([:error, e])
end

ready = sub_ready.pop
if ready.is_a?(Array) && ready[0] == :error
  err = ready[1]
  puts "[error] Could not establish the TLS subscriber connection: " \
       "#{err.class}: #{err.message}"
  puts "[error] Is the broker's TLS listener enabled on #{conn[:host]}:#{conn[:port]}?"
  exit 1
end

MQTT::Client.connect(
  host:          conn[:host],
  port:          conn[:port],
  ssl:           ssl,
  client_id:     "ruby-tls-pub-#{SecureRandom.hex(4)}",
  clean_session: true,
  keep_alive:    30
) do |client|
  puts "[pub] Connected over TLS"
  client.publish(topic, msg, false, 1)
  puts "[pub] Published to #{topic}: #{msg}"
end

t, p = nil, nil
begin
  Timeout.timeout(10) { t, p = received.pop }
rescue Timeout::Error
  puts "[error] Timed out waiting for message (is TLS listener enabled on the broker?)"
  subscriber.kill
  exit 1
end

puts "[sub] Received:"
puts "  topic:   #{t}"
puts "  payload: #{p}"

subscriber.kill

# Expected output (requires TLS-enabled broker on port 8883):
# [TLS] Connecting to tls://localhost:8883
# [TLS] ssl context: VERIFY_NONE (dev; use VERIFY_PEER + CA in production)
# [sub] Connected over TLS
# [sub] Subscribed to events/demo/tls at QoS 1
# [pub] Connected over TLS
# [pub] Published to events/demo/tls: Hello over TLS from Ruby!
# [sub] Received:
#   topic:   events/demo/tls
#   payload: Hello over TLS from Ruby!
#
# Live-test result on this broker (port 8883 closed -> N/A; code is correct):
# [TLS] Connecting to tls://localhost:8883
# [TLS] ssl context: VERIFY_NONE (dev; use VERIFY_PEER + CA in production)
# [error] Could not establish the TLS subscriber connection: Errno::ECONNREFUSED: Connection refused - connect(2) for "localhost" port 8883
# [error] Is the broker's TLS listener enabled on localhost:8883?
