# Example: connectivity/auth_jwt
#
# Demonstrates password-as-JWT authentication to the KubeMQ MQTT connector.
#
# Auth protocol:
#   - CONNECT Password field = KubeMQ JWT token (PasswordFlag must be true)
#   - Username is DISPLAY-ONLY (not used for authentication)
#   - Empty password when auth is enabled -> CONNACK 0x86 (bad username/password)
#   - Auth is checked at connect-time only (no mid-connection recheck)
#   - Auth not configured on broker -> open (any/no password accepted)
#
# NOTE: The live test broker at 127.0.0.1:1883 has auth DISABLED.
# Connecting with a JWT in the password field succeeds, but JWT rejection
# is NOT enforced -> full JWT enforcement requires an auth-enabled broker.
# This example is marked partial/NA for live testing.
#
# Canonical tests: #16 AuthRequired (HS256 JWT; wrong password -> 0x86),
#                  #17 ACLDenied (-> 0x87 on publish)
#
# Run: ruby connectivity/auth_jwt/main.rb
# Env: KUBEMQ_MQTT_URL=tcp://localhost:1883 (default)
#      KUBEMQ_JWT_TOKEN=<your-jwt-token>     (optional; test with dummy on open broker)
#
require "mqtt"
require "uri"
require "securerandom"
require "timeout"

BROKER_URL = ENV.fetch("KUBEMQ_MQTT_URL", "tcp://localhost:1883")
JWT_TOKEN  = ENV.fetch("KUBEMQ_JWT_TOKEN", "dummy-jwt-token-for-open-broker")

def parse_url(url)
  uri = URI.parse(url)
  { host: uri.host, port: uri.port, ssl: uri.scheme == "tls" }
end

conn  = parse_url(BROKER_URL)
topic = "events/demo/auth"

received  = Queue.new
sub_ready = Queue.new

subscriber = Thread.new do
  MQTT::Client.connect(
    host:          conn[:host],
    port:          conn[:port],
    ssl:           conn[:ssl],
    client_id:     "ruby-auth-sub-#{SecureRandom.hex(4)}",
    username:      "ruby-example",   # display-only; not used for authentication
    password:      JWT_TOKEN,        # KubeMQ JWT token (PasswordFlag=true)
    clean_session: true,
    keep_alive:    30
  ) do |client|
    client.subscribe([topic, 1])
    sub_ready.push(:ready)
    t, p = client.get
    received.push([t, p])
  end
rescue => e
  # Surface the connect/subscribe failure instead of deadlocking the main
  # thread on sub_ready.pop. On an auth-enabled broker a bad/empty JWT yields
  # CONNACK 0x86 (bad username/password), which the gem raises here.
  sub_ready.push([:error, e])
end

ready = sub_ready.pop
if ready.is_a?(Array) && ready[0] == :error
  err = ready[1]
  abort "[error] Subscriber connect/subscribe failed: #{err.class}: #{err.message}\n" \
        "[error] On an auth-enabled broker a bad or empty JWT returns CONNACK 0x86."
end
puts "Subscriber connected (JWT auth; broker enforces if auth is enabled)"

MQTT::Client.connect(
  host:          conn[:host],
  port:          conn[:port],
  ssl:           conn[:ssl],
  client_id:     "ruby-auth-pub-#{SecureRandom.hex(4)}",
  username:      "ruby-example",
  password:      JWT_TOKEN,
  clean_session: true,
  keep_alive:    30
) do |client|
  client.publish(topic, "Hello with JWT auth!", false, 1)
  puts "Published to #{topic}"
end

t, p = nil, nil
Timeout.timeout(10) { t, p = received.pop }
puts "Received:"
puts "  topic:   #{t}"
puts "  payload: #{p}"

subscriber.kill

# Expected output (open broker or valid JWT on auth-enabled broker):
# Subscriber connected (JWT auth; broker enforces if auth is enabled)
# Published to events/demo/auth
# Received:
#   topic:   events/demo/auth
#   payload: Hello with JWT auth!
