# Example: events/wildcard_subscribe
#
# Demonstrates overlapping MQTT wildcard subscriptions on Events and the
# "one copy per matching bridge registry entry" behavior (gotcha #6), using a
# PER-RUN UNIQUE channel so the count is deterministic on a busy/shared broker
# (mirrors the Go/Java reference examples; integration test #25).
#
# A per-run unique id scopes the channel so no prior/other run reuses it:
#   pub_topic    = events/leak/<id>/x       (the single publish target)
#   exact filter = events/leak/<id>/x       -> KubeMQ leak.<id>.x  (exact)
#   hash filter  = events/leak/<id>/#       -> KubeMQ leak.<id>.>  ('#' -> '>')
#   bare filter  = #                        -> KubeMQ '>'          (INFORMATIONAL)
#
# Three SEPARATE subscriber clients (distinct client_ids => three INDEPENDENT
# bridge registry entries). Publishing "events/leak/<id>/x" ONCE routes through
# the array to all THREE matching entries, and each entry re-injects the topic
# back into the broker. With no cross-entry deduplication (gotcha #6; MQTT 5.0
# §3.3.5 permits one copy per matching subscription), EVERY subscriber whose
# filter matches receives copies — one per matching bridge entry.
#
#   - The two UNIQUE filters (exact + scoped '#') are scoped to <id>, so only
#     the three entries above match: each receives EXACTLY 3 copies.
#   - The bare "#" filter ALSO matches every Events topic, so it receives the
#     same 3 copies PLUS any ambient Events traffic on a shared broker. It is
#     therefore treated as INFORMATIONAL (assert >= 3, not == 3).
#
# IMPORTANT: the copies come from the THREE bridge entries, not from stacking
# multiple filters on one client. MQTT collapses overlapping filters registered
# on the SAME client into a single delivery, so this example uses three separate
# clients (mirroring the canonical test) to reproduce the contract faithfully.
#
# Contrast: subscribing to "store/#" (non-events) -> SUBACK 0xA2 (wildcard
# subscriptions are supported only on the events pattern). The njh/ruby-mqtt gem
# (MQTT 3.1.1) issues SUBSCRIBE fire-and-forget and does not surface the SUBACK
# reason code, so this contrast is DOCUMENTED here rather than asserted; the Go,
# Java, Python, JavaScript, C#, and Rust examples assert the 0xA2 reject directly.
#
# Canonical test: integration test #25 QueueNoWildcardLeak
#   (connector_integration_test.go:1176-1207, wantCopies = 3)
#
# Run: ruby events/wildcard_subscribe/main.rb
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

# Per-run unique id so this example's overlapping filters target a freshly
# minted KubeMQ channel that no prior/other run reuses.
RUN_ID    = SecureRandom.hex(4)
pub_topic = "events/leak/#{RUN_ID}/x"

# Filters: two UNIQUE (scoped to RUN_ID) + one bare informational "#".
EXACT_FILTER = pub_topic                     # exact            -> leak.<id>.x
HASH_FILTER  = "events/leak/#{RUN_ID}/#"     # scoped multi-lvl -> leak.<id>.>
BARE_FILTER  = "#"                           # bare informational -> '>'

# The two filters we assert EXACTLY 3 copies on (deterministic), and the bare
# "#" filter which is informational only (>= 3, varies with ambient traffic).
UNIQUE_FILTERS = [EXACT_FILTER, HASH_FILTER].freeze
SUB_FILTERS    = [EXACT_FILTER, HASH_FILTER, BARE_FILTER].freeze

WANT_COPIES = 3 # 3 matching bridge entries -> 3 copies per matching subscriber

# Unique payload so we count ONLY copies of our own publish — the "#" filter
# also matches unrelated events traffic on a busy/shared broker (e.g. internal
# kubemq.cluster.* snapshots or other clients). The gotcha is about how many
# copies of THIS publish arrive, not about total broker traffic.
PAYLOAD = "wildcard-overlap-test-#{SecureRandom.hex(6)}"

# Per-filter copy counts of OUR publish, plus a mutex guarding them.
counts    = Hash.new(0)
counts_mu = Mutex.new
ready     = Queue.new

puts "=== Events wildcard overlap: one copy per matching bridge entry ==="
puts "Per-run unique channel : #{pub_topic}"
puts "Publish topic          : #{pub_topic}"
puts "Subscribers            : #{SUB_FILTERS.size} separate clients, one filter each — " \
     "#{SUB_FILTERS.map(&:inspect).join(', ')}"
puts "  '+' maps to KubeMQ '*' (single-level wildcard)"
puts "  '#' maps to KubeMQ '>' (multi-level wildcard)"
puts "  Non-events wildcards (store/#, queues/#, ...) return SUBACK 0xA2."
puts "Each distinct filter is an independent bridge entry; all three match the publish."
puts ""

subscribers = SUB_FILTERS.map do |filter|
  Thread.new do
    MQTT::Client.connect(
      host:          conn[:host],
      port:          conn[:port],
      ssl:           conn[:ssl],
      client_id:     "ruby-mqtt-wildcard-#{SecureRandom.hex(4)}",
      clean_session: true,
      keep_alive:    30
    ) do |client|
      client.subscribe([filter, 1])
      ready.push(filter)
      loop do
        topic, payload = client.get
        next unless topic == pub_topic && payload == PAYLOAD
        counts_mu.synchronize { counts[filter] += 1 }
      end
    end
  rescue => e
    ready.push([:error, filter, e.message])
  end
end

# Wait until all subscriptions are established.
SUB_FILTERS.size.times do
  r = ready.pop
  if r.is_a?(Array) && r[0] == :error
    abort "[error] subscriber for filter #{r[1].inspect} failed: #{r[2]}"
  end
end
# Small settle so all SUBSCRIBE/bridge registrations are live before publishing.
sleep 0.6

# Publish exactly once to the per-run unique topic.
MQTT::Client.connect(
  host:          conn[:host],
  port:          conn[:port],
  ssl:           conn[:ssl],
  client_id:     "ruby-mqtt-wildcard-pub-#{SecureRandom.hex(4)}",
  clean_session: true,
  keep_alive:    30
) do |client|
  client.publish(pub_topic, PAYLOAD, false, 1)
  puts "Published once to: #{pub_topic}"
end

# Let the fan-out settle, then read the per-filter copy counts.
sleep 2.5
final = counts_mu.synchronize { counts.dup }

puts ""
puts "Copies of our publish received per subscriber:"
SUB_FILTERS.each do |filter|
  label = filter == BARE_FILTER ? "(informational, >= #{WANT_COPIES})" : "(unique, expected #{WANT_COPIES})"
  puts "  filter=#{filter.inspect.ljust(24)} copies=#{final[filter].to_i}  #{label}"
end

subscribers.each(&:kill)

# Assert EXACTLY WANT_COPIES on the two UNIQUE filters (deterministic). The bare
# "#" filter is informational: assert >= WANT_COPIES (it also picks up ambient
# Events traffic on a shared broker — that is gotcha #6 itself, not an error).
bad_unique = UNIQUE_FILTERS.reject { |f| final[f].to_i == WANT_COPIES }
unless bad_unique.empty?
  raise "Expected exactly #{WANT_COPIES} copies on each unique filter; wrong for: " \
        "#{bad_unique.map { |f| "#{f.inspect}=#{final[f].to_i}" }.join(', ')}"
end

bare = final[BARE_FILTER].to_i
if bare < WANT_COPIES
  raise "Expected >= #{WANT_COPIES} copies on bare '#' filter, got #{bare}"
end

puts ""
puts "OK — gotcha #6 confirmed: #{WANT_COPIES} matching bridge entries -> #{WANT_COPIES} copies " \
     "on each unique filter, no cross-entry dedup."
if bare > WANT_COPIES
  puts "     (bare '#' saw #{bare} copies — #{bare - WANT_COPIES} extra from ambient Events " \
       "traffic on the shared broker; '#' maps to the global '>' channel, gotcha #6, not an error.)"
end

# Contrast (documented, not asserted in Ruby): a wildcard subscribe on a
# non-events pattern such as "store/#" is rejected with SUBACK 0xA2
# (wildcard-subscriptions-not-supported). The njh/ruby-mqtt gem (MQTT 3.1.1)
# sends SUBSCRIBE fire-and-forget and does not surface the SUBACK reason code,
# so this is documented here; the Go/Java/Python/JS/C#/Rust examples assert it.
puts ""
puts "Contrast: subscribing to a non-events wildcard 'store/#' would return"
puts "          SUBACK 0xA2 (wildcard-subscriptions-not-supported). Wildcards are"
puts "          accepted ONLY on Events subscriptions; store/#, queues/#,"
puts "          commands/#, queries/# are all rejected with 0xA2 (integration test #14)."

# Expected output (captured from a live run; <id> and the payload suffix are
# randomized per run — here <id>=13a1a320. Verified green across 3 runs):
# === Events wildcard overlap: one copy per matching bridge entry ===
# Per-run unique channel : events/leak/13a1a320/x
# Publish topic          : events/leak/13a1a320/x
# Subscribers            : 3 separate clients, one filter each — "events/leak/13a1a320/x", "events/leak/13a1a320/#", "#"
#   '+' maps to KubeMQ '*' (single-level wildcard)
#   '#' maps to KubeMQ '>' (multi-level wildcard)
#   Non-events wildcards (store/#, queues/#, ...) return SUBACK 0xA2.
# Each distinct filter is an independent bridge entry; all three match the publish.
#
# Published once to: events/leak/13a1a320/x
#
# Copies of our publish received per subscriber:
#   filter="events/leak/13a1a320/x" copies=3  (unique, expected 3)
#   filter="events/leak/13a1a320/#" copies=3  (unique, expected 3)
#   filter="#"                      copies=3  (informational, >= 3)
#
# OK — gotcha #6 confirmed: 3 matching bridge entries -> 3 copies on each unique filter, no cross-entry dedup.
#
# Contrast: subscribing to a non-events wildcard 'store/#' would return
#           SUBACK 0xA2 (wildcard-subscriptions-not-supported). Wildcards are
#           accepted ONLY on Events subscriptions; store/#, queues/#,
#           commands/#, queries/# are all rejected with 0xA2 (integration test #14).
