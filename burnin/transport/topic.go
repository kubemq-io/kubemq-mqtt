// Package transport provides the MQTT publish/subscribe abstraction for the
// burn-in harness. topic.go builds the connector's topic grammar; mqtt_client.go
// defines the Transport interface and its eclipse/paho.golang v5 implementation.
package transport

import "fmt"

// Topic prefixes select the KubeMQ pattern in the connector's topic grammar.
// (kubemq-server/connectors/mqtt): the prefix before the first '/' chooses the
// pattern; the remainder is the channel with '/' mapped to '.'.
const (
	PrefixEvents   = "events"
	PrefixStore    = "store"  // Events-Store
	PrefixQueues   = "queues" // Queues (produce)
	PrefixCommands = "commands"
	PrefixQueries  = "queries"
	PrefixReply    = "$reply" // v5 RPC response topic namespace ($reply/<clientID>/<suffix>)
	PrefixShare    = "$share" // v5 shared subscription ($share/<group>/<filter>)
)

// PublishTopic builds the MQTT publish topic for a pattern + channel.
// The connector maps '/' in the channel to '.' in the KubeMQ channel, so the
// channel argument here is the raw MQTT channel segment (e.g. "site1/temp").
//
//	events     -> events/<ch>      (Events)
//	events_store -> store/<ch>     (Events-Store, always StartNewOnly over MQTT)
//	queues     -> queues/<ch>      (Queues produce)
//	commands   -> commands/<ch>    (Commands RPC; v5 only)
//	queries    -> queries/<ch>     (Queries RPC; v5 only)
func PublishTopic(pattern, channel string) string {
	return prefixForPattern(pattern) + "/" + channel
}

// EventsSubscribeFilter builds an Events subscription filter. Wildcards
// ('+'->'*', '#'->'>') are allowed ONLY on Events subscriptions.
func EventsSubscribeFilter(channel string) string {
	return PrefixEvents + "/" + channel
}

// StoreSubscribeFilter builds an Events-Store subscription filter.
func StoreSubscribeFilter(channel string) string {
	return PrefixStore + "/" + channel
}

// SharedQueueFilter builds the MQTT 5.0 shared-subscription filter for consuming
// from a Queues channel: $share/<group>/queues/<ch>. Must be subscribed at
// QoS >= 1 (QoS 0 -> SUBACK 0x83; a plain queues/... subscribe -> SUBACK 0x83).
func SharedQueueFilter(group, channel string) string {
	return fmt.Sprintf("%s/%s/%s/%s", PrefixShare, group, PrefixQueues, channel)
}

// ReplyTopic builds a client's own response-topic namespace for v5 RPC:
// $reply/<clientID>/<suffix>. The client SUBSCRIBES here first, then publishes
// to commands/queries with Properties.ResponseTopic set to this value. The
// suffix must be in the client's OWN clientID namespace (else PUBACK 0x83).
func ReplyTopic(clientID, suffix string) string {
	return fmt.Sprintf("%s/%s/%s", PrefixReply, clientID, suffix)
}

// RPCPublishTopic builds the commands/<ch> or queries/<ch> publish topic.
func RPCPublishTopic(pattern, channel string) string {
	return prefixForPattern(pattern) + "/" + channel
}

func prefixForPattern(pattern string) string {
	switch pattern {
	case "events":
		return PrefixEvents
	case "events_store":
		return PrefixStore
	case "queues":
		return PrefixQueues
	case "commands":
		return PrefixCommands
	case "queries":
		return PrefixQueries
	default:
		return PrefixEvents
	}
}
