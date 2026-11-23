// Package bus publishes pipeline events to NATS JetStream and reads the
// resume position back from it.
package bus

import (
	"fmt"
	"hash/fnv"

	"github.com/ZAUakaAlexey/threadroute/internal/model"
)

// Stream and subject layout. Every consumer depends on these names.
const (
	// StreamEvents is the JetStream stream that holds connector output.
	StreamEvents = "EVENTS"
	// SubjectAll matches every subject of StreamEvents.
	SubjectAll = "tr.>"

	// subjectMessagePrefix is followed by a two-digit shard: tr.msg.07.
	subjectMessagePrefix = "tr.msg."
	// SubjectMessages matches every message shard.
	SubjectMessages = "tr.msg.*"
	// SubjectDeleted carries message deletions. A deletion does not name its
	// conversation, so it cannot be put on a conversation shard.
	SubjectDeleted = "tr.deleted"
	// SubjectAccount carries account status changes.
	SubjectAccount = "tr.account"
)

// Headers set on every published event.
const (
	// HeaderHost, HeaderSeq and HeaderWitnessed store the source cursor, so the
	// last message of the stream is also the resume position of the connector.
	HeaderHost      = "Tr-Host"
	HeaderSeq       = "Tr-Seq"
	HeaderWitnessed = "Tr-Witnessed" // unix microseconds
	HeaderKind      = "Tr-Kind"
)

// MaxShards is the largest shard count the subject format allows.
const MaxShards = 100

// Shard maps a conversation to one of n shards. All messages of one
// conversation get the same shard, so one consumer sees them in order.
//
// Changing n moves conversations between shards and breaks ordering for
// conversations that are open at that moment.
func Shard(conversationID string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(conversationID))
	return int(h.Sum32() % uint32(n))
}

// Subject returns the subject an event is published on.
func Subject(ev model.Event, shards int) (string, error) {
	switch ev.Kind {
	case model.KindMessage:
		if ev.Message == nil {
			return "", fmt.Errorf("event %s: kind %q without message", ev.DedupID, ev.Kind)
		}
		return MessageSubject(Shard(ev.Message.ConversationID, shards)), nil
	case model.KindMessageDeleted:
		return SubjectDeleted, nil
	case model.KindAccount:
		return SubjectAccount, nil
	default:
		return "", fmt.Errorf("event %s: unknown kind %q", ev.DedupID, ev.Kind)
	}
}

// MessageSubject returns the subject of one message shard.
func MessageSubject(shard int) string {
	return fmt.Sprintf("%s%02d", subjectMessagePrefix, shard)
}
