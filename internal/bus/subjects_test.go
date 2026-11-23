package bus

import (
	"testing"

	"github.com/ZAUakaAlexey/threadroute/internal/model"
)

func TestShardIsStableAndInRange(t *testing.T) {
	const shards = 16
	ids := []string{"", "conv-a", "conv-b", "at://did:example:alice/app.example.post/1"}
	for _, id := range ids {
		got := Shard(id, shards)
		if got < 0 || got >= shards {
			t.Errorf("Shard(%q) = %d, want 0..%d", id, got, shards-1)
		}
		if again := Shard(id, shards); again != got {
			t.Errorf("Shard(%q) is not stable: %d then %d", id, got, again)
		}
	}
}

func TestSubject(t *testing.T) {
	msg := &model.Message{ID: "m2", ConversationID: "conv-a", ParentID: "m1"}
	tests := []struct {
		name    string
		ev      model.Event
		want    string
		wantErr bool
	}{
		{"message", model.Event{Kind: model.KindMessage, Message: msg}, MessageSubject(Shard("conv-a", 16)), false},
		{"deleted", model.Event{Kind: model.KindMessageDeleted}, SubjectDeleted, false},
		{"account", model.Event{Kind: model.KindAccount}, SubjectAccount, false},
		{"message without payload", model.Event{Kind: model.KindMessage}, "", true},
		{"unknown kind", model.Event{Kind: "other"}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Subject(tt.ev, 16)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Subject() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Subject() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMessageSubjectFormat(t *testing.T) {
	if got := MessageSubject(7); got != "tr.msg.07" {
		t.Errorf("MessageSubject(7) = %q, want tr.msg.07", got)
	}
}
