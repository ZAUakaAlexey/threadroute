package conv

import (
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestNewTextReader(t *testing.T) {
	if _, err := NewTextReader(nil, ""); err == nil {
		t.Error("NewTextReader(nil) error = nil, want an error")
	}
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { rdb.Close() })
	r, err := NewTextReader(rdb, "")
	if err != nil {
		t.Fatalf("NewTextReader() error = %v", err)
	}
	if r.prefix != DefaultKeyPrefix {
		t.Errorf("prefix = %q, want %q", r.prefix, DefaultKeyPrefix)
	}
	// The bodies are written with the conv change of stage 4. Until then
	// they say so clearly.
	if _, err := r.Messages(t.Context(), "a", "b"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Messages() error = %v, want ErrNotImplemented", err)
	}
	if _, _, err := r.Message(t.Context(), "a", "b", "m1"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Message() error = %v, want ErrNotImplemented", err)
	}
}
