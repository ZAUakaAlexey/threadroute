package normalize

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// fakeHello answers every command with a fixed reply, as a stand-in for a
// server whose version miniredis cannot report.
type fakeHello struct {
	reply any
	err   error
}

func (f fakeHello) Do(ctx context.Context, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx, args...)
	if f.err != nil {
		cmd.SetErr(f.err)
	} else {
		cmd.SetVal(f.reply)
	}
	return cmd
}

func TestCheckRedisVersionMiniredis(t *testing.T) {
	// miniredis answers HELLO with a fixed version, 8.4.0, so only the
	// passing case can run against it.
	_, rdb := newTestRedis(t)
	if err := CheckRedisVersion(testContext(t), rdb); err != nil {
		t.Errorf("CheckRedisVersion(miniredis) error = %v", err)
	}
}

func TestCheckRedisVersionErrors(t *testing.T) {
	t.Run("server error", func(t *testing.T) {
		mr, rdb := newTestRedis(t)
		mr.SetError("ERR synthetic error")
		if err := CheckRedisVersion(testContext(t), rdb); err == nil {
			t.Error("CheckRedisVersion() error = nil")
		}
	})
	t.Run("server down", func(t *testing.T) {
		mr, rdb := newTestRedis(t)
		mr.Close()
		if err := CheckRedisVersion(testContext(t), rdb); err == nil {
			t.Error("CheckRedisVersion() error = nil")
		}
	})
}

func TestCheckRedisVersionReplies(t *testing.T) {
	tests := []struct {
		name    string
		reply   any
		wantErr string
	}{
		{name: "resp3 map new enough", reply: map[any]any{"server": "redis", "version": "7.0.0"}},
		{name: "resp3 map newer", reply: map[any]any{"version": "8.2.1"}},
		{name: "string map", reply: map[string]any{"version": "7.4.2"}},
		{name: "resp2 list", reply: []any{"server", "redis", "version", "7.2.4", "proto", int64(2)}},
		{name: "too old", reply: map[any]any{"version": "6.2.14"}, wantErr: "too old"},
		{name: "too old in a list", reply: []any{"version", "6.0.0"}, wantErr: "too old"},
		{name: "no version", reply: map[any]any{"server": "redis"}, wantErr: "no version"},
		{name: "version not a string", reply: map[any]any{"version": int64(7)}, wantErr: "no version"},
		{name: "unexpected reply", reply: "OK", wantErr: "no version"},
		{name: "malformed version", reply: map[any]any{"version": "seven"}, wantErr: "major.minor.patch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckRedisVersion(testContext(t), fakeHello{reply: tt.reply})
			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("CheckRedisVersion() error = %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Errorf("CheckRedisVersion() error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestRequireVersion(t *testing.T) {
	tests := []struct {
		version string
		ok      bool
	}{
		{"7.0.0", true}, {"7", true}, {"7.0", true}, {"7.0.15", true}, {"10.0.0", true},
		{"6.9.99", false}, {"6", false}, {"0.0.1", false},
		{"", false}, {"7.x", false}, {"7.0.0.1", false}, {"-7.0.0", false},
	}
	for _, tt := range tests {
		if err := requireVersion(tt.version, MinRedisVersion); (err == nil) != tt.ok {
			t.Errorf("requireVersion(%q) error = %v, want ok %v", tt.version, err, tt.ok)
		}
	}
}
