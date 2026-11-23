package normalize

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t testing.TB) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr:         mr.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		MaxRetries:   -1,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func newTestDeduper(t testing.TB, rdb redis.Cmdable) *Deduper {
	t.Helper()
	d, err := NewDeduper(rdb, 10*time.Minute)
	if err != nil {
		t.Fatalf("NewDeduper() error = %v", err)
	}
	return d
}

func TestDeduperVerdicts(t *testing.T) {
	mr, rdb := newTestRedis(t)
	d := newTestDeduper(t, rdb)
	ctx := testContext(t)
	idA := commitID(testBob, "create", "aaa111")
	idB := commitID(testCarol, "create", "aaa111")
	at := func(p int32, o int64) Position { return Position{Partition: p, Offset: o} }

	steps := []struct {
		name  string
		items []DedupItem
		want  []Verdict
	}{
		{name: "first time", items: []DedupItem{{idA, at(2, 10)}}, want: []Verdict{VerdictFirst}},
		{name: "same position again", items: []DedupItem{{idA, at(2, 10)}}, want: []Verdict{VerdictSamePosition}},
		{name: "other offset", items: []DedupItem{{idA, at(2, 11)}}, want: []Verdict{VerdictDuplicate}},
		{name: "other partition", items: []DedupItem{{idA, at(3, 10)}}, want: []Verdict{VerdictDuplicate}},
		{
			name:  "one event twice in one call",
			items: []DedupItem{{idB, at(1, 5)}, {idB, at(1, 9)}, {idA, at(2, 10)}},
			want:  []Verdict{VerdictFirst, VerdictDuplicate, VerdictSamePosition},
		},
		{name: "empty call", items: nil, want: nil},
	}
	for _, s := range steps {
		got, err := d.Check(ctx, s.items)
		if err != nil {
			t.Fatalf("%s: Check() error = %v", s.name, err)
		}
		if !reflect.DeepEqual(got, s.want) {
			t.Errorf("%s: Check() = %v, want %v", s.name, got, s.want)
		}
	}

	if got, err := mr.Get(DedupKey(idA)); err != nil || got != "2:10" {
		t.Errorf("key value = %q, %v, want %q", got, err, "2:10")
	}
	if got, err := mr.Get(DedupKey(idB)); err != nil || got != "1:5" {
		t.Errorf("key value = %q, %v, want %q", got, err, "1:5")
	}
}

func TestDeduperLifetime(t *testing.T) {
	mr, rdb := newTestRedis(t)
	d, err := NewDeduper(rdb, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := testContext(t)
	id := commitID(testBob, "create", "aaa111")
	if _, err := d.Check(ctx, []DedupItem{{id, Position{0, 1}}}); err != nil {
		t.Fatal(err)
	}
	if got := mr.TTL(DedupKey(id)); got != 90*time.Second {
		t.Errorf("TTL = %s, want 90s", got)
	}

	// A later check does not refresh the lifetime.
	mr.FastForward(30 * time.Second)
	if _, err := d.Check(ctx, []DedupItem{{id, Position{0, 1}}}); err != nil {
		t.Fatal(err)
	}
	if got := mr.TTL(DedupKey(id)); got != 60*time.Second {
		t.Errorf("TTL after a second check = %s, want 60s", got)
	}

	// After the lifetime the event counts as new again.
	mr.FastForward(61 * time.Second)
	got, err := d.Check(ctx, []DedupItem{{id, Position{0, 2}}})
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != VerdictFirst {
		t.Errorf("verdict after expiry = %v, want VerdictFirst", got[0])
	}
}

func TestDeduperRedisError(t *testing.T) {
	ctx := testContext(t)
	items := []DedupItem{{commitID(testBob, "create", "aaa111"), Position{0, 1}}}

	t.Run("command error", func(t *testing.T) {
		mr, rdb := newTestRedis(t)
		d := newTestDeduper(t, rdb)
		mr.SetError("LOADING synthetic error")
		got, err := d.Check(ctx, items)
		if err == nil || got != nil {
			t.Fatalf("Check() = %v, %v, want an error and no verdicts", got, err)
		}
	})
	t.Run("server down", func(t *testing.T) {
		mr, rdb := newTestRedis(t)
		d := newTestDeduper(t, rdb)
		mr.Close()
		got, err := d.Check(ctx, items)
		if err == nil || got != nil {
			t.Fatalf("Check() = %v, %v, want an error and no verdicts", got, err)
		}
		if errors.Is(err, redis.Nil) {
			t.Errorf("error is redis.Nil: %v", err)
		}
	})
}

func TestNewDeduperInvalid(t *testing.T) {
	_, rdb := newTestRedis(t)
	if _, err := NewDeduper(nil, time.Minute); err == nil {
		t.Error("NewDeduper(nil client) error = nil")
	}
	if _, err := NewDeduper(rdb, 0); err == nil {
		t.Error("NewDeduper(0 lifetime) error = nil")
	}
}

func TestVerdictProcess(t *testing.T) {
	if !VerdictFirst.Process() || !VerdictSamePosition.Process() || VerdictDuplicate.Process() || Verdict(0).Process() {
		t.Error("Process() is wrong for some verdict")
	}
}

// BenchmarkDedupCheck measures one Check call for a batch of 100 new events
// against miniredis. It shows the cost of building and parsing the
// pipeline, not the network cost of a real Redis.
func BenchmarkDedupCheck(b *testing.B) {
	_, rdb := newTestRedis(b)
	d := newTestDeduper(b, rdb)
	ctx := testContext(b)
	items := make([]DedupItem, 100)
	var offset int64
	for b.Loop() {
		for i := range items {
			offset++
			items[i] = DedupItem{EventID: "c:bench:" + Position{0, offset}.String(), Position: Position{0, offset}}
		}
		if _, err := d.Check(ctx, items); err != nil {
			b.Fatal(err)
		}
	}
}
