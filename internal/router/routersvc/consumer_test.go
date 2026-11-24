package routersvc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

const testTopic = "conversations-test"

// fakeAdmin answers the offset requests of the catch-up.
type fakeAdmin struct {
	start, end map[int32]int64
	committed  map[int32]int64
	err        error
	// fails makes the next calls of FetchOffsets fail.
	fails int
}

func listed(m map[int32]int64) kadm.ListedOffsets {
	out := kadm.ListedOffsets{testTopic: {}}
	for p, o := range m {
		out[testTopic][p] = kadm.ListedOffset{Topic: testTopic, Partition: p, Offset: o}
	}
	return out
}

func (f *fakeAdmin) ListStartOffsets(context.Context, ...string) (kadm.ListedOffsets, error) {
	return listed(f.start), f.err
}

func (f *fakeAdmin) ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error) {
	return listed(f.end), f.err
}

func (f *fakeAdmin) FetchOffsets(context.Context, string) (kadm.OffsetResponses, error) {
	if f.fails > 0 {
		f.fails--
		return nil, kerr.CoordinatorNotAvailable
	}
	out := kadm.OffsetResponses{}
	for p, at := range f.committed {
		if out[testTopic] == nil {
			out[testTopic] = map[int32]kadm.OffsetResponse{}
		}
		out[testTopic][p] = kadm.OffsetResponse{Offset: kadm.Offset{Topic: testTopic, Partition: p, At: at}}
	}
	return out, nil
}

func TestBacklog(t *testing.T) {
	tests := []struct {
		name          string
		admin         fakeAdmin
		wantRemaining []int32
		wantBehind    int64
	}{
		{
			name:  "new group on an empty topic",
			admin: fakeAdmin{start: map[int32]int64{0: 0, 1: 0}, end: map[int32]int64{0: 0, 1: 0}},
		},
		{
			name:          "new group starts at the log start",
			admin:         fakeAdmin{start: map[int32]int64{0: 5, 1: 0}, end: map[int32]int64{0: 8, 1: 2}},
			wantRemaining: []int32{0, 1}, wantBehind: 5,
		},
		{
			name: "group at the end",
			admin: fakeAdmin{start: map[int32]int64{0: 0, 1: 0}, end: map[int32]int64{0: 8, 1: 2},
				committed: map[int32]int64{0: 8, 1: 2}},
		},
		{
			name: "group partly behind",
			admin: fakeAdmin{start: map[int32]int64{0: 0, 1: 0, 2: 0}, end: map[int32]int64{0: 8, 1: 2, 2: 4},
				committed: map[int32]int64{0: 6, 1: 2}},
			wantRemaining: []int32{0, 2}, wantBehind: 6,
		},
		{
			name: "committed offset older than the log start",
			admin: fakeAdmin{start: map[int32]int64{0: 10}, end: map[int32]int64{0: 12},
				committed: map[int32]int64{0: 3}},
			wantRemaining: []int32{0}, wantBehind: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &consumer{admin: &tt.admin, topic: testTopic, group: "g"}
			pos, end, err := c.backlog(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if got := remainingPartitions(pos, end); !slices.Equal(got, tt.wantRemaining) {
				t.Errorf("remaining %v, want %v", got, tt.wantRemaining)
			}
			if got := behind(pos, end); got != tt.wantBehind {
				t.Errorf("behind %d, want %d", got, tt.wantBehind)
			}
		})
	}

	c := &consumer{admin: &fakeAdmin{err: errors.New("no broker")}, topic: testTopic, group: "g"}
	if _, _, err := c.backlog(t.Context()); err == nil {
		t.Error("backlog() with an admin error = nil error")
	}
}

func TestIsDataError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "22021"}, true}, // invalid byte sequence
		{fmt.Errorf("apply: %w", &pgconn.PgError{Code: "23514"}), true},
		{&pgconn.PgError{Code: "54000"}, true}, // program limit exceeded
		{&pgconn.PgError{Code: "08006"}, false},
		{&pgconn.PgError{Code: "40P01"}, false},
		{&pgconn.PgError{Code: "2"}, false},
		{errors.New("plain"), false},
		{context.DeadlineExceeded, false},
	}
	for _, tt := range tests {
		if got := isDataError(tt.err); got != tt.want {
			t.Errorf("isDataError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestApplyResult(t *testing.T) {
	tests := []struct {
		r    router.ApplyResult
		want string
	}{
		{router.ApplyResult{Skipped: true}, resultDuplicate},
		{router.ApplyResult{Queued: true, Ended: true}, resultQueued},
		{router.ApplyResult{Pending: true}, resultPending},
		{router.ApplyResult{Ended: true}, resultEnded},
		{router.ApplyResult{Reset: true}, resultReset},
		{router.ApplyResult{}, resultApplied},
	}
	for _, tt := range tests {
		if got := applyResult(tt.r); got != tt.want {
			t.Errorf("applyResult(%+v) = %q, want %q", tt.r, got, tt.want)
		}
	}
}

func TestObserveCounts(t *testing.T) {
	m, err := newMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	m.observeCounts(router.Counts{
		Conversations: map[string]int64{"waiting": 3, "assigned": 2},
		Queues: []router.QueueDepth{
			{Lang: "en", Waiting: 2, Oldest: now.Add(-30 * time.Second)},
			{Lang: "en", TeamCompanyID: "test-co-1", Waiting: 1, Oldest: now.Add(-10 * time.Second)},
			{Lang: "en", TeamCompanyID: "test-co-2", Waiting: 4, Oldest: now.Add(-20 * time.Second)},
		},
		Operators: []router.OperatorCount{
			{Presence: opapi.PresenceOnline, N: 5, Capacity: 20},
			{Presence: opapi.PresenceOnline, InTeam: true, N: 2, Capacity: 8},
		},
		LiveSlots: 7, ActiveTeams: 2, UnsentOutbox: 3, OldestUnsent: now.Add(-time.Second),
	}, now)
	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"waiting state", testutil.ToFloat64(m.conversations.WithLabelValues("waiting")), 3},
		{"shared en", testutil.ToFloat64(m.queueWaiting.WithLabelValues("en", groupShared)), 2},
		{"team en summed", testutil.ToFloat64(m.queueWaiting.WithLabelValues("en", groupTeam)), 5},
		{"team en oldest", testutil.ToFloat64(m.queueOldest.WithLabelValues("en", groupTeam)), 20},
		{"pool online", testutil.ToFloat64(m.operators.WithLabelValues("online", "pool")), 5},
		{"team capacity", testutil.ToFloat64(m.capacity.WithLabelValues("online", groupTeam)), 8},
		{"live slots", testutil.ToFloat64(m.liveSlots), 7},
		{"outbox oldest", testutil.ToFloat64(m.outboxOldest), 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	// A queue that is gone does not keep its last value.
	m.observeCounts(router.Counts{Conversations: map[string]int64{}}, now)
	if n := testutil.CollectAndCount(m.queueWaiting); n != 0 {
		t.Errorf("queue series after an empty sample = %d, want 0", n)
	}
}

// TestCatchUpRetriesOffsets checks that a coordinator that is not ready
// yet, as on a broker that has just started, does not end the catch-up.
func TestCatchUpRetriesOffsets(t *testing.T) {
	r, failed := newTestRunner(newFakeTimer(), time.Minute)
	admin := &fakeAdmin{start: map[int32]int64{0: 0}, end: map[int32]int64{0: 0}, fails: 3}
	c := &consumer{admin: admin, topic: testTopic, group: "g", runner: r, logger: r.logger}
	if err := c.catchUp(t.Context()); err != nil {
		t.Fatalf("catchUp() = %v", err)
	}
	if (*failed)["catchup_offsets"] != 3 {
		t.Errorf("failures counted = %d, want 3", (*failed)["catchup_offsets"])
	}
}
