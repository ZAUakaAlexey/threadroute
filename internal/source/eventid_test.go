package source

import (
	"testing"
	"time"
)

const (
	testDID  = "did:plc:testalice0000000000000000"
	testDID2 = "did:plc:testbob00000000000000000"
	testPost = "app.bsky.feed.post"
	testRev  = "3testrev00001"
)

func TestCommitEventID(t *testing.T) {
	tests := []struct {
		name                         string
		did, coll, rkey, rev, opName string
		want                         string
	}{
		{
			name: "create",
			did:  testDID, coll: testPost, rkey: "aaa111", rev: testRev, opName: "create",
			want: "c:did:plc:testalice0000000000000000:app.bsky.feed.post:aaa111:3testrev00001:create",
		},
		{
			name: "delete",
			did:  testDID, coll: testPost, rkey: "aaa111", rev: testRev, opName: "delete",
			want: "c:did:plc:testalice0000000000000000:app.bsky.feed.post:aaa111:3testrev00001:delete",
		},
		{
			name: "record key with colon is escaped",
			did:  testDID, coll: testPost, rkey: "aaa:111", rev: testRev, opName: "create",
			want: "c:did:plc:testalice0000000000000000:app.bsky.feed.post:aaa%3A111:3testrev00001:create",
		},
		{
			name: "percent is escaped",
			did:  testDID, coll: testPost, rkey: "aaa%3A111", rev: testRev, opName: "create",
			want: "c:did:plc:testalice0000000000000000:app.bsky.feed.post:aaa%253A111:3testrev00001:create",
		},
		{
			name: "did with extra segments is kept as it is",
			did:  "did:web:example.test:user:one", coll: testPost, rkey: "aaa111", rev: testRev, opName: "update",
			want: "c:did:web:example.test:user:one:app.bsky.feed.post:aaa111:3testrev00001:update",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CommitEventID(tt.did, tt.coll, tt.rkey, tt.rev, tt.opName)
			if got != tt.want {
				t.Errorf("CommitEventID() = %q, want %q", got, tt.want)
			}
		})
	}
}

const (
	testTime  = "2026-01-02T03:04:05.123Z"
	testTime2 = "2026-01-02T03:04:06.000Z"
)

func TestAccountEventID(t *testing.T) {
	tests := []struct {
		name         string
		did          string
		active       bool
		status, time string
		want         string
	}{
		{name: "active", did: testDID, active: true, time: testTime, want: "a:did:plc:testalice0000000000000000:1::2026-01-02T03%3A04%3A05.123Z"},
		{name: "deleted", did: testDID, status: "deleted", time: testTime, want: "a:did:plc:testalice0000000000000000:0:deleted:2026-01-02T03%3A04%3A05.123Z"},
		{name: "other author", did: testDID2, status: "takendown", time: testTime, want: "a:did:plc:testbob00000000000000000:0:takendown:2026-01-02T03%3A04%3A05.123Z"},
		{name: "empty time", did: testDID, active: true, want: "a:did:plc:testalice0000000000000000:1::"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AccountEventID(tt.did, tt.active, tt.status, tt.time); got != tt.want {
				t.Errorf("AccountEventID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSyncEventID(t *testing.T) {
	witnessed := time.UnixMicro(1_700_000_000_123_456)
	tests := []struct {
		name, did, rev, time string
		witnessed            time.Time
		want                 string
	}{
		{name: "simple", did: testDID, rev: testRev, time: testTime, witnessed: witnessed, want: "s:did:plc:testalice0000000000000000:3testrev00001:2026-01-02T03%3A04%3A05.123Z"},
		{name: "rev with colon is escaped", did: testDID, rev: "a:b", time: testTime, want: "s:did:plc:testalice0000000000000000:a%3Ab:2026-01-02T03%3A04%3A05.123Z"},
		{name: "rev without time", did: testDID, rev: testRev, witnessed: witnessed, want: "s:did:plc:testalice0000000000000000:3testrev00001:"},
		{name: "time without rev", did: testDID, time: testTime, witnessed: witnessed, want: "s:did:plc:testalice0000000000000000::2026-01-02T03%3A04%3A05.123Z"},
		{name: "fallback to witnessed time", did: testDID, witnessed: witnessed, want: "s:did:plc:testalice0000000000000000:w1700000000123456"},
		{name: "fallback with zero witnessed time", did: testDID, want: "s:did:plc:testalice0000000000000000:w0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SyncEventID(tt.did, tt.rev, tt.time, tt.witnessed); got != tt.want {
				t.Errorf("SyncEventID() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestEventIDNoCollision checks pairs of different events whose fields would
// give the same ID without escaping.
func TestEventIDNoCollision(t *testing.T) {
	tests := []struct {
		name string
		a, b string
	}{
		{
			name: "colon moved between did and collection",
			a:    CommitEventID("did:web:x", "y:app.bsky.feed.post", "aaa111", testRev, "create"),
			b:    CommitEventID("did:web:x:y", "app.bsky.feed.post", "aaa111", testRev, "create"),
		},
		{
			name: "colon moved between rkey and rev",
			a:    CommitEventID(testDID, testPost, "aaa:111", "222", "create"),
			b:    CommitEventID(testDID, testPost, "aaa", "111:222", "create"),
		},
		{
			name: "escaped text in input against a real colon",
			a:    CommitEventID(testDID, testPost, "aaa%3A111", testRev, "create"),
			b:    CommitEventID(testDID, testPost, "aaa:111", testRev, "create"),
		},
		{
			name: "account with different status",
			a:    AccountEventID(testDID, false, "deleted", testTime),
			b:    AccountEventID(testDID, false, "takendown", testTime),
		},
		{
			name: "account with different upstream time",
			a:    AccountEventID(testDID, false, "deleted", testTime),
			b:    AccountEventID(testDID, false, "deleted", testTime2),
		},
		{
			name: "account active against not active",
			a:    AccountEventID(testDID, true, "", testTime),
			b:    AccountEventID(testDID, false, "", testTime),
		},
		{
			name: "colon moved between sync rev and time",
			a:    SyncEventID(testDID, "a:b", "c", time.Time{}),
			b:    SyncEventID(testDID, "a", "b:c", time.Time{}),
		},
		{
			name: "sync fallback against a rev that looks like it",
			a:    SyncEventID(testDID, "", "", time.UnixMicro(1_700_000_000_000_000)),
			b:    SyncEventID(testDID, "w1700000000000000", "", time.Time{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.a == tt.b {
				t.Errorf("different events got the same ID %q", tt.a)
			}
		})
	}
}

// TestEventIDIgnoresHostAndSeq checks that the same event seen on two hosts,
// with different seq values and witnessed times, gets the same ID.
func TestEventIDIgnoresHostAndSeq(t *testing.T) {
	type delivery struct {
		host      string
		seq       uint64
		witnessed time.Time
	}
	east := delivery{host: "jetstream-east.example.test", seq: 1001, witnessed: time.UnixMicro(1_700_000_000_000_000)}
	west := delivery{host: "jetstream-west.example.test", seq: 77, witnessed: time.UnixMicro(1_700_000_000_250_000)}

	tests := []struct {
		name string
		id   func(d delivery) string
	}{
		{name: "commit", id: func(delivery) string { return CommitEventID(testDID, testPost, "aaa111", testRev, "create") }},
		// The account seq differs between hosts and is not an argument.
		{name: "account", id: func(delivery) string { return AccountEventID(testDID, false, "deleted", testTime) }},
		{name: "sync", id: func(d delivery) string { return SyncEventID(testDID, testRev, testTime, d.witnessed) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if a, b := tt.id(east), tt.id(west); a != b {
				t.Errorf("ID depends on delivery: %q != %q", a, b)
			}
		})
	}
}

func TestCursorIsZero(t *testing.T) {
	tests := []struct {
		name string
		c    Cursor
		want bool
	}{
		{name: "empty", c: Cursor{}, want: true},
		{name: "host and seq without time", c: Cursor{Host: "jetstream-east.example.test", Seq: 5}, want: true},
		{name: "with time", c: Cursor{Witnessed: time.UnixMicro(1_700_000_000_000_000)}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.IsZero(); got != tt.want {
				t.Errorf("IsZero() = %v, want %v", got, tt.want)
			}
		})
	}
}
