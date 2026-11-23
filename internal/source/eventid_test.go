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

func TestAccountEventID(t *testing.T) {
	tests := []struct {
		name string
		did  string
		seq  int64
		want string
	}{
		{name: "simple", did: testDID, seq: 42, want: "a:did:plc:testalice0000000000000000:42"},
		{name: "zero seq", did: testDID, seq: 0, want: "a:did:plc:testalice0000000000000000:0"},
		{name: "other author", did: testDID2, seq: 42, want: "a:did:plc:testbob00000000000000000:42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AccountEventID(tt.did, tt.seq); got != tt.want {
				t.Errorf("AccountEventID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSyncEventID(t *testing.T) {
	tests := []struct {
		name, did, rev, want string
	}{
		{name: "simple", did: testDID, rev: testRev, want: "s:did:plc:testalice0000000000000000:3testrev00001"},
		{name: "rev with colon is escaped", did: testDID, rev: "a:b", want: "s:did:plc:testalice0000000000000000:a%3Ab"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SyncEventID(tt.did, tt.rev); got != tt.want {
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
			name: "sync rev against account seq",
			a:    SyncEventID(testDID, "42"),
			b:    AccountEventID(testDID, 42),
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
		{name: "account", id: func(delivery) string { return AccountEventID(testDID, 42) }},
		{name: "sync", id: func(delivery) string { return SyncEventID(testDID, testRev) }},
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
