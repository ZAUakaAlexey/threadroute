package bluesky

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

// fakeJetstream is a minimal local Jetstream v2 server over plain HTTP. It
// accepts one WebSocket subscription, records the query, and sends the given
// text frames. It speaks only the part of RFC 6455 the test needs: the
// handshake and unmasked server text frames.
type fakeJetstream struct {
	frames []string

	mu    sync.Mutex
	query url.Values
}

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func (f *fakeJetstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/xrpc/network.bsky.jetstream.subscribeEvents" {
		http.NotFound(w, r)
		return
	}
	f.mu.Lock()
	f.query = r.URL.Query()
	f.mu.Unlock()

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "no hijack", http.StatusInternalServerError)
		return
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()

	sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + wsGUID))
	fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\n"+
		"Upgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Accept: %s\r\nSec-WebSocket-Protocol: xrpc.v1.json\r\n\r\n",
		base64.StdEncoding.EncodeToString(sum[:]))
	for _, fr := range f.frames {
		writeTextFrame(rw.Writer, []byte(fr))
	}
	if err := rw.Flush(); err != nil {
		return
	}
	// Wait for anything from the client (its close frame) or an error,
	// then drop the TCP connection so the client does not wait for a
	// close reply.
	_, _ = rw.Reader.ReadByte()
}

func (f *fakeJetstream) lastQuery() url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.query
}

func writeTextFrame(w *bufio.Writer, payload []byte) {
	_ = w.WriteByte(0x81) // FIN and text opcode
	switch n := len(payload); {
	case n < 126:
		_ = w.WriteByte(byte(n))
	case n < 1<<16:
		_ = w.WriteByte(126)
		_ = binary.Write(w, binary.BigEndian, uint16(n))
	default:
		_ = w.WriteByte(127)
		_ = binary.Write(w, binary.BigEndian, uint64(n))
	}
	_, _ = w.Write(payload)
}

// frame wraps a payload object in the xrpc.v1.json message envelope.
func frame(t *testing.T, payload map[string]any) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"$type": "message", "payload": payload})
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	return string(b)
}

func TestEndToEndWithLocalServer(t *testing.T) {
	ts := func(d time.Duration) string { return testBase.Add(d).Format("2006-01-02T15:04:05.000000Z") }
	frames := []string{
		frame(t, map[string]any{
			"$type": "network.bsky.jetstream.subscribeEvents#commit",
			"did":   testDID, "seq": 101, "time": ts(time.Second), "witnessedAt": ts(time.Second),
			"collection": testCollection, "rkey": testRkey, "rev": testRev, "operation": "create",
			"record": map[string]any{"$type": testCollection, "text": testText, "createdAt": "2026-01-02T03:04:05.000Z"},
		}),
		frame(t, map[string]any{
			"$type": "network.bsky.jetstream.subscribeEvents#account",
			"did":   testDID2, "seq": 102, "time": ts(2 * time.Second), "witnessedAt": ts(2 * time.Second),
			"account": map[string]any{"did": testDID2, "active": false, "status": "deleted", "seq": 777, "time": ts(0)},
		}),
		frame(t, map[string]any{
			"$type": "network.bsky.jetstream.subscribeEvents#sync",
			"did":   testDID, "seq": 103, "time": ts(3 * time.Second), "witnessedAt": ts(3 * time.Second),
			"sync": map[string]any{"did": testDID, "rev": testRev, "seq": 888, "time": ts(0)},
		}),
	}
	tests := []struct {
		name          string
		primaryIsDown bool
	}{
		{name: "primary host serves"},
		{name: "primary host is down, failover host serves", primaryIsDown: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runEndToEnd(t, frames, tt.primaryIsDown)
		})
	}
}

func runEndToEnd(t *testing.T, frames []string, primaryIsDown bool) {
	fake := &fakeJetstream{frames: frames}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	// A closed local port stands for a host that cannot be reached.
	live := strings.TrimPrefix(srv.URL, "http://")
	down := unusedLocalAddr(t)
	primary, failover := live, down
	if primaryIsDown {
		primary, failover = down, live
	}
	c, err := New(Config{
		PrimaryHost:  primary,
		FailoverHost: failover,
		Collection:   testCollection,
		Logger:       testLogger(),
		Registerer:   prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// The fake server has no zstd dictionary endpoint.
	c.sdkExtra = []jetstream.Option{jetstream.WithZstdCompression(false)}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var events []source.RawEvent
	var cursor source.Cursor
	sink := func(_ context.Context, b source.Batch) error {
		events = append(events, b.Events...)
		cursor = b.Cursor
		if len(events) >= 3 {
			cancel()
		}
		return nil
	}
	from := source.Cursor{Witnessed: testBase, Seq: 7}
	if err := c.Run(ctx, from, sink); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("Run stopped by %v, want the sink cancel", ctx.Err())
	}
	reconnects := testutil.ToFloat64(c.metrics.errors.WithLabelValues(errTypeReconnect))
	if primaryIsDown && reconnects < 1 {
		t.Errorf("reconnect errors = %v, want at least 1 for the failed primary", reconnects)
	}
	if !primaryIsDown && reconnects != 0 {
		t.Errorf("reconnect errors = %v, want 0", reconnects)
	}

	q := fake.lastQuery()
	if got := strings.Join(q["kinds"], ","); got != "commit,account,sync" {
		t.Errorf("kinds = %q", got)
	}
	if got := strings.Join(q["collections"], ","); got != testCollection {
		t.Errorf("collections = %q", got)
	}
	wantCursor := fmt.Sprint(testBase.Add(-5 * time.Second).UnixMicro())
	if got := q.Get("cursor"); got != wantCursor {
		t.Errorf("cursor = %q, want %q (from minus the 5 s rewind)", got, wantCursor)
	}
	if q.Has("zstdDictionary") {
		t.Errorf("zstdDictionary sent although compression is off")
	}

	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	wantIDs := []string{
		"c:did:plc:testalice0000000000000000:app.bsky.feed.post:aaa111:3testrev00001:create",
		"a:did:plc:testbob00000000000000000:0:deleted:2026-01-02T03%3A04%3A05.123456Z",
		"s:did:plc:testalice0000000000000000:3testrev00001:2026-01-02T03%3A04%3A05.123456Z",
	}
	for i, ev := range events {
		if ev.ID != wantIDs[i] {
			t.Errorf("event %d ID = %q, want %q", i, ev.ID, wantIDs[i])
		}
	}
	checkPayload(t, events[0].Payload, "commit", true)
	if !cursor.Witnessed.Equal(testBase.Add(3*time.Second)) || cursor.Seq != 103 {
		t.Errorf("last cursor = %+v, want witnessed %v seq 103", cursor, testBase.Add(3*time.Second))
	}
}

// unusedLocalAddr returns a loopback address with nothing listening on it.
func unusedLocalAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}
