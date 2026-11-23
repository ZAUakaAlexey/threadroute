package record

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Synthetic test data. Nothing here is real user data.
const (
	testDID  = "did:plc:testalice0000000000000000"
	testRkey = "aaa111"
	testText = "synthetic reply one"
	testKind = "commit"
	// The facet field has characters that encoding/json escapes by default
	// (&, <, > and U+2028). They must be stored as they are.
	testValueFmt = `{
  "did": "did:plc:testalice0000000000000000",
  "kind": "commit",
  "commit": {"collection": "app.bsky.feed.post", "rkey": "aaa111", "rev": "rev%d",
    "record": {"text": "synthetic reply one", "n": %d,
      "facet": "https://example.com/?a=1&b=<2>` + "\u2028" + `"}}
}`
)

var testStart = time.Date(2026, 11, 23, 20, 0, 0, 0, time.UTC)

// fakeClock is a clock that only moves when the test moves it.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{t: t} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testEntry returns a synthetic entry. The value has newlines and spaces on
// purpose, to check that an entry still takes one line. Even entries have a
// witnessed time (a little before the record timestamp), odd ones have none.
func testEntry(i int) Entry {
	var witnessed time.Time
	if i%2 == 0 {
		witnessed = testStart.Add(-2*time.Second + time.Duration(i)*time.Millisecond + 7*time.Microsecond)
	}
	return Entry{
		Partition: int32(i % 6),
		Offset:    int64(i),
		Timestamp: testStart.Add(time.Duration(i) * 1500 * time.Microsecond),
		Key:       testDID,
		EventID:   fmt.Sprintf("c:%s:app.bsky.feed.post:%s:rev%d:create", testDID, testRkey, i),
		EventKind: testKind,
		Witnessed: witnessed,
		Value:     json.RawMessage(fmt.Sprintf(testValueFmt, i, i)),
	}
}

// openTestWriter opens a Writer in a new temporary directory. The background
// check is effectively off, tests call check directly.
func openTestWriter(t *testing.T, opts WriterOptions) (*Writer, *prometheus.Registry) {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	if opts.CheckInterval == 0 {
		opts.CheckInterval = time.Hour
	}
	reg := prometheus.NewRegistry()
	w, err := OpenWriter(opts, discardLogger(), reg)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, reg
}

func readAll(t *testing.T, dir string) []Entry {
	t.Helper()
	var out []Entry
	for e, err := range NewReader(dir).All() {
		if err != nil {
			t.Fatalf("Reader.All() error = %v", err)
		}
		out = append(out, e)
	}
	return out
}

// sameJSON reports whether a and b hold the same JSON value. Whitespace and
// string escapes (such as \u2028 for U+2028) do not matter.
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	decode := func(in []byte) any {
		dec := json.NewDecoder(bytes.NewReader(in))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("decode JSON: %v", err)
		}
		return v
	}
	return reflect.DeepEqual(decode(a), decode(b))
}

// files returns the names of the complete and temporary segments in dir.
func files(t *testing.T, dir string) (complete, temp []string) {
	t.Helper()
	infos, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments() error = %v", err)
	}
	for _, info := range infos {
		if info.temp {
			temp = append(temp, info.name)
		} else {
			complete = append(complete, info.name)
		}
	}
	return complete, temp
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Size()
}

func join(dir, name string) string { return filepath.Join(dir, name) }
