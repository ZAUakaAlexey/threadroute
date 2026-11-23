package record

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRoundTripOverRotatedSegments(t *testing.T) {
	clock := newFakeClock(testStart)
	w, _ := openTestWriter(t, WriterOptions{SegmentMaxBytes: 2_000, Now: clock.Now})

	const n = 50
	var want []Entry
	for i := range n {
		e := testEntry(i)
		want = append(want, e)
		if err := w.Append(e); err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
		clock.Advance(time.Second)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	complete, temp := files(t, w.opts.Dir)
	if len(complete) < 3 {
		t.Fatalf("got %d segments, want at least 3 rotated segments", len(complete))
	}
	if len(temp) != 0 {
		t.Fatalf("temporary files left after Close: %v", temp)
	}

	got := readAll(t, w.opts.Dir)
	if len(got) != n {
		t.Fatalf("read %d entries, want %d", len(got), n)
	}
	for i := range got {
		g, e := got[i], want[i]
		if g.Partition != e.Partition || g.Offset != e.Offset || g.Key != e.Key ||
			g.EventID != e.EventID || g.EventKind != e.EventKind {
			t.Errorf("entry %d = %+v, want %+v", i, g, e)
		}
		if !g.Timestamp.Equal(e.Timestamp) {
			t.Errorf("entry %d timestamp = %s, want %s", i, g.Timestamp, e.Timestamp)
		}
		if !g.Witnessed.Equal(e.Witnessed) || g.Witnessed.IsZero() != (i%2 == 1) {
			t.Errorf("entry %d witnessed = %s, want %s", i, g.Witnessed, e.Witnessed)
		}
		if !sameJSON(t, g.Value, e.Value) {
			t.Errorf("entry %d value = %s, want %s", i, g.Value, e.Value)
		}
	}
}

func TestEntryLineFormat(t *testing.T) {
	e := testEntry(7)
	line, err := e.marshalLine()
	if err != nil {
		t.Fatalf("marshalLine() error = %v", err)
	}
	// &, < and > stay as they are. encoding/json always writes U+2028 as
	// \u2028, which is the same JSON string.
	if bytes.Count(line, []byte{'\n'}) != 1 || line[len(line)-1] != '\n' {
		t.Fatalf("entry is not one line: %q", line)
	}
	want := `{"partition":1,"offset":7,"ts_us":1795464000010500,"key":"did:plc:testalice0000000000000000",` +
		`"event_id":"c:did:plc:testalice0000000000000000:app.bsky.feed.post:aaa111:rev7:create","event_kind":"commit",` +
		`"value":{"did":"did:plc:testalice0000000000000000","kind":"commit","commit":{"collection":"app.bsky.feed.post","rkey":"aaa111","rev":"rev7","record":{"text":"synthetic reply one","n":7,"facet":"https://example.com/?a=1&b=<2>\u2028"}}}}` + "\n"
	if string(line) != want {
		t.Errorf("line =\n%s\nwant\n%s", line, want)
	}

	// With the witnessed time header the field is present.
	line, err = testEntry(8).marshalLine()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(line, []byte(`"ts_us":1795464000012000,"witnessed_us":1795463998008007,"key":`)) {
		t.Errorf("line without the expected witnessed_us: %s", line)
	}
}

func TestParseWitnessed(t *testing.T) {
	tests := []struct {
		in     string
		wantUS int64
		wantOK bool
	}{
		{"1795463998008007", 1795463998008007, true},
		{"1", 1, true},
		{"", 0, false},
		{"0", 0, false},
		{"-5", 0, false},
		{"1.5", 0, false},
		{" 12", 0, false},
		{"synthetic", 0, false},
		{"99999999999999999999", 0, false},
	}
	for _, tt := range tests {
		got, ok := parseWitnessed([]byte(tt.in))
		if ok != tt.wantOK || (ok && got.UnixMicro() != tt.wantUS) || (!ok && !got.IsZero()) {
			t.Errorf("parseWitnessed(%q) = %v, %v; want %d, %v", tt.in, got, ok, tt.wantUS, tt.wantOK)
		}
	}
}

func TestAppendInvalidEntry(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Entry)
	}{
		{"empty value", func(e *Entry) { e.Value = nil }},
		{"value not JSON", func(e *Entry) { e.Value = []byte("synthetic reply one") }},
		{"key not UTF-8", func(e *Entry) { e.Key = "did:plc:\xff" }},
		{"witnessed before 1970", func(e *Entry) { e.Witnessed = time.Unix(-10, 0) }},
	}
	w, _ := openTestWriter(t, WriterOptions{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := testEntry(1)
			tt.edit(&e)
			err := w.Append(e)
			if !errors.Is(err, ErrInvalidEntry) {
				t.Fatalf("Append() error = %v, want ErrInvalidEntry", err)
			}
			if strings.Contains(err.Error(), testText) {
				t.Errorf("error leaks the value: %v", err)
			}
		})
	}
	// The writer stays usable.
	if err := w.Append(testEntry(2)); err != nil {
		t.Fatalf("Append() after invalid entry error = %v", err)
	}
}

func TestRotationBySize(t *testing.T) {
	// All entries the same length: no witnessed time, one-digit numbers.
	entry := func(i int) Entry {
		e := testEntry(i)
		e.Witnessed = time.Time{}
		return e
	}
	line, err := entry(0).marshalLine()
	if err != nil {
		t.Fatal(err)
	}
	// Room for three entries: the third one reaches the limit.
	maxBytes := int64(len(line))*3 - 1
	clock := newFakeClock(testStart)
	w, _ := openTestWriter(t, WriterOptions{SegmentMaxBytes: maxBytes, Now: clock.Now})
	for i := range 7 {
		if err := w.Append(entry(i)); err != nil {
			t.Fatal(err)
		}
	}
	complete, temp := files(t, w.opts.Dir)
	if len(complete) != 2 || len(temp) != 1 {
		t.Fatalf("got complete=%v temp=%v, want 2 complete and 1 temporary", complete, temp)
	}
	// The clock did not move, so the names differ only by sequence.
	if complete[0] >= complete[1] || !strings.HasPrefix(complete[0], "seg-20261123T200000.000000Z-") {
		t.Errorf("names do not sort: %v", complete)
	}
	// Temporary files are not read.
	if got := readAll(t, w.opts.Dir); len(got) != 6 {
		t.Errorf("read %d entries, want 6 from complete segments only", len(got))
	}
}

func TestRotationByAge(t *testing.T) {
	clock := newFakeClock(testStart)
	w, _ := openTestWriter(t, WriterOptions{SegmentMaxAge: time.Hour, Now: clock.Now})

	steps := []struct {
		advance      time.Duration
		wantComplete int
	}{
		{0, 0},
		{30 * time.Minute, 0},
		{29 * time.Minute, 0},
		{time.Minute, 1}, // the first segment is one hour old
		{10 * time.Minute, 1},
	}
	for i, s := range steps {
		clock.Advance(s.advance)
		if err := w.Append(testEntry(i)); err != nil {
			t.Fatal(err)
		}
		complete, _ := files(t, w.opts.Dir)
		if len(complete) != s.wantComplete {
			t.Fatalf("step %d: %d complete segments, want %d", i, len(complete), s.wantComplete)
		}
	}

	// With no new entries the background check closes the old segment.
	clock.Advance(time.Hour)
	w.check()
	complete, temp := files(t, w.opts.Dir)
	if len(complete) != 2 || len(temp) != 0 {
		t.Fatalf("after check: complete=%v temp=%v, want 2 complete and none temporary", complete, temp)
	}
	if got := readAll(t, w.opts.Dir); len(got) != len(steps) {
		t.Errorf("read %d entries, want %d", len(got), len(steps))
	}
}

func TestFlushMakesTempFileDurableButNotComplete(t *testing.T) {
	w, _ := openTestWriter(t, WriterOptions{})
	for i := range 3 {
		if err := w.Append(testEntry(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	complete, temp := files(t, w.opts.Dir)
	if len(complete) != 0 || len(temp) != 1 {
		t.Fatalf("complete=%v temp=%v, want only one temporary file", complete, temp)
	}
	// The flushed data decodes, even though the zstd frame has no end yet.
	f, err := os.Open(join(w.opts.Dir, temp[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec, err := zstd.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	buf := make([]byte, 64<<10)
	n, _ := dec.Read(buf)
	if got := bytes.Count(buf[:n], []byte{'\n'}); got != 3 {
		t.Errorf("flushed temporary file holds %d lines, want 3", got)
	}
	if got := readAll(t, w.opts.Dir); len(got) != 0 {
		t.Errorf("reader returned %d entries from a temporary file", len(got))
	}
}

func TestOpenRemovesLeftoverTempFiles(t *testing.T) {
	dir := t.TempDir()
	leftover := segmentName(testStart, 7) + tempSuffix
	if err := os.WriteFile(join(dir, leftover), []byte("half written"), filePerm); err != nil {
		t.Fatal(err)
	}
	other := "notes.txt"
	if err := os.WriteFile(join(dir, other), nil, filePerm); err != nil {
		t.Fatal(err)
	}

	clock := newFakeClock(testStart)
	w, _ := openTestWriter(t, WriterOptions{Dir: dir, Now: clock.Now})
	if _, err := os.Stat(join(dir, leftover)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("leftover temporary file still exists, stat error = %v", err)
	}
	if _, err := os.Stat(join(dir, other)); err != nil {
		t.Errorf("unrelated file was touched: %v", err)
	}
	if err := w.Append(testEntry(0)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	complete, _ := files(t, dir)
	if want := segmentName(testStart, 8); len(complete) != 1 || complete[0] != want {
		t.Errorf("complete = %v, want [%s] (sequence continues after the leftover)", complete, want)
	}
}

// writeSegments writes one segment per start time and closes the writer.
func writeSegments(t *testing.T, dir string, starts []time.Time) {
	t.Helper()
	clock := newFakeClock(starts[0])
	w, _ := openTestWriter(t, WriterOptions{Dir: dir, Now: clock.Now, SegmentMaxAge: time.Minute, Retention: 365 * 24 * time.Hour})
	for i, s := range starts {
		clock.Advance(s.Sub(clock.Now()))
		if err := w.Append(testEntry(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionByAge(t *testing.T) {
	dir := t.TempDir()
	var starts []time.Time
	for i := range 5 {
		starts = append(starts, testStart.Add(time.Duration(i)*time.Hour))
	}
	writeSegments(t, dir, starts)
	all, _ := files(t, dir)

	// 14 days and 2.5 hours later the first three segments are too old.
	clock := newFakeClock(testStart.Add(DefaultRetention + 150*time.Minute))
	w, _ := openTestWriter(t, WriterOptions{Dir: dir, Now: clock.Now})
	complete, _ := files(t, dir)
	if want := all[3:]; strings.Join(complete, ",") != strings.Join(want, ",") {
		t.Fatalf("after open: complete = %v, want %v", complete, want)
	}

	// Later, the periodic check deletes the rest but not the open segment.
	if err := w.Append(testEntry(99)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(3 * time.Hour)
	w.mu.Lock()
	w.applyRetentionLocked()
	w.mu.Unlock()
	complete, temp := files(t, dir)
	if len(complete) != 0 || len(temp) != 1 {
		t.Fatalf("after check: complete=%v temp=%v, want only the open segment", complete, temp)
	}
	if got := testutil.ToFloat64(w.metrics.segmentsDeleted.WithLabelValues(reasonAge)); got != 5 {
		t.Errorf("segments_deleted_total{reason=age} = %v, want 5", got)
	}
}

func TestRetentionByTotalSize(t *testing.T) {
	dir := t.TempDir()
	var starts []time.Time
	for i := range 5 {
		starts = append(starts, testStart.Add(time.Duration(i)*time.Hour))
	}
	writeSegments(t, dir, starts)
	all, _ := files(t, dir)
	keep := fileSize(t, join(dir, all[3])) + fileSize(t, join(dir, all[4]))

	clock := newFakeClock(testStart.Add(5 * time.Hour))
	w, _ := openTestWriter(t, WriterOptions{Dir: dir, Now: clock.Now, MaxTotalBytes: keep})
	complete, _ := files(t, dir)
	if want := all[3:]; strings.Join(complete, ",") != strings.Join(want, ",") {
		t.Fatalf("complete = %v, want the two newest %v", complete, want)
	}
	if got := testutil.ToFloat64(w.metrics.segmentsDeleted.WithLabelValues(reasonSize)); got != 3 {
		t.Errorf("segments_deleted_total{reason=size} = %v, want 3", got)
	}

	// The open segment counts toward the total but is never deleted.
	for i := range 20 {
		if err := w.Append(testEntry(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	w.check()
	complete, temp := files(t, dir)
	if len(temp) != 1 {
		t.Fatalf("open segment missing: temp=%v", temp)
	}
	if len(complete) >= 2 {
		t.Errorf("complete = %v, want the oldest deleted to make room for the open segment", complete)
	}
}

func TestCorruptSegment(t *testing.T) {
	tests := []struct {
		name     string
		content  func(t *testing.T) []byte
		wantLine string
	}{
		{
			name: "bad JSON on line 3",
			content: func(t *testing.T) []byte {
				var lines []byte
				for i := range 2 {
					l, err := testEntry(i).marshalLine()
					if err != nil {
						t.Fatal(err)
					}
					lines = append(lines, l...)
				}
				lines = append(lines, []byte(`{"partition":0,"value":synthetic reply one}`+"\n")...)
				return zstdBytes(t, lines)
			},
			wantLine: "line 3",
		},
		{
			name: "negative witnessed_us on line 1",
			content: func(t *testing.T) []byte {
				return zstdBytes(t, []byte(`{"partition":0,"offset":1,"ts_us":1,"witnessed_us":-4,"value":{}}`+"\n"))
			},
			wantLine: "line 1",
		},
		{
			name: "missing value on line 1",
			content: func(t *testing.T) []byte {
				return zstdBytes(t, []byte(`{"partition":0,"offset":1,"ts_us":1}`+"\n"))
			},
			wantLine: "line 1",
		},
		{
			name: "truncated zstd frame",
			content: func(t *testing.T) []byte {
				l, err := testEntry(0).marshalLine()
				if err != nil {
					t.Fatal(err)
				}
				b := zstdBytes(t, bytes.Repeat(l, 50))
				return b[:len(b)/2]
			},
			wantLine: "line ",
		},
		{
			name:     "not zstd",
			content:  func(*testing.T) []byte { return []byte("synthetic reply one\n") },
			wantLine: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			name := segmentName(testStart, 1)
			if err := os.WriteFile(join(dir, name), tt.content(t), filePerm); err != nil {
				t.Fatal(err)
			}
			var gotErr error
			for _, err := range NewReader(dir).All() {
				if err != nil {
					gotErr = err
				}
			}
			if gotErr == nil {
				t.Fatal("Reader.All() gave no error for a corrupt segment")
			}
			msg := gotErr.Error()
			if !strings.Contains(msg, name) || !strings.Contains(msg, tt.wantLine) {
				t.Errorf("error = %q, want file %s and %q", msg, name, tt.wantLine)
			}
			if strings.Contains(msg, testText) {
				t.Errorf("error leaks line content: %q", msg)
			}
		})
	}
}

func zstdBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	return enc.EncodeAll(b, nil)
}

func TestReaderStopsEarly(t *testing.T) {
	clock := newFakeClock(testStart)
	w, _ := openTestWriter(t, WriterOptions{SegmentMaxBytes: 1, Now: clock.Now})
	for i := range 5 {
		if err := w.Append(testEntry(i)); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	for _, err := range NewReader(w.opts.Dir).All() {
		if err != nil {
			t.Fatal(err)
		}
		n++
		if n == 2 {
			break
		}
	}
	if n != 2 {
		t.Errorf("read %d entries, want 2", n)
	}
}

func TestPermissions(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) string
	}{
		{"new nested directory", func(t *testing.T) string { return filepath.Join(t.TempDir(), "a", "b") }},
		{"existing open directory", func(t *testing.T) string {
			d := filepath.Join(t.TempDir(), "open")
			if err := os.Mkdir(d, 0o755); err != nil {
				t.Fatal(err)
			}
			return d
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := tt.setup(t)
			w, _ := openTestWriter(t, WriterOptions{Dir: dir, SegmentMaxBytes: 1})
			for i := range 3 {
				if err := w.Append(testEntry(i)); err != nil {
					t.Fatal(err)
				}
			}
			fi, err := os.Stat(dir)
			if err != nil {
				t.Fatal(err)
			}
			if got := fi.Mode().Perm(); got != 0o700 {
				t.Errorf("directory mode = %o, want 700", got)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) == 0 {
				t.Fatal("no files written")
			}
			for _, de := range entries {
				info, err := de.Info()
				if err != nil {
					t.Fatal(err)
				}
				if got := info.Mode().Perm(); got != 0o600 {
					t.Errorf("%s mode = %o, want 600", de.Name(), got)
				}
			}
		})
	}
}

func TestConcurrentAppendAndClose(t *testing.T) {
	w, _ := openTestWriter(t, WriterOptions{SegmentMaxBytes: 4_000})

	var (
		mu       sync.Mutex
		appended int
		wg       sync.WaitGroup
	)
	for g := range 4 {
		wg.Go(func() {
			for i := 0; ; i++ {
				err := w.Append(testEntry(g*1_000_000 + i))
				if errors.Is(err, ErrClosed) {
					return
				}
				if err != nil {
					t.Errorf("Append() error = %v", err)
					return
				}
				mu.Lock()
				appended++
				mu.Unlock()
				if i%10 == 0 {
					if err := w.Flush(); err != nil && !errors.Is(err, ErrClosed) {
						t.Errorf("Flush() error = %v", err)
					}
				}
			}
		})
	}
	time.Sleep(50 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
	if got := readAll(t, w.opts.Dir); len(got) != appended {
		t.Errorf("read %d entries, want %d appended before Close", len(got), appended)
	}
	if got := testutil.ToFloat64(w.metrics.entries); int(got) != appended {
		t.Errorf("entries_total = %v, want %d", got, appended)
	}
}

func TestMetricsRegistered(t *testing.T) {
	clock := newFakeClock(testStart)
	w, reg := openTestWriter(t, WriterOptions{SegmentMaxBytes: 1, Now: clock.Now})
	for i := range 3 {
		if err := w.Append(testEntry(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(w.metrics.segments); got != 3 {
		t.Errorf("segments_total = %v, want 3", got)
	}
	if got := testutil.ToFloat64(w.metrics.bytes.WithLabelValues(stageCompressed)); got <= 0 {
		t.Errorf("bytes_total{stage=compressed} = %v, want above 0", got)
	}
	names := []string{
		"threadroute_record_entries_total",
		"threadroute_record_bytes_total",
		"threadroute_record_segments_total",
		"threadroute_record_segments_deleted_total",
	}
	n, err := testutil.GatherAndCount(reg, names...)
	if err != nil {
		t.Fatal(err)
	}
	// entries 1, bytes 2, segments 1, deleted 2.
	if n != 6 {
		t.Errorf("gathered %d series, want 6", n)
	}
}

func TestAppendAfterClose(t *testing.T) {
	w, _ := openTestWriter(t, WriterOptions{})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(testEntry(0)); !errors.Is(err, ErrClosed) {
		t.Errorf("Append() error = %v, want ErrClosed", err)
	}
	if err := w.Flush(); !errors.Is(err, ErrClosed) {
		t.Errorf("Flush() error = %v, want ErrClosed", err)
	}
}

func BenchmarkAppend(b *testing.B) {
	value := []byte(`{"did":"did:plc:testalice0000000000000000","kind":"commit","commit":{"collection":"app.bsky.feed.post","rkey":"aaa111","record":{"text":"` +
		strings.Repeat("synthetic reply one ", 45) + `"}}}`)
	e := testEntry(0)
	e.Value = value
	w, err := OpenWriter(WriterOptions{Dir: b.TempDir(), CheckInterval: time.Hour}, discardLogger(), prometheus.NewRegistry())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(value)))
	n := 0
	for b.Loop() {
		e.Offset = int64(n)
		if err := w.Append(e); err != nil {
			b.Fatal(err)
		}
		n++
	}
	b.ReportMetric(float64(n)/b.Elapsed().Seconds(), "entries/s")
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
}

func TestOneWriterPerDirectory(t *testing.T) {
	dir := t.TempDir()
	first, _ := openTestWriter(t, WriterOptions{Dir: dir})
	if err := first.Append(testEntry(0)); err != nil {
		t.Fatal(err)
	}

	_, err := OpenWriter(WriterOptions{Dir: dir, CheckInterval: time.Hour}, discardLogger(), prometheus.NewRegistry())
	if !errors.Is(err, ErrDirLocked) {
		t.Fatalf("second OpenWriter() error = %v, want ErrDirLocked", err)
	}
	if !strings.Contains(err.Error(), "only one writer") {
		t.Errorf("error does not say why: %v", err)
	}
	// The second writer did not touch the open segment of the first.
	if _, temp := files(t, dir); len(temp) != 1 {
		t.Fatalf("open segment of the first writer is gone: temp=%v", temp)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, _ := openTestWriter(t, WriterOptions{Dir: dir})
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, dir); len(got) != 1 {
		t.Errorf("read %d entries, want 1", len(got))
	}
}

func TestCompletePositions(t *testing.T) {
	w, _ := openTestWriter(t, WriterOptions{})
	for i := range 8 {
		if err := w.Append(testEntry(i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.CompletePositions(); len(got) != 0 {
		t.Fatalf("positions before rotation = %v, want none", got)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := w.CompletePositions(); len(got) != 0 {
		t.Fatalf("positions after flush = %v, want none", got)
	}
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	want := map[int32]int64{0: 6, 1: 7, 2: 2, 3: 3, 4: 4, 5: 5}
	if got := w.CompletePositions(); !reflect.DeepEqual(got, want) {
		t.Fatalf("positions after rotation = %v, want %v", got, want)
	}
	if err := w.Append(testEntry(12)); err != nil {
		t.Fatal(err)
	}
	if got := w.CompletePositions(); !reflect.DeepEqual(got, want) {
		t.Errorf("open segment changed positions: %v", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	want[0] = 12
	if got := w.CompletePositions(); !reflect.DeepEqual(got, want) {
		t.Errorf("positions after Close = %v, want %v", got, want)
	}
}
