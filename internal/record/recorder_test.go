package record

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
)

const testGroup = "record-test-group"

func newTestCluster(t *testing.T) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(bus.PartitionsRaw, bus.TopicRaw),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// produce writes synthetic records numbered from..to-1 to the raw topic.
func produce(t *testing.T, c *kfake.Cluster, from, to int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var recs []*kgo.Record
	for i := from; i < to; i++ {
		e := testEntry(i)
		headers := []kgo.RecordHeader{
			{Key: bus.HeaderEventID, Value: []byte(e.EventID)},
			{Key: bus.HeaderEventKind, Value: []byte(e.EventKind)},
			{Key: "unrelated", Value: []byte("ignored")},
		}
		if !e.Witnessed.IsZero() {
			headers = append(headers, kgo.RecordHeader{
				Key: bus.HeaderEventWitnessed, Value: []byte(strconv.FormatInt(e.Witnessed.UnixMicro(), 10)),
			})
		}
		recs = append(recs, &kgo.Record{
			Topic:     bus.TopicRaw,
			Partition: e.Partition,
			Key:       []byte(e.Key),
			Value:     e.Value,
			Timestamp: e.Timestamp,
			Headers:   headers,
		})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// startRecorder runs a Recorder in the background. The returned stop
// function cancels it and returns the result of Run.
func startRecorder(t *testing.T, c *kfake.Cluster, w EntryWriter, reg prometheus.Registerer) func() error {
	t.Helper()
	r, err := NewRecorder(c.ListenAddrs(), testGroup, StartBeginning, w, discardLogger(), reg,
		kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatalf("NewRecorder() error = %v", err)
	}
	r.commitInterval = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var once sync.Once
	var runErr error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case runErr = <-done:
			case <-time.After(30 * time.Second):
				runErr = fmt.Errorf("Run did not return after cancel")
			}
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRecorderWritesEntriesAndResumesFromCommit(t *testing.T) {
	c := newTestCluster(t)
	produce(t, c, 0, 12)

	// First run: everything from the beginning.
	dir1 := t.TempDir()
	w1, _ := openTestWriter(t, WriterOptions{Dir: dir1})
	stop1 := startRecorder(t, c, w1, prometheus.NewRegistry())
	waitFor(t, "12 entries", func() bool { return entriesCount(w1) == 12 })
	if err := stop1(); err != nil {
		t.Fatalf("Run() error = %v, want nil after cancel", err)
	}

	got := readAll(t, dir1)
	if len(got) != 12 {
		t.Fatalf("read %d entries after shutdown, want 12", len(got))
	}
	byID := map[string]Entry{}
	for _, e := range got {
		byID[e.EventID] = e
	}
	for i := range 12 {
		want := testEntry(i)
		g, ok := byID[want.EventID]
		if !ok {
			t.Errorf("entry %d missing", i)
			continue
		}
		if !g.Witnessed.Equal(want.Witnessed) {
			t.Errorf("entry %d witnessed = %s, want %s", i, g.Witnessed, want.Witnessed)
		}
		if g.Partition != want.Partition || g.Key != want.Key || g.EventKind != want.EventKind {
			t.Errorf("entry %d = %+v, want %+v", i, g, want)
		}
		if !g.Timestamp.Equal(want.Timestamp.Truncate(time.Millisecond)) {
			t.Errorf("entry %d timestamp = %s, want %s (Kafka keeps milliseconds)", i, g.Timestamp, want.Timestamp)
		}
		if !sameJSON(t, g.Value, want.Value) {
			t.Errorf("entry %d value differs", i)
		}
	}

	// Second run with the same group: only new records are read.
	produce(t, c, 100, 105)
	dir2 := t.TempDir()
	w2, _ := openTestWriter(t, WriterOptions{Dir: dir2})
	stop2 := startRecorder(t, c, w2, prometheus.NewRegistry())
	waitFor(t, "5 entries", func() bool { return entriesCount(w2) >= 5 })
	if err := stop2(); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	got2 := readAll(t, dir2)
	if len(got2) != 5 {
		t.Fatalf("second run read %d entries, want 5", len(got2))
	}
	seen := map[string]bool{}
	for _, e := range got2 {
		seen[e.EventID] = true
	}
	for i := 100; i < 105; i++ {
		if !seen[testEntry(i).EventID] {
			t.Errorf("second run missed new record %d", i)
		}
	}
	for i := range 12 {
		if seen[testEntry(i).EventID] {
			t.Errorf("second run re-read committed record %d", i)
		}
	}
}

func entriesCount(w *Writer) int {
	return int(testutil.ToFloat64(w.metrics.entries))
}

// commitLog records the offset commits of the test group, seen by the fake
// cluster.
type commitLog struct {
	mu      sync.Mutex
	commits []map[int32]int64
	onAdd   func()
}

func watchCommits(c *kfake.Cluster) *commitLog {
	l := &commitLog{}
	c.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		r := req.(*kmsg.OffsetCommitRequest)
		if r.Group != testGroup {
			return nil, nil, false
		}
		m := map[int32]int64{}
		for _, tp := range r.Topics {
			for _, p := range tp.Partitions {
				m[p.Partition] = p.Offset
			}
		}
		l.mu.Lock()
		l.commits = append(l.commits, m)
		onAdd := l.onAdd
		l.mu.Unlock()
		if onAdd != nil {
			onAdd()
		}
		return nil, nil, false // observe only
	})
	return l
}

// latest returns the last committed offset per partition.
func (l *commitLog) latest() map[int32]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[int32]int64{}
	for _, c := range l.commits {
		for p, o := range c {
			out[p] = o
		}
	}
	return out
}

func (l *commitLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.commits)
}

// nextOffsets returns, per partition, the offset after the highest entry.
func nextOffsets(entries []Entry) map[int32]int64 {
	out := map[int32]int64{}
	for _, e := range entries {
		if e.Offset+1 > out[e.Partition] {
			out[e.Partition] = e.Offset + 1
		}
	}
	return out
}

func TestRecorderCommitsOnlyCompleteSegments(t *testing.T) {
	c := newTestCluster(t)
	commits := watchCommits(c)
	clock := newFakeClock(testStart)
	w, _ := openTestWriter(t, WriterOptions{
		Now:           clock.Now,
		SegmentMaxAge: time.Hour,
		CheckInterval: 20 * time.Millisecond,
	})
	produce(t, c, 0, 20)
	stop := startRecorder(t, c, w, prometheus.NewRegistry())
	waitFor(t, "20 entries", func() bool { return entriesCount(w) == 20 })

	// Many polls and commit intervals pass, but the segment is open.
	time.Sleep(300 * time.Millisecond)
	if n := commits.count(); n != 0 {
		t.Fatalf("%d commits before any segment was complete, want 0", n)
	}

	// The age check of the writer completes the segment, and the recorder
	// commits all of it.
	clock.Advance(2 * time.Hour)
	first := readAllWhenComplete(t, w.opts.Dir, 20)
	want := nextOffsets(first)
	waitFor(t, "commit of the first segment", func() bool { return reflect.DeepEqual(commits.latest(), want) })

	// Records in the new open segment are not committed.
	produce(t, c, 20, 30)
	waitFor(t, "30 entries", func() bool { return entriesCount(w) == 30 })
	time.Sleep(300 * time.Millisecond)
	if got := commits.latest(); !reflect.DeepEqual(got, want) {
		t.Fatalf("commits moved to %v while the segment is open, want %v", got, want)
	}

	// Shutdown completes the segment and commits everything.
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	all := readAll(t, w.opts.Dir)
	if len(all) != 30 {
		t.Fatalf("read %d entries, want 30", len(all))
	}
	if got, want := commits.latest(), nextOffsets(all); !reflect.DeepEqual(got, want) {
		t.Errorf("final commits = %v, want %v", got, want)
	}
}

// readAllWhenComplete waits until the complete segments hold n entries.
func readAllWhenComplete(t *testing.T, dir string, n int) []Entry {
	t.Helper()
	var got []Entry
	waitFor(t, "complete segments", func() bool {
		got = got[:0]
		for e, err := range NewReader(dir).All() {
			if err != nil {
				return false
			}
			got = append(got, e)
		}
		return len(got) == n
	})
	return got
}

// event is one step seen by the order test.
type event struct {
	kind      string // append, rotate, close, commit
	partition int32
	offset    int64
}

// orderWriter is a fake EntryWriter that logs its calls. Entries count as
// complete after Rotate or Close.
type orderWriter struct {
	mu       sync.Mutex
	events   []event
	open     map[int32]int64
	complete map[int32]int64
	closed   bool
}

func (o *orderWriter) add(e event) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
}

func (o *orderWriter) Append(e Entry) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.open == nil {
		o.open = map[int32]int64{}
	}
	o.open[e.Partition] = e.Offset
	o.events = append(o.events, event{kind: "append", partition: e.Partition, offset: e.Offset})
	return nil
}

func (o *orderWriter) completeLocked(kind string) {
	if o.complete == nil {
		o.complete = map[int32]int64{}
	}
	for p, off := range o.open {
		o.complete[p] = off
	}
	o.open = nil
	o.events = append(o.events, event{kind: kind})
}

func (o *orderWriter) Rotate() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.completeLocked("rotate")
	return nil
}

func (o *orderWriter) CompletePositions() map[int32]int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return maps.Clone(o.complete)
}

func (o *orderWriter) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	o.completeLocked("close")
	return nil
}

func (o *orderWriter) snapshot() ([]event, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]event(nil), o.events...), o.closed
}

func TestRecorderShutdownClosesThenCommits(t *testing.T) {
	c := newTestCluster(t)
	ow := &orderWriter{}
	commits := watchCommits(c)
	commits.onAdd = func() { ow.add(event{kind: "commit"}) }

	produce(t, c, 0, 30)
	stop := startRecorder(t, c, ow, prometheus.NewRegistry())
	waitFor(t, "30 appends", func() bool {
		evs, _ := ow.snapshot()
		n := 0
		for _, e := range evs {
			if e.kind == "append" {
				n++
			}
		}
		return n == 30
	})
	time.Sleep(200 * time.Millisecond)
	if n := commits.count(); n != 0 {
		t.Fatalf("%d commits before shutdown without a rotation, want 0", n)
	}
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	evs, closed := ow.snapshot()
	if !closed {
		t.Fatal("Run did not close the writer")
	}
	closeAt, commitAt := -1, -1
	for i, e := range evs {
		switch e.kind {
		case "close":
			closeAt = i
		case "commit":
			if commitAt < 0 {
				commitAt = i
			}
		}
	}
	if commitAt < 0 {
		t.Fatal("no commit at shutdown")
	}
	if closeAt < 0 || commitAt < closeAt {
		t.Errorf("commit at step %d, close at step %d: want close first", commitAt, closeAt)
	}
	var appended []Entry
	for _, e := range evs {
		if e.kind == "append" {
			appended = append(appended, Entry{Partition: e.partition, Offset: e.offset})
		}
	}
	if got, want := commits.latest(), nextOffsets(appended); !reflect.DeepEqual(got, want) {
		t.Errorf("commits = %v, want %v", got, want)
	}
}

// crashWriter stops like a crashed process: the open segment stays under
// its temporary name and the directory lock is released.
type crashWriter struct{ *Writer }

func (c crashWriter) Close() error { return c.abandon() }

// abandon is a test helper that drops the Writer without completing the
// open segment.
func (w *Writer) abandon() error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.done)
	}
	if w.cur != nil {
		_ = w.enc.Flush()
		_ = w.cur.file.Close()
		w.cur = nil
	}
	w.mu.Unlock()
	w.wg.Wait()
	return unlockDir(w.lock)
}

func TestRecorderCrashLosesNothing(t *testing.T) {
	c := newTestCluster(t)
	dir := t.TempDir()
	line, err := testEntry(0).marshalLine()
	if err != nil {
		t.Fatal(err)
	}
	// About four entries per segment.
	opts := WriterOptions{Dir: dir, SegmentMaxBytes: int64(len(line)) * 4}

	// First run crashes after 22 records: some segments are complete, the
	// last one is a temporary file.
	produce(t, c, 0, 22)
	w1, _ := openTestWriter(t, opts)
	stop1 := startRecorder(t, c, crashWriter{w1}, prometheus.NewRegistry())
	waitFor(t, "22 entries", func() bool { return entriesCount(w1) == 22 })
	if err := stop1(); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	complete1 := readAll(t, dir)
	if _, temp := files(t, dir); len(temp) != 1 {
		t.Fatalf("temporary files after the crash = %v, want 1", temp)
	}
	if len(complete1) == 0 || len(complete1) == 22 {
		t.Fatalf("complete entries after the crash = %d, want some but not all", len(complete1))
	}

	// Second run on the same directory and group.
	produce(t, c, 22, 30)
	w2, _ := openTestWriter(t, opts)
	if _, temp := files(t, dir); len(temp) != 0 {
		t.Fatalf("leftover temporary files not removed on open: %v", temp)
	}
	again := 30 - len(complete1)
	stop2 := startRecorder(t, c, w2, prometheus.NewRegistry())
	waitFor(t, "the uncommitted and new entries", func() bool { return entriesCount(w2) >= again })
	if err := stop2(); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	if n := entriesCount(w2); n != again {
		t.Errorf("second run appended %d entries, want %d", n, again)
	}

	seen := map[string]int{}
	for _, e := range readAll(t, dir) {
		seen[e.EventID]++
	}
	for i := range 30 {
		if n := seen[testEntry(i).EventID]; n != 1 {
			t.Errorf("record %d is %d times in complete segments, want once", i, n)
		}
	}
	if len(seen) != 30 {
		t.Errorf("complete segments hold %d distinct records, want 30", len(seen))
	}
}

func TestRecorderStartAtEnd(t *testing.T) {
	c := newTestCluster(t)
	produce(t, c, 0, 6)

	w, _ := openTestWriter(t, WriterOptions{})
	r, err := NewRecorder(c.ListenAddrs(), testGroup, StartEnd, w, discardLogger(), prometheus.NewRegistry(),
		kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// The recorder may not have its partitions yet, so keep producing new
	// records until one arrives.
	deadline := time.Now().Add(20 * time.Second)
	for next := 200; entriesCount(w) == 0; next++ {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for a new record")
		}
		produce(t, c, next, next+1)
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for _, e := range readAll(t, w.opts.Dir) {
		for i := range 6 {
			if e.EventID == testEntry(i).EventID {
				t.Errorf("start at end recorded old record %d", i)
			}
		}
	}
}

func TestRecorderCancelWithoutRecords(t *testing.T) {
	c := newTestCluster(t)
	w, _ := openTestWriter(t, WriterOptions{})
	stop := startRecorder(t, c, w, prometheus.NewRegistry())
	time.Sleep(200 * time.Millisecond)
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if err := w.Append(testEntry(0)); err == nil {
		t.Error("writer still open after Run returned")
	}
}

func TestNewRecorderValidates(t *testing.T) {
	tests := []struct {
		name    string
		brokers []string
		group   string
		start   StartPosition
	}{
		{"no brokers", nil, testGroup, StartEnd},
		{"no group", []string{"localhost:9092"}, "", StartEnd},
		{"bad start", []string{"localhost:9092"}, testGroup, "middle"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRecorder(tt.brokers, tt.group, tt.start, &orderWriter{}, discardLogger(), prometheus.NewRegistry())
			if err == nil {
				t.Fatal("NewRecorder() error = nil, want an error")
			}
		})
	}
}

func TestRecorderSharesErrorsMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	w, err := OpenWriter(WriterOptions{Dir: t.TempDir(), CheckInterval: time.Hour}, discardLogger(), reg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	r, err := NewRecorder([]string{"localhost:9092"}, testGroup, StartEnd, w, discardLogger(), reg)
	if err != nil {
		t.Fatalf("NewRecorder() on a shared registry error = %v", err)
	}
	r.client.Close()
	if r.errors != w.metrics.errors {
		t.Error("Recorder and Writer use different errors_total collectors")
	}
}

func TestRecorderInvalidWitnessedHeader(t *testing.T) {
	c := newTestCluster(t)
	bad := []string{"", "0", "-5", "synthetic"}
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	var recs []*kgo.Record
	for i, v := range bad {
		e := testEntry(i)
		recs = append(recs, &kgo.Record{
			Topic: bus.TopicRaw, Partition: 0, Key: []byte(e.Key), Value: e.Value,
			Headers: []kgo.RecordHeader{
				{Key: bus.HeaderEventID, Value: []byte(e.EventID)},
				{Key: bus.HeaderEventWitnessed, Value: []byte(v)},
			},
		})
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cl.Close()

	// The recorder shares the registry, and so errors_total, with the writer.
	w, reg := openTestWriter(t, WriterOptions{})
	stop := startRecorder(t, c, w, reg)
	waitFor(t, "all entries", func() bool { return entriesCount(w) == len(bad) })
	if err := stop(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	got := readAll(t, w.opts.Dir)
	if len(got) != len(bad) {
		t.Fatalf("read %d entries, want %d (invalid header must not drop the record)", len(got), len(bad))
	}
	for _, e := range got {
		if !e.Witnessed.IsZero() {
			t.Errorf("entry %d witnessed = %s, want absent", e.Offset, e.Witnessed)
		}
	}
	if n := testutil.ToFloat64(w.metrics.errors.WithLabelValues(opHeader)); int(n) != len(bad) {
		t.Errorf("errors_total{op=invalid_header} = %v, want %d", n, len(bad))
	}
}
