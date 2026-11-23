package bus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

const testOtherSource = "synthetic-other"

type cursorInput struct {
	key   string
	value []byte // nil writes a tombstone
}

func cursorValue(t *testing.T, n int) []byte {
	t.Helper()
	c := testCursor(n)
	b, err := MarshalCursor(CursorRecord{
		Host:      c.Host,
		Witnessed: c.Witnessed,
		Seq:       c.Seq,
		WrittenAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("MarshalCursor() error = %v", err)
	}
	return b
}

func produceCursorRecords(t *testing.T, c *kfake.Cluster, in []cursorInput) {
	t.Helper()
	if len(in) == 0 {
		return
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatalf("kgo.NewClient() error = %v", err)
	}
	defer cl.Close()
	recs := make([]*kgo.Record, len(in))
	for i, r := range in {
		recs[i] = &kgo.Record{Topic: TopicCursor, Key: []byte(r.key), Value: r.value}
	}
	if err := cl.ProduceSync(testContext(t), recs...).FirstErr(); err != nil {
		t.Fatalf("ProduceSync() error = %v", err)
	}
}

func TestReadCursor(t *testing.T) {
	tests := []struct {
		name      string
		records   func(t *testing.T) []cursorInput
		wantFound bool
		wantN     int // testCursor(wantN) is the expected cursor
		wantWarn  bool
		wantReset bool // an info line about a tombstone reset
	}{
		{
			name:    "empty topic",
			records: func(*testing.T) []cursorInput { return nil },
		},
		{
			name: "newest of several",
			records: func(t *testing.T) []cursorInput {
				return []cursorInput{
					{SourceBluesky, cursorValue(t, 1)},
					{SourceBluesky, cursorValue(t, 2)},
					{SourceBluesky, cursorValue(t, 3)},
				}
			},
			wantFound: true, wantN: 3,
		},
		{
			name: "malformed newest is skipped",
			records: func(t *testing.T) []cursorInput {
				return []cursorInput{
					{SourceBluesky, cursorValue(t, 1)},
					{SourceBluesky, cursorValue(t, 2)},
					{SourceBluesky, []byte("not json")},
				}
			},
			wantFound: true, wantN: 2, wantWarn: true,
		},
		{
			name: "only malformed",
			records: func(*testing.T) []cursorInput {
				return []cursorInput{{SourceBluesky, []byte(`{"host":"h"}`)}}
			},
			wantWarn: true,
		},
		{
			name: "other source only",
			records: func(t *testing.T) []cursorInput {
				return []cursorInput{{testOtherSource, cursorValue(t, 5)}}
			},
		},
		{
			name: "other source records are ignored",
			records: func(t *testing.T) []cursorInput {
				return []cursorInput{
					{SourceBluesky, cursorValue(t, 1)},
					{testOtherSource, cursorValue(t, 9)},
					{testOtherSource, []byte("not json")},
				}
			},
			wantFound: true, wantN: 1,
		},
		{
			name: "newest tombstone resets to live",
			records: func(t *testing.T) []cursorInput {
				return []cursorInput{
					{SourceBluesky, cursorValue(t, 1)},
					{SourceBluesky, nil},
				}
			},
			wantReset: true,
		},
		{
			name: "malformed after a tombstone keeps the reset",
			records: func(t *testing.T) []cursorInput {
				return []cursorInput{
					{SourceBluesky, cursorValue(t, 1)},
					{SourceBluesky, nil},
					{SourceBluesky, []byte("not json")},
				}
			},
			wantWarn: true, wantReset: true,
		},
		{
			name: "tombstone for another source is ignored",
			records: func(t *testing.T) []cursorInput {
				return []cursorInput{
					{SourceBluesky, cursorValue(t, 1)},
					{testOtherSource, nil},
				}
			},
			wantFound: true, wantN: 1,
		},
		{
			name: "cursor after a tombstone",
			records: func(t *testing.T) []cursorInput {
				return []cursorInput{
					{SourceBluesky, cursorValue(t, 1)},
					{SourceBluesky, nil},
					{SourceBluesky, cursorValue(t, 4)},
				}
			},
			wantFound: true, wantN: 4,
		},
		{
			name: "empty non-null value is malformed",
			records: func(t *testing.T) []cursorInput {
				return []cursorInput{
					{SourceBluesky, cursorValue(t, 2)},
					{SourceBluesky, []byte{}},
				}
			},
			wantFound: true, wantN: 2, wantWarn: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClusterWithTopics(t)
			produceCursorRecords(t, c, tt.records(t))
			logs := &bufferLogger{}

			got, found, err := ReadCursor(testContext(t), c.ListenAddrs(), SourceBluesky, logs.logger())
			if err != nil {
				t.Fatalf("ReadCursor() error = %v", err)
			}
			if found != tt.wantFound {
				t.Fatalf("ReadCursor() found = %v, want %v", found, tt.wantFound)
			}
			if found {
				want := testCursor(tt.wantN)
				if got.Host != want.Host || got.Seq != want.Seq || !got.Witnessed.Equal(want.Witnessed) {
					t.Errorf("ReadCursor() = %+v, want %+v", got, want)
				}
			} else if got != (source.Cursor{}) {
				t.Errorf("ReadCursor() = %+v with found = false, want the zero cursor", got)
			}
			warned := strings.Contains(logs.String(), "level=WARN")
			if warned != tt.wantWarn {
				t.Errorf("warning logged = %v, want %v; logs:\n%s", warned, tt.wantWarn, logs.String())
			}
			reset := strings.Contains(logs.String(), `level=INFO msg="cursor was reset by a tombstone`)
			if reset != tt.wantReset {
				t.Errorf("reset logged = %v, want %v; logs:\n%s", reset, tt.wantReset, logs.String())
			}
		})
	}
}

func TestReadCursorAfterWriteBatch(t *testing.T) {
	c := newTestClusterWithTopics(t)
	w := newTestWriter(t, c, testProducerLimits(), nil)
	ctx := testContext(t)
	for n := 1; n <= 3; n++ {
		if err := w.WriteBatch(ctx, source.Batch{Events: testEvents(2), Cursor: testCursor(n)}); err != nil {
			t.Fatalf("WriteBatch() error = %v", err)
		}
	}
	got, found, err := ReadCursor(ctx, c.ListenAddrs(), SourceBluesky, discardLogger())
	if err != nil || !found {
		t.Fatalf("ReadCursor() = %v, %v, want a cursor", found, err)
	}
	if want := testCursor(3); !got.Witnessed.Equal(want.Witnessed) || got.Seq != want.Seq || got.Host != want.Host {
		t.Errorf("ReadCursor() = %+v, want %+v", got, want)
	}
}

func TestReadCursorMissingTopic(t *testing.T) {
	c := newTestCluster(t)
	_, found, err := ReadCursor(testContext(t), c.ListenAddrs(), SourceBluesky, discardLogger())
	if err == nil || found {
		t.Errorf("ReadCursor() = %v, %v, want an error", found, err)
	}
}

func TestReadCursorCancelled(t *testing.T) {
	c := newTestClusterWithTopics(t)
	produceCursorRecords(t, c, []cursorInput{{SourceBluesky, cursorValue(t, 1)}})

	ctx, cancel := context.WithCancel(testContext(t))
	cancel()
	_, found, err := ReadCursor(ctx, c.ListenAddrs(), SourceBluesky, discardLogger())
	if !errors.Is(err, context.Canceled) || found {
		t.Errorf("ReadCursor() = %v, %v, want context.Canceled", found, err)
	}
}

func TestReadCursorStopsAtDeadline(t *testing.T) {
	c := newTestClusterWithTopics(t)
	produceCursorRecords(t, c, []cursorInput{{SourceBluesky, cursorValue(t, 1)}})

	// Hold every fetch, so the read cannot reach the end of the topic.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	c.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.SleepControl(func() { <-release })
		return nil, nil, false
	})

	ctx, cancel := context.WithTimeout(testContext(t), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, found, err := ReadCursor(ctx, c.ListenAddrs(), SourceBluesky, discardLogger())
	if !errors.Is(err, context.DeadlineExceeded) || found {
		t.Errorf("ReadCursor() = %v, %v, want context.DeadlineExceeded", found, err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("ReadCursor() took %s after a 300ms deadline", d)
	}
}
