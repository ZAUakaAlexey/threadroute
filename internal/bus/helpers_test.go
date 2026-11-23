package bus

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Synthetic, clearly fake test data (decision 0004).
const (
	testAlice = "did:plc:testalice0000000000000000"
	testBob   = "did:plc:testbob000000000000000000"
	testCarol = "did:plc:testcarol0000000000000000"
)

// testRawLimits are small, valid limits for the raw topic.
func testRawLimits() RawLimits {
	return RawLimits{Retention: 2 * time.Hour, MaxBytes: 4 * RawMinMaxBytes}
}

func testSpecs(t testing.TB) []TopicSpec {
	t.Helper()
	raw, err := RawTopicSpec(testRawLimits())
	if err != nil {
		t.Fatalf("RawTopicSpec() error = %v", err)
	}
	return []TopicSpec{raw, EventsTopicSpec(), CursorTopicSpec()}
}

func testContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newTestCluster starts a one-broker fake cluster that is closed at the end
// of the test.
func newTestCluster(t testing.TB, opts ...kfake.Opt) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(append([]kfake.Opt{kfake.NumBrokers(1)}, opts...)...)
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func newTestClient(t testing.TB, c *kfake.Cluster) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatalf("kgo.NewClient() error = %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func newTestAdmin(t testing.TB, c *kfake.Cluster) *kadm.Client {
	t.Helper()
	return kadm.NewClient(newTestClient(t, c))
}

// newTestClusterWithTopics starts a cluster and creates the three topics.
func newTestClusterWithTopics(t testing.TB) *kfake.Cluster {
	t.Helper()
	c := newTestCluster(t)
	if err := EnsureTopics(testContext(t), newTestClient(t, c), discardLogger(), testSpecs(t)...); err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}
	return c
}

func testProducerLimits() ProducerLimits {
	return ProducerLimits{MaxBufferedRecords: 1000, MaxBufferedBytes: 8 << 20, DeliveryTimeout: 10 * time.Second}
}

func newTestWriter(t testing.TB, c *kfake.Cluster, limits ProducerLimits, reg prometheus.Registerer) *RawWriter {
	t.Helper()
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	w, err := NewRawWriter(c.ListenAddrs(), limits, SourceBluesky, discardLogger(), reg)
	if err != nil {
		t.Fatalf("NewRawWriter() error = %v", err)
	}
	t.Cleanup(func() {
		// Short, because a test can leave records that never get acked.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = w.Close(ctx)
	})
	return w
}

// readAll reads every record of a topic, up to the end offsets at the time
// of the call.
func readAll(t testing.TB, c *kfake.Cluster, topic string) []*kgo.Record {
	t.Helper()
	ctx := testContext(t)
	adm := newTestAdmin(t, c)
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil || ends.Error() != nil {
		t.Fatalf("ListEndOffsets(%s) error = %v, %v", topic, err, ends.Error())
	}
	want := 0
	parts := map[int32]kgo.Offset{}
	ends.Each(func(o kadm.ListedOffset) {
		want += int(o.Offset)
		parts[o.Partition] = kgo.NewOffset().AtStart()
	})
	if want == 0 {
		return nil
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: parts}),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient() error = %v", err)
	}
	defer cl.Close()
	var out []*kgo.Record
	for len(out) < want {
		f := cl.PollFetches(ctx)
		if errs := f.Errors(); len(errs) > 0 {
			t.Fatalf("PollFetches(%s) errors = %v", topic, errs)
		}
		out = append(out, f.Records()...)
	}
	return out
}

func endOffset(t testing.TB, c *kfake.Cluster, topic string) int64 {
	t.Helper()
	ends, err := newTestAdmin(t, c).ListEndOffsets(testContext(t), topic)
	if err != nil || ends.Error() != nil {
		t.Fatalf("ListEndOffsets(%s) error = %v, %v", topic, err, ends.Error())
	}
	var sum int64
	ends.Each(func(o kadm.ListedOffset) { sum += o.Offset })
	return sum
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// bufferLogger is a logger that writes text to a buffer that is safe to
// read while other goroutines log.
type bufferLogger struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *bufferLogger) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *bufferLogger) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *bufferLogger) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func syntheticPayload(n int) []byte {
	return fmt.Appendf(nil, `{"text":"synthetic reply one","n":%d}`, n)
}
