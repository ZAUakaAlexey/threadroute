//go:build integration

package bus

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

// kafkaImage is the same broker as in deploy/compose.yaml.
const kafkaImage = "apache/kafka:4.4.0"

const (
	// kafkaHostPort is the container port of the listener for the test
	// process. Docker maps it to a random host port.
	kafkaHostPort = "19092/tcp"
	// advertisedFile is written by a start hook once the mapped port is
	// known. The container waits for it before it starts Kafka.
	advertisedFile = "/tmp/advertised-listeners"
)

// startKafka starts a single Apache Kafka node in KRaft combined mode and
// returns its bootstrap address.
//
// The Testcontainers Kafka module (v0.45.0) only supports the
// confluentinc/confluent-local image: its start script needs files that
// exist only in Confluent images. So the module is not used: this test
// runs the same plain Apache image as deploy/compose.yaml, with the same
// trick as the module: the container waits for a file with
// the advertised listeners, which a start hook writes once Docker has
// mapped the port.
func startKafka(t *testing.T) string {
	t.Helper()
	ctx := t.Context()

	ctr, err := testcontainers.Run(ctx, kafkaImage,
		testcontainers.WithExposedPorts(kafkaHostPort),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "INTERNAL://:9092,CONTROLLER://:9093,HOST://:19092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,HOST:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "INTERNAL",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			"KAFKA_HEAP_OPTS":                                "-Xmx512m -Xms512m",
		}),
		testcontainers.WithEntrypoint("sh", "-c",
			"while [ ! -f "+advertisedFile+" ]; do sleep 0.1; done; "+
				`export KAFKA_ADVERTISED_LISTENERS="$(cat `+advertisedFile+`)"; `+
				"exec /etc/kafka/docker/run"),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{
				func(ctx context.Context, c testcontainers.Container) error {
					if err := wait.ForMappedPort(kafkaHostPort).WaitUntilReady(ctx, c); err != nil {
						return err
					}
					endpoint, err := c.PortEndpoint(ctx, kafkaHostPort, "HOST")
					if err != nil {
						return err
					}
					listeners := "INTERNAL://localhost:9092," + endpoint
					if err := c.CopyToContainer(ctx, []byte(listeners), advertisedFile, 0o644); err != nil {
						return err
					}
					return wait.ForLog("Kafka Server started").
						WithStartupTimeout(2*time.Minute).WaitUntilReady(ctx, c)
				},
			},
		}),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start %s: %v", kafkaImage, err)
	}
	addr, err := ctr.PortEndpoint(ctx, kafkaHostPort, "")
	if err != nil {
		t.Fatalf("kafka endpoint: %v", err)
	}
	return addr
}

func TestIntegrationRealBroker(t *testing.T) {
	broker := startKafka(t)
	brokers := []string{broker}

	t.Run("EnsureTopics", func(t *testing.T) {
		raw, err := RawTopicSpec(DefaultRawLimits())
		if err != nil {
			t.Fatal(err)
		}
		specs := []TopicSpec{raw, EventsTopicSpec(), ControlTopicSpec(), ConversationsTopicSpec(), CursorTopicSpec()}
		testEnsureTopicsOnBroker(t, brokers, specs)

		// The smallest valid limits give the smallest segment.bytes
		// (RawSegmentBytesMin). This also changes the existing raw topic,
		// so the alter path runs against the real broker.
		minimal, err := RawTopicSpec(RawLimits{Retention: RawSegmentInterval, MaxBytes: RawMinMaxBytes})
		if err != nil {
			t.Fatal(err)
		}
		if got := *minimal.Configs[ConfigSegmentBytes]; got != strconv.FormatInt(RawSegmentBytesMin, 10) {
			t.Fatalf("minimal segment.bytes = %s, want %d", got, RawSegmentBytesMin)
		}
		testEnsureTopicsOnBroker(t, brokers, []TopicSpec{minimal})
		// Put the defaults back for the next subtests.
		testEnsureTopicsOnBroker(t, brokers, []TopicSpec{raw})
	})

	t.Run("CursorConfigsUpgrade", func(t *testing.T) {
		testCursorConfigsUpgradeOnBroker(t, brokers)
	})

	t.Run("WriteBatchAndReadCursor", func(t *testing.T) {
		testWriteBatchOnBroker(t, brokers)
	})

	t.Run("SameKeySamePartition", func(t *testing.T) {
		testKeyPartitionOnBroker(t, brokers)
	})

	// Last: it writes tens of thousands of cursor records, and the
	// subtests above count the records of the cursor topic.
	t.Run("ReadCursorLargeTopic", func(t *testing.T) {
		testReadCursorLargeTopicOnBroker(t, brokers)
	})
}

func newBrokerClient(t *testing.T, brokers []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func testEnsureTopicsOnBroker(t *testing.T, brokers []string, specs []TopicSpec) {
	t.Helper()
	ctx := testContext(t)
	cl := newBrokerClient(t, brokers)

	if err := EnsureTopics(ctx, cl, discardLogger(), specs...); err != nil {
		t.Fatalf("first EnsureTopics() error = %v", err)
	}

	// countingAdmin is declared in admin_test.go.
	admin := &countingAdmin{TopicAdmin: kadm.NewClient(cl)}
	if err := ensureTopics(ctx, admin, cl, discardLogger(), specs...); err != nil {
		t.Fatalf("second EnsureTopics() error = %v", err)
	}
	if c, p, a := admin.creates.Load(), admin.partitions.Load(), admin.alters.Load(); c+p+a != 0 {
		t.Errorf("second call changed topics: %d creates, %d partition updates, %d config alters", c, p, a)
	}

	adm := kadm.NewClient(cl)
	for _, spec := range specs {
		layout, exists, err := describeTopic(ctx, cl, spec.Name)
		if err != nil || !exists {
			t.Fatalf("describe %s: exists=%v, error=%v", spec.Name, exists, err)
		}
		if layout.partitions != spec.Partitions || layout.replicas != int(spec.ReplicationFactor) {
			t.Errorf("%s layout = %+v, want %d partitions, %d replicas", spec.Name, layout, spec.Partitions, spec.ReplicationFactor)
		}

		rcs, err := adm.DescribeTopicConfigs(ctx, spec.Name)
		if err != nil {
			t.Fatal(err)
		}
		rc, err := rcs.On(spec.Name, nil)
		if err != nil || rc.Err != nil {
			t.Fatalf("describe configs of %s: %v, %v", spec.Name, err, rc.Err)
		}
		got := map[string]kadm.Config{}
		for _, c := range rc.Configs {
			got[c.Key] = c
		}
		for key, want := range spec.Configs {
			c, ok := got[key]
			if !ok {
				t.Errorf("%s: config %s missing on the broker", spec.Name, key)
				continue
			}
			if c.MaybeValue() != *want {
				t.Errorf("%s: config %s = %q, want %q", spec.Name, key, c.MaybeValue(), *want)
			}
			if c.Source.String() != "DYNAMIC_TOPIC_CONFIG" {
				t.Errorf("%s: config %s has source %s, want DYNAMIC_TOPIC_CONFIG", spec.Name, key, c.Source)
			}
		}
	}
}

// readTopic reads every record of a topic up to its end offsets now.
func readTopic(t *testing.T, brokers []string, topic string) []*kgo.Record {
	t.Helper()
	ctx := testContext(t)
	cl := newBrokerClient(t, brokers, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil || ends.Error() != nil {
		t.Fatalf("ListEndOffsets(%s) error = %v, %v", topic, err, ends.Error())
	}
	want := 0
	ends.Each(func(o kadm.ListedOffset) { want += int(o.Offset) })
	var out []*kgo.Record
	for len(out) < want {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records of %s: %v", len(out), want, topic, ctx.Err())
		}
		if errs := f.Errors(); len(errs) > 0 {
			t.Fatalf("fetch %s: %v", topic, errs[0].Err)
		}
		out = append(out, f.Records()...)
	}
	return out
}

func newBrokerWriter(t *testing.T, brokers []string) *RawWriter {
	t.Helper()
	w, err := NewRawWriter(brokers, testProducerLimits(), SourceBluesky, discardLogger(), prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.Close(ctx)
	})
	return w
}

func testWriteBatchOnBroker(t *testing.T, brokers []string) {
	ctx := testContext(t)
	w := newBrokerWriter(t, brokers)

	events := testEvents(30)
	batch := source.Batch{Events: events, Cursor: testCursor(30)}
	before := time.Now().Add(-2 * time.Second)
	if err := w.WriteBatch(ctx, batch); err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	after := time.Now().Add(2 * time.Second)

	records := readTopic(t, brokers, TopicRaw)
	if len(records) != len(events) {
		t.Fatalf("got %d raw records, want %d", len(records), len(events))
	}
	byID := map[string]source.RawEvent{}
	for _, ev := range events {
		byID[ev.ID] = ev
	}
	for _, r := range records {
		id, _ := header(r, HeaderEventID)
		ev, ok := byID[id]
		if !ok {
			t.Errorf("record at %d/%d has unknown event ID %q", r.Partition, r.Offset, id)
			continue
		}
		if string(r.Key) != ev.Key || string(r.Value) != string(ev.Payload) {
			t.Errorf("event %s: key or value differs", id)
		}
		if kind, _ := header(r, HeaderEventKind); kind != string(ev.Kind) {
			t.Errorf("event %s: kind header %q, want %q", id, kind, ev.Kind)
		}
		if ws, _ := header(r, HeaderEventWitnessed); ws != strconv.FormatInt(ev.Witnessed.UnixMicro(), 10) {
			t.Errorf("event %s: witnessed header %q", id, ws)
		}
		// 1 is LogAppendTime: the broker stamped the record.
		if tt := r.Attrs.TimestampType(); tt != 1 {
			t.Errorf("event %s: timestamp type %d, want 1 (LogAppendTime)", id, tt)
		}
		if r.Timestamp.Before(before) || r.Timestamp.After(after) {
			t.Errorf("event %s: timestamp %s is not the append time", id, r.Timestamp)
		}
	}

	cursors := readTopic(t, brokers, TopicCursor)
	if len(cursors) != 1 || string(cursors[0].Key) != SourceBluesky {
		t.Fatalf("got %d cursor records, want 1 with key %q", len(cursors), SourceBluesky)
	}
	got, found, err := ReadCursor(ctx, brokers, SourceBluesky, discardLogger())
	if err != nil || !found {
		t.Fatalf("ReadCursor() = %v, %v", found, err)
	}
	want := testCursor(30)
	if !got.Witnessed.Equal(want.Witnessed) || got.Seq != want.Seq || got.Host != want.Host {
		t.Errorf("ReadCursor() = %+v, want %+v", got, want)
	}
}

func testKeyPartitionOnBroker(t *testing.T, brokers []string) {
	ctx := testContext(t)
	// Two writers with separate clients, so the mapping does not depend on
	// the state of one client.
	for range 2 {
		w := newBrokerWriter(t, brokers)
		for range 3 {
			if err := w.WriteBatch(ctx, source.Batch{Events: testEvents(60)}); err != nil {
				t.Fatalf("WriteBatch() error = %v", err)
			}
		}
	}

	partitionOf := map[string]int32{}
	used := map[int32]bool{}
	for _, r := range readTopic(t, brokers, TopicRaw) {
		used[r.Partition] = true
		p, seen := partitionOf[string(r.Key)]
		if !seen {
			partitionOf[string(r.Key)] = r.Partition
			continue
		}
		if p != r.Partition {
			t.Errorf("key %s is in partitions %d and %d", r.Key, p, r.Partition)
		}
	}
	if len(partitionOf) != 3 {
		t.Errorf("got %d keys, want 3", len(partitionOf))
	}
	t.Logf("3 keys in %d partitions", len(used))
}

// testCursorConfigsUpgradeOnBroker removes the compaction configs from the
// cursor topic, as a topic created by an older version has them, and checks
// that EnsureTopics sets them again in one alter call.
func testCursorConfigsUpgradeOnBroker(t *testing.T, brokers []string) {
	ctx := testContext(t)
	cl := newBrokerClient(t, brokers)
	adm := kadm.NewClient(cl)
	spec := CursorTopicSpec()

	var remove []kadm.AlterConfig
	for key := range spec.Configs {
		if key != ConfigCleanupPolicy {
			remove = append(remove, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: key})
		}
	}
	resps, err := adm.AlterTopicConfigs(ctx, remove, TopicCursor)
	if err == nil {
		for _, r := range resps {
			if r.Err != nil {
				err = r.Err
			}
		}
	}
	if err != nil {
		t.Fatalf("remove cursor configs: %v", err)
	}

	admin := &countingAdmin{TopicAdmin: adm}
	if err := ensureTopics(ctx, admin, cl, discardLogger(), spec); err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}
	if got := admin.alters.Load(); got != 1 {
		t.Errorf("alter calls = %d, want 1", got)
	}

	// EnsureTopics compares values only. min.compaction.lag.ms is 0, the
	// broker default, so on an upgraded topic it stays a default and is
	// not set as a topic config. The value is still the spec value.
	rcs, err := adm.DescribeTopicConfigs(ctx, TopicCursor)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := rcs.On(TopicCursor, nil)
	if err != nil || rc.Err != nil {
		t.Fatalf("describe configs of %s: %v, %v", TopicCursor, err, rc.Err)
	}
	got := map[string]string{}
	for _, c := range rc.Configs {
		got[c.Key] = c.MaybeValue()
	}
	for key, want := range spec.Configs {
		if got[key] != *want {
			t.Errorf("%s: config %s = %q, want %q", TopicCursor, key, got[key], *want)
		}
	}

	again := &countingAdmin{TopicAdmin: adm}
	if err := ensureTopics(ctx, again, cl, discardLogger(), spec); err != nil {
		t.Fatalf("second EnsureTopics() error = %v", err)
	}
	if got := again.alters.Load(); got != 0 {
		t.Errorf("second call alter calls = %d, want 0", got)
	}
}

// largeCursorRecords is the number of cursor records the large topic test
// writes: about 27 minutes of cursor writes at the measured rate.
const largeCursorRecords = 30_000

// readCursorLargeBound is the time ReadCursor may take on the large topic.
// Reading from the end takes a few tens of milliseconds on a local broker;
// the bound leaves room for a slow CI machine.
const readCursorLargeBound = 3 * time.Second

func testReadCursorLargeTopicOnBroker(t *testing.T, brokers []string) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	// Small batches, close to the real writer, which sends one record per
	// batch, so the topic fills several 1 MiB segments. 512 bytes is the
	// smallest batch size the client accepts: two cursor records.
	cl := newBrokerClient(t, brokers, kgo.ProducerBatchMaxBytes(512))

	// One record for another source first. Reading it later has to step
	// back through many windows to the start of the topic.
	other := &kgo.Record{Topic: TopicCursor, Key: CursorKey(testOtherSource), Value: cursorValue(t, 5)}
	if err := cl.ProduceSync(ctx, other).FirstErr(); err != nil {
		t.Fatalf("produce cursor record: %v", err)
	}

	start := time.Now()
	recs := make([]*kgo.Record, largeCursorRecords)
	for i := range recs {
		v, err := MarshalCursor(CursorRecord{
			Host:      "jetstream-east.example.test",
			Witnessed: testWitnessedBase.Add(time.Duration(i+1) * time.Millisecond),
			Seq:       uint64(1000 + i + 1),
			WrittenAt: time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
		recs[i] = &kgo.Record{Topic: TopicCursor, Key: CursorKey(SourceBluesky), Value: v}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce cursor records: %v", err)
	}
	t.Logf("produced %d cursor records in %s", largeCursorRecords, time.Since(start).Round(time.Millisecond))

	counter := &fetchCounter{}
	readStart := time.Now()
	got, found, err := ReadCursor(ctx, brokers, SourceBluesky, discardLogger(), kgo.WithHooks(counter))
	took := time.Since(readStart)
	if err != nil || !found {
		t.Fatalf("ReadCursor() = %v, %v", found, err)
	}
	if want := testCursor(largeCursorRecords); got.Seq != want.Seq || !got.Witnessed.Equal(want.Witnessed) {
		t.Errorf("ReadCursor() = %+v, want %+v", got, want)
	}
	t.Logf("ReadCursor took %s and fetched %d records", took.Round(time.Millisecond), counter.n.Load())
	if took > readCursorLargeBound {
		t.Errorf("ReadCursor took %s, want at most %s", took, readCursorLargeBound)
	}
	if n := counter.n.Load(); n > 2*cursorWindowFirst {
		t.Errorf("ReadCursor fetched %d records, want at most %d", n, 2*cursorWindowFirst)
	}

	testReadOtherSourceOnBroker(t, ctx, brokers, "before compaction")

	// Compaction. The topic now has closed 1 MiB segments full of old
	// copies of one key. The broker cleaner looks for work every 15
	// seconds (log.cleaner.backoff.ms), so with the low dirty ratio it
	// compacts them well within a minute. On a local broker this took
	// about 15 seconds. The open segment is not compacted, so the count
	// does not go down to one.
	//
	// The baseline is the number of offsets, not a count read now: the
	// cleaner runs on its own timer and may already have run.
	logStart, end, err := cursorOffsets(ctx, kadm.NewClient(cl))
	if err != nil {
		t.Fatal(err)
	}
	offsets := int(end - logStart)
	deadline := time.Now().Add(compactionWait)
	for {
		count := countBrokerRecords(t, brokers, end)
		if count < offsets/2 {
			t.Logf("compacted after %s: %d offsets, %d records left, %d bytes on disk",
				time.Since(start).Round(time.Second), offsets, count, cursorTopicSize(t, cl))
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not compacted within %s: %d offsets, %d records, %d bytes on disk",
				compactionWait, offsets, count, cursorTopicSize(t, cl))
		}
		time.Sleep(2 * time.Second)
	}

	// After compaction the offsets have gaps, and the newest record of
	// each source is still found.
	got, found, err = ReadCursor(ctx, brokers, SourceBluesky, discardLogger())
	if err != nil || !found {
		t.Fatalf("ReadCursor() after compaction = %v, %v", found, err)
	}
	if want := testCursor(largeCursorRecords); got.Seq != want.Seq {
		t.Errorf("ReadCursor() after compaction = %+v, want %+v", got, want)
	}
	testReadOtherSourceOnBroker(t, ctx, brokers, "after compaction")
}

// testReadOtherSourceOnBroker reads the cursor of testOtherSource, whose
// only record is at the start of the topic.
func testReadOtherSourceOnBroker(t *testing.T, ctx context.Context, brokers []string, when string) {
	t.Helper()
	counter := &fetchCounter{}
	readStart := time.Now()
	got, found, err := ReadCursor(ctx, brokers, testOtherSource, discardLogger(), kgo.WithHooks(counter))
	if err != nil || !found {
		t.Fatalf("ReadCursor(%s) %s = %v, %v", testOtherSource, when, found, err)
	}
	if want := testCursor(5); got.Seq != want.Seq {
		t.Errorf("ReadCursor(%s) %s = %+v, want %+v", testOtherSource, when, got, want)
	}
	t.Logf("ReadCursor(%s) %s took %s and fetched %d records",
		testOtherSource, when, time.Since(readStart).Round(time.Millisecond), counter.n.Load())
}

// compactionWait bounds the wait for the broker cleaner in the large topic
// test.
const compactionWait = 60 * time.Second

// cursorTopicSize returns the on-disk size of the cursor topic.
func cursorTopicSize(t *testing.T, cl *kgo.Client) int64 {
	t.Helper()
	dirs, err := kadm.NewClient(cl).DescribeAllLogDirs(testContext(t), kadm.TopicsSet{TopicCursor: {cursorPartition: {}}})
	if err != nil {
		t.Fatal(err)
	}
	var size int64
	for _, d := range dirs {
		if err := d.Error(); err != nil {
			t.Fatal(err)
		}
		size += d.Size()
	}
	return size
}

// countBrokerRecords counts the records of the cursor topic below end. It
// stops at end-1, because a compacted topic holds fewer records than its
// end offset.
func countBrokerRecords(t *testing.T, brokers []string, end int64) int {
	t.Helper()
	if end == 0 {
		return 0
	}
	ctx := testContext(t)
	cl := newBrokerClient(t, brokers, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		TopicCursor: {cursorPartition: kgo.NewOffset().AtStart()},
	}))
	defer cl.Close()
	n := 0
	for {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("count cursor records: %v", ctx.Err())
		}
		if errs := f.Errors(); len(errs) > 0 {
			t.Fatalf("fetch %s: %v", TopicCursor, errs[0].Err)
		}
		last := false
		f.EachRecord(func(r *kgo.Record) {
			if r.Offset < end {
				n++
			}
			last = last || r.Offset >= end-1
		})
		if last {
			return n
		}
	}
}
