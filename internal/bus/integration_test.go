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
		specs := []TopicSpec{raw, EventsTopicSpec(), CursorTopicSpec()}
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

	t.Run("WriteBatchAndReadCursor", func(t *testing.T) {
		testWriteBatchOnBroker(t, brokers)
	})

	t.Run("SameKeySamePartition", func(t *testing.T) {
		testKeyPartitionOnBroker(t, brokers)
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
