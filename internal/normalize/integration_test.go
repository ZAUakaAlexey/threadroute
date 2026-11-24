//go:build integration

package normalize

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
)

// The integration tests prove the three properties that the fake cluster and
// miniredis cannot: on a real Apache Kafka broker and a real Redis,
//
//  1. a transaction aborted in the middle of a batch leaves nothing visible
//     to a read-committed consumer, and the next run emits each event once,
//  2. one event ID at two raw offsets gives one output,
//  3. with Redis stopped the service stops, and nothing is lost after Redis
//     is back.
//
// The subtests share one broker and one Redis. Each one uses its own
// consumer group, transactional ID and event IDs, and empties Redis first.
// A new group reads the raw topic from the start, so it also emits the
// records of earlier subtests again. The checks therefore look only at the
// event IDs of their own subtest.

// kafkaImage is the same broker as in deploy/compose.yaml.
const kafkaImage = "apache/kafka:4.4.0"

// redisImage is an official Redis image. SET with NX and GET together needs
// Redis 7.0 or newer.
const redisImage = "redis:7-alpine"

const (
	kafkaHostPort  = "19092/tcp"
	advertisedFile = "/tmp/advertised-listeners"
	redisPort      = "6379/tcp"
)

// startKafka starts a single Apache Kafka node in KRaft combined mode and
// returns its bootstrap address. It is a copy of the helper in
// internal/bus/integration_test.go, which a test of another package cannot
// import. See there for why the Testcontainers Kafka module is not used.
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

// startRedis starts Redis and returns the container.
func startRedis(t *testing.T) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.Run(t.Context(), redisImage,
		testcontainers.WithExposedPorts(redisPort),
		testcontainers.WithWaitStrategy(wait.ForListeningPort(redisPort).WithStartupTimeout(time.Minute)),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start %s: %v", redisImage, err)
	}
	return ctr
}

// redisAddr returns the mapped address. It can change after a restart of
// the container, so it is read again each time.
func redisAddr(t *testing.T, ctr testcontainers.Container) string {
	t.Helper()
	addr, err := ctr.PortEndpoint(t.Context(), redisPort, "")
	if err != nil {
		t.Fatalf("redis endpoint: %v", err)
	}
	return addr
}

func TestIntegrationNormalize(t *testing.T) {
	brokers := []string{startKafka(t)}
	redisCtr := startRedis(t)
	createRawTopic(t, brokers)

	t.Run("AbortMidBatch", func(t *testing.T) {
		testAbortMidBatchOnBroker(t, brokers, redisCtr)
	})
	t.Run("SameEventTwoOffsets", func(t *testing.T) {
		testSameEventOnBroker(t, brokers, redisCtr)
	})
	t.Run("TransactionsWithoutOutput", func(t *testing.T) {
		testNoOutputOnBroker(t, brokers, redisCtr)
	})
	// Last: it stops and starts Redis.
	t.Run("RedisStopped", func(t *testing.T) {
		testRedisStoppedOnBroker(t, brokers, redisCtr)
	})
}

// setup returns a config with its own group and transactional ID, and a
// Redis client on an empty Redis.
func setup(t *testing.T, brokers []string, redisCtr testcontainers.Container, name string) (Config, *redis.Client) {
	t.Helper()
	addr := redisAddr(t, redisCtr)
	cfg := testConfig(brokers, addr)
	cfg.GroupID = "normalize-it-" + name
	cfg.TransactionalID = "normalize-it-" + name
	cfg.MaxBatchRecords = 20
	rdb := NewRedisClient(cfg)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.FlushAll(testContext(t)).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	return cfg, rdb
}

// replies returns n customer replies with event IDs unique to name, spread
// over three authors, and their event IDs.
func replies(t *testing.T, name string, n int) ([]*kgo.Record, []string) {
	t.Helper()
	root := postURI(testAlice, "root000")
	authors := []string{testBob, testCarol, "did:plc:testdave00000000000000000"}
	recs := make([]*kgo.Record, n)
	ids := make([]string, n)
	for i := range n {
		author := authors[i%len(authors)]
		rkey := fmt.Sprintf("%s%04d", name, i)
		ids[i] = commitID(author, "create", rkey)
		recs[i] = rawRecord(author, commitValue(t, author, "create", "app.bsky.feed.post", rkey, replyRecord(root, root)), ids[i], "commit", testWitnessed)
	}
	return recs, ids
}

// rawCount is the number of records in the raw topic. The raw topic is not
// written in transactions, so it is the sum of the end offsets.
func rawCount(t *testing.T, brokers []string) int {
	t.Helper()
	n := 0
	for _, end := range rawEndOffsets(t, brokers) {
		n += int(end)
	}
	return n
}

// countIDs reads the events topic with the given isolation level and counts
// the records of each wanted event ID. It reads until every ID was seen at
// least once (when all is true) or until one was seen (when all is false),
// and then for quiet more. A read that must see nothing reads for quiet
// only.
func countIDs(t *testing.T, brokers []string, level kgo.IsolationLevel, ids []string, need int, quiet time.Duration) map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	cl := newClient(t, brokers,
		kgo.ConsumeTopics(bus.TopicEvents),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(level),
	)
	defer cl.Close()
	wanted := map[string]bool{}
	for _, id := range ids {
		wanted[id] = true
	}
	counts := map[string]int{}
	poll := func(ctx context.Context) {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			if id := headerOf(r, bus.HeaderEventID); wanted[id] {
				counts[id]++
			}
		})
	}
	for len(counts) < need {
		poll(ctx)
		if ctx.Err() != nil {
			t.Fatalf("saw %d of %d event IDs", len(counts), need)
		}
	}
	qctx, qcancel := context.WithTimeout(ctx, quiet)
	defer qcancel()
	for qctx.Err() == nil {
		poll(qctx)
	}
	return counts
}

func countValue(c prometheus.Collector) float64 {
	return testutil.ToFloat64(c)
}

func assertOnce(t *testing.T, counts map[string]int, ids []string) {
	t.Helper()
	for _, id := range ids {
		if counts[id] != 1 {
			t.Errorf("event %q seen %d times by read committed, want 1", id, counts[id])
		}
	}
}

func groupOffsetsOf(t *testing.T, brokers []string, group string) map[int32]int64 {
	t.Helper()
	resp, err := kadm.NewClient(newClient(t, brokers)).FetchOffsets(testContext(t), group)
	if err != nil {
		t.Fatalf("FetchOffsets() error = %v", err)
	}
	out := map[int32]int64{}
	resp.Each(func(o kadm.OffsetResponse) {
		if o.Topic == bus.TopicRaw && o.Err == nil && o.At >= 0 {
			out[o.Partition] = o.At
		}
	})
	return out
}

// testNoOutputOnBroker runs many small transactions that produce no record,
// only offsets, as most batches of live traffic do. On Kafka 4.x the broker
// often refuses such an offset commit with CONCURRENT_TRANSACTIONS when it
// comes right after the previous transaction (see Service). The service
// must count these aborts with reason "other", not "rebalance", go on, and
// commit every record once. Whether an abort happens depends on timing, so
// the count is logged, not required.
func testNoOutputOnBroker(t *testing.T, brokers []string, redisCtr testcontainers.Container) {
	cfg, rdb := setup(t, brokers, redisCtr, "nooutput")
	cfg.MaxBatchRecords = 5
	recs := make([]*kgo.Record, 200)
	for i := range recs {
		rkey := fmt.Sprintf("nooutput%04d", i)
		recs[i] = rawRecord(testBob, commitValue(t, testBob, "create", "app.bsky.feed.post", rkey, replyRecord("", "")),
			commitID(testBob, "create", rkey), "commit", testWitnessed)
	}
	produceRaw(t, brokers, recs...)
	total := rawCount(t, brokers)

	r := startService(t, cfg, rdb, nil)
	r.waitCommitted(t, total)
	if err := r.stop(t); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	m := r.svc.metrics
	assertIdentity(t, m)
	if got := countValue(m.rawRecords); got != float64(total) {
		t.Errorf("raw_records_total = %v, want %d", got, total)
	}
	for _, reason := range []string{abortRebalance, abortError} {
		if got := countValue(m.aborts.WithLabelValues(reason)); got != 0 {
			t.Errorf("aborted transactions{reason=%s} = %v, want 0", reason, got)
		}
	}
	t.Logf("transactions: %v committed, %v aborted with reason other",
		countValue(m.transactions.WithLabelValues(resultCommitted)), countValue(m.aborts.WithLabelValues(abortOther)))
}

func testAbortMidBatchOnBroker(t *testing.T, brokers []string, redisCtr testcontainers.Container) {
	cfg, rdb := setup(t, brokers, redisCtr, "abort")
	recs, ids := replies(t, "abort", 60)
	produceRaw(t, brokers, recs...)

	// The first batch produces its outputs, then fails before the end of
	// its transaction.
	var abortedIDs []string
	r := startService(t, cfg, rdb, func(svc *Service) {
		svc.beforeEnd = func(_ context.Context, batch []*kgo.Record) error {
			for _, rec := range batch {
				abortedIDs = append(abortedIDs, headerOf(rec, bus.HeaderEventID))
			}
			return errInjected
		}
	})
	if err := r.wait(t); !errors.Is(err, errInjected) {
		t.Fatalf("Run() error = %v, want the injected error", err)
	}
	t.Logf("aborted a batch of %d records", len(abortedIDs))

	// The aborted records are in the topic: read uncommitted sees them.
	if got := countIDs(t, brokers, kgo.ReadUncommitted(), abortedIDs, 1, time.Second); len(got) == 0 {
		t.Fatal("read uncommitted sees no aborted record")
	}
	// Read committed sees none of them.
	if got := countIDs(t, brokers, kgo.ReadCommitted(), ids, 0, 3*time.Second); len(got) != 0 {
		t.Errorf("read committed sees %d events after the abort, want 0", len(got))
	}
	if got := groupOffsetsOf(t, brokers, cfg.GroupID); len(got) != 0 {
		t.Errorf("offsets committed after the abort: %v", got)
	}
	// The dedup keys of the aborted batch are set, so the restart goes
	// through the same-position path.
	for _, id := range abortedIDs {
		if n, err := rdb.Exists(testContext(t), DedupKey(id)).Result(); err != nil || n != 1 {
			t.Errorf("dedup key of %q: exists %d, error %v", id, n, err)
		}
	}

	r2 := startService(t, cfg, rdb, nil)
	r2.waitCommitted(t, rawCount(t, brokers))
	if err := r2.stop(t); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	assertOnce(t, countIDs(t, brokers, kgo.ReadCommitted(), ids, len(ids), 3*time.Second), ids)
	if got := countValue(r2.svc.metrics.reprocessed); got < float64(len(abortedIDs)) {
		t.Errorf("reprocessed_total = %v, want at least %d", got, len(abortedIDs))
	}
	assertIdentity(t, r2.svc.metrics)
}

func testSameEventOnBroker(t *testing.T, brokers []string, redisCtr testcontainers.Container) {
	cfg, rdb := setup(t, brokers, redisCtr, "dup")
	recs, ids := replies(t, "dup", 1)
	copyOf := func() *kgo.Record {
		c := *recs[0]
		return &c
	}
	produceRaw(t, brokers, copyOf(), copyOf())

	r := startService(t, cfg, rdb, nil)
	r.waitCommitted(t, rawCount(t, brokers))
	// A third copy after the first copies were committed.
	produceRaw(t, brokers, copyOf())
	r.waitCommitted(t, rawCount(t, brokers))
	if err := r.stop(t); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertOnce(t, countIDs(t, brokers, kgo.ReadCommitted(), ids, 1, 3*time.Second), ids)
	if got := countValue(r.svc.metrics.duplicates); got != 2 {
		t.Errorf("duplicates_total = %v, want 2", got)
	}
	assertIdentity(t, r.svc.metrics)
}

func testRedisStoppedOnBroker(t *testing.T, brokers []string, redisCtr testcontainers.Container) {
	cfg, rdb := setup(t, brokers, redisCtr, "redis")
	recs, ids := replies(t, "redis", 30)
	produceRaw(t, brokers, recs...)

	stopTimeout := 10 * time.Second
	if err := redisCtr.Stop(t.Context(), &stopTimeout); err != nil {
		t.Fatalf("stop redis: %v", err)
	}
	r := startService(t, cfg, rdb, nil)
	err := r.wait(t)
	if err == nil || !strings.Contains(err.Error(), "dedup") {
		t.Fatalf("Run() error = %v, want a dedup error", err)
	}
	t.Logf("with Redis stopped: %v", err)
	if got := countValue(r.svc.metrics.rawRecords); got != 0 {
		t.Errorf("raw_records_total = %v with Redis stopped, want 0", got)
	}
	if got := groupOffsetsOf(t, brokers, cfg.GroupID); len(got) != 0 {
		t.Errorf("offsets committed with Redis stopped: %v", got)
	}
	if got := countIDs(t, brokers, kgo.ReadCommitted(), ids, 0, 3*time.Second); len(got) != 0 {
		t.Errorf("read committed sees %d events with Redis stopped, want 0", len(got))
	}

	if err := redisCtr.Start(t.Context()); err != nil {
		t.Fatalf("start redis again: %v", err)
	}
	cfg.RedisAddr = redisAddr(t, redisCtr)
	rdb2 := NewRedisClient(cfg)
	t.Cleanup(func() { _ = rdb2.Close() })
	deadline := time.Now().Add(30 * time.Second)
	for rdb2.Ping(testContext(t)).Err() != nil {
		if time.Now().After(deadline) {
			t.Fatal("redis did not come back")
		}
		time.Sleep(100 * time.Millisecond)
	}

	r2 := startService(t, cfg, rdb2, nil)
	r2.waitCommitted(t, rawCount(t, brokers))
	if err := r2.stop(t); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	assertOnce(t, countIDs(t, brokers, kgo.ReadCommitted(), ids, len(ids), 3*time.Second), ids)
	assertIdentity(t, r2.svc.metrics)
}
