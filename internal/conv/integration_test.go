//go:build integration

package conv

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// kafkaImage is the same broker as in deploy/compose.yaml.
const kafkaImage = "apache/kafka:4.4.0"

// redisImage is the official Redis image.
const redisImage = "redis:7-alpine"

const (
	kafkaHostPort  = "19092/tcp"
	advertisedFile = "/tmp/advertised-listeners"
)

// startKafka starts one Apache Kafka node in KRaft combined mode and
// returns its bootstrap address. It is the same setup as in
// internal/bus/integration_test.go, which explains why the Testcontainers
// Kafka module is not used.
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

func startRedis(t *testing.T) string {
	t.Helper()
	ctx := t.Context()
	ctr, err := testcontainers.Run(ctx, redisImage,
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(wait.ForLog("Ready to accept connections").WithStartupTimeout(time.Minute)),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start %s: %v", redisImage, err)
	}
	addr, err := ctr.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		t.Fatalf("redis endpoint: %v", err)
	}
	return addr
}

func TestIntegration(t *testing.T) {
	redisAddr := startRedis(t)
	brokers := []string{startKafka(t)}

	cl := newKafkaClient(t, brokers)
	if err := bus.EnsureTopics(testCtx(t), cl, discardLogger(),
		bus.EventsTopicSpec(), bus.ControlTopicSpec(), bus.ConversationsTopicSpec()); err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}

	// (2) The Lua scripts on real Redis, including redelivery, which must
	// change no counter and no admission set, and exact 16-digit
	// microsecond times (t0 has a microsecond part).
	t.Run("ScriptsOnRealRedis", func(t *testing.T) {
		realRedisAddr = redisAddr
		defer func() { realRedisAddr = "" }()
		for name, test := range map[string]func(*testing.T){
			"Admission":                TestAdmission,
			"AdmissionEndsAfterIdle":   TestAdmissionEndsAfterIdle,
			"CompanyLimit":             TestCompanyLimit,
			"OpenAddAnswer":            TestOpenAddAnswer,
			"CloseAtDeadline":          TestCloseAtDeadline,
			"ReopenContinuesSequence":  TestReopenContinuesSequence,
			"MessagesKeptLimit":        TestMessagesKeptLimit,
			"LateMessage":              TestLateMessageInsertedInSourceOrder,
			"Watermark":                TestWatermark,
			"IdempotentRedelivery":     TestIdempotentRedelivery,
			"DeletionAfterMessage":     TestDeletionAfterMessage,
			"DeletionBeforeMessage":    TestDeletionBeforeMessage,
			"ControlOrderSameState":    TestControlOrderSameState,
			"CustomerRemoval":          TestCustomerRemoval,
			"CompanyRemoval":           TestCompanyRemoval,
			"SessionRecordForgotten":   TestSessionRecordForgottenAfter24Hours,
			"SameMessageNewEventID":    TestSameMessageUnderNewEventID,
			"CompanyMessageNoOpenConv": TestCompanyMessageWithNoOpenConversation,
			"EditOfStoredMessage":      TestEditOfStoredMessage,
			"EditDeliveredTwice":       TestEditDeliveredTwice,
			"EditOfUnknownMessage":     TestEditOfUnknownMessage,
			"EditAfterDeletion":        TestEditAfterDeletion,
			"EditOfClosedConversation": TestEditOfClosedConversationWithKeptText,
		} {
			t.Run(name, test)
		}
	})

	// (1) conv never applies a record of an aborted transaction.
	t.Run("AbortedTransactionNotSeen", func(t *testing.T) {
		env := newRealEnv(t, redisAddr)
		ctx := testCtx(t)
		aborted := customerMsg(testCompany, testAlice, "test:aborted", 0)
		// Same conversation, so both records are in one partition and the
		// aborted one comes first.
		committed := customerMsg(testCompany, testAlice, "test:committed", time.Second)
		tx := newKafkaClient(t, brokers, kgo.TransactionalID("test-normalize-it"))
		for _, step := range []struct {
			m   event.Message
			end kgo.TransactionEndTry
		}{{aborted, kgo.TryAbort}, {committed, kgo.TryCommit}} {
			if err := tx.BeginTransaction(); err != nil {
				t.Fatal(err)
			}
			if err := tx.ProduceSync(ctx, messageRecord(t, step.m)).FirstErr(); err != nil {
				t.Fatal(err)
			}
			if err := tx.EndTransaction(ctx, step.end); err != nil {
				t.Fatal(err)
			}
		}
		pub := newPublisher(t, brokers)
		svc := startServiceGroup(t, brokers, "it-aborted", env.store, pub)
		waitFor(t, "the committed record", func() bool { return env.applied(t, committed.EventID) })
		if err := svc.stop(); err != nil {
			t.Fatal(err)
		}
		if env.applied(t, aborted.EventID) {
			t.Error("a record of an aborted transaction was applied")
		}
	})

	// (3) A deletion that arrives before its message is still applied,
	// end to end through both topics.
	t.Run("DeletionBeforeMessage", func(t *testing.T) {
		env := newRealEnv(t, redisAddr)
		// Another company than the outbox test below, so the two do not
		// share a conversation key in the conversations topic.
		env.admit(t, testCompany2, 0)
		pub := newPublisher(t, brokers)
		svc := startServiceGroup(t, brokers, "it-deletion", env.store, pub)

		del := deletion("test:early", testAlice, 2*time.Minute)
		b, err := event.MarshalControl(del)
		if err != nil {
			t.Fatal(err)
		}
		producer := newKafkaClient(t, brokers)
		if err := producer.ProduceSync(testCtx(t), &kgo.Record{
			Topic: bus.TopicControl, Key: event.AuthorKey(testAlice), Value: b,
		}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the deletion", func() bool { return env.applied(t, del.EventID) })

		msg := customerMsg(testCompany2, testAlice, "test:early", time.Minute)
		produceMessages(t, brokers, msg)
		waitFor(t, "the message", func() bool { return env.applied(t, msg.EventID) })
		if err := svc.stop(); err != nil {
			t.Fatal(err)
		}
		if env.hasText(t, "test:early") {
			t.Error("text of a deleted message is stored")
		}
		if c := env.conv(t, testCompany2, testAlice); c["nc"] != "1" || c["st"] != "o" {
			t.Errorf("conversation = %v, want open with one customer message", c)
		}
	})

	// (4) The outbox is drained after a restart.
	t.Run("OutboxAfterRestart", func(t *testing.T) {
		env := newRealEnv(t, redisAddr)
		env.admit(t, testCompany, 0)
		input := conversationInput()
		produceMessages(t, brokers, input...)

		first := startServiceGroup(t, brokers, "it-outbox", env.store, &failingPublisher{})
		select {
		case err := <-first.done:
			close(first.done)
			if err == nil {
				t.Fatal("first Run() = nil, want the publish error")
			}
		case <-time.After(time.Minute):
			t.Fatal("first run did not fail")
		}
		st, err := env.store.Stats(testCtx(t), bus.PartitionsEvents)
		if err != nil || st.OutboxPending == 0 {
			t.Fatalf("outbox after the failed publish: %+v, %v", st, err)
		}

		second := startServiceGroup(t, brokers, "it-outbox", env.store, newPublisher(t, brokers))
		waitDone(t, env, input)
		if err := second.stop(); err != nil {
			t.Fatal(err)
		}
		perConv := dedupe(readConversations(t, brokers, 1))
		assertAllPublished(t, env, perConv)
		for who, want := range map[string]int{testAlice: 4, testBob: 3} {
			list := perConv[convKey(testCompany, who)]
			if len(list) < want {
				t.Errorf("%s: %d events, want at least %d", who, len(list), want)
			}
			for i, ev := range list {
				if ev.Seq != uint64(i+1) {
					t.Errorf("%s: event %d has seq %d", who, i, ev.Seq)
				}
			}
		}
	})
}

func newRealEnv(t *testing.T, addr string) *testEnv {
	t.Helper()
	realRedisAddr = addr
	defer func() { realRedisAddr = "" }()
	return newTestEnv(t, testRules())
}

func (e *testEnv) applied(t *testing.T, eventID string) bool {
	t.Helper()
	n, err := e.rdb.Exists(testCtx(t), DefaultKeyPrefix+"ap:"+eventID).Result()
	if err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func newPublisher(t *testing.T, brokers []string) *KafkaPublisher {
	t.Helper()
	pub, err := NewKafkaPublisher(brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	return pub
}
