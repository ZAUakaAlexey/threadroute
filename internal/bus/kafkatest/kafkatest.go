//go:build integration

// Package kafkatest starts a real Apache Kafka for integration tests
// through Testcontainers. It is built only with the integration build tag.
//
// It is shared by the router service and the simulator, whose integration
// tests run together against the same kind of broker. Each call starts its
// own container on a random host port.
package kafkatest

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Image is the same broker as in deploy/compose.yaml.
const Image = "apache/kafka:4.4.0"

const (
	hostPort       = "19092/tcp"
	advertisedFile = "/tmp/advertised-listeners"
)

// Start starts one Apache Kafka node in KRaft combined mode and returns its
// bootstrap address. Topics are not created automatically. The container is
// removed when the test ends.
//
// The advertised listener needs the mapped host port, which is known only
// after the start, so the entrypoint waits for a file with the listeners
// that a start hook writes.
func Start(t testing.TB) string {
	t.Helper()
	ctx := t.Context()
	ctr, err := testcontainers.Run(ctx, Image,
		testcontainers.WithExposedPorts(hostPort),
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
					if err := wait.ForMappedPort(hostPort).WaitUntilReady(ctx, c); err != nil {
						return err
					}
					endpoint, err := c.PortEndpoint(ctx, hostPort, "HOST")
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
		t.Fatalf("start %s: %v", Image, err)
	}
	addr, err := ctr.PortEndpoint(ctx, hostPort, "")
	if err != nil {
		t.Fatalf("kafka endpoint: %v", err)
	}
	return addr
}
