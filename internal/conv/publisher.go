package conv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
)

// Publisher sends conversation events to Kafka and returns after every
// record is confirmed or one failed. Records of one key must be confirmed in
// order, and a failure of one record must fail the later records of the same
// partition.
type Publisher interface {
	Publish(ctx context.Context, records []*kgo.Record) error
}

// DefaultDeliveryTimeout bounds how long a conversation event may wait for
// delivery. A record that times out fails the publish, the outbox keeps it,
// and the Service stops with an error.
const DefaultDeliveryTimeout = 2 * time.Minute

// KafkaPublisher produces to the conversations topic with the settings of
// decision 0001: idempotent writes, acks from all in-sync replicas and zstd.
type KafkaPublisher struct {
	client *kgo.Client
}

// NewKafkaPublisher creates the producer client. extra options are applied
// last, so tests can change them.
func NewKafkaPublisher(brokers []string, extra ...kgo.Opt) (*KafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, errors.New("new publisher: no brokers")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(bus.TopicConversations),
		// Idempotent writes are the franz-go default. They keep the order of
		// the records of one partition across retries.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.RecordDeliveryTimeout(DefaultDeliveryTimeout),
	}
	cl, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("new publisher: %w", err)
	}
	return &KafkaPublisher{client: cl}, nil
}

// Publish implements Publisher. franz-go fails every buffered record of a
// partition when one of them fails, so a later record of a conversation is
// never confirmed while an earlier one failed.
func (p *KafkaPublisher) Publish(ctx context.Context, records []*kgo.Record) error {
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("publish conversation events: %w", err)
	}
	return nil
}

// Close flushes and closes the client.
func (p *KafkaPublisher) Close() {
	p.client.Close()
}
