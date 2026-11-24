package routersvc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// topicInterval is the pause between two checks for the input topic.
const topicInterval = time.Second

// waitForTopic checks with metadata requests until the topic exists, for up
// to wait. A broker that does not answer is retried the same way, so this is
// also the wait for Kafka at start. The router does not create the
// conversations topic: conv does.
//
// meta is usually a *kgo.Client. The request goes to a broker directly and
// is not answered from the metadata cache of the client.
func waitForTopic(ctx context.Context, meta kmsg.Requestor, topic string, wait time.Duration, logger *slog.Logger) error {
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	var lastErr error
	for attempt := 1; ; attempt++ {
		// One check may not take the whole wait: the client can hold a
		// request while no broker answers.
		actx, acancel := context.WithTimeout(wctx, topicInterval)
		exists, err := topicExists(actx, meta, topic)
		acancel()
		if err == nil && exists {
			return nil
		}
		if err != nil && wctx.Err() == nil {
			lastErr = err
		}
		if attempt == 1 {
			logger.Warn("waiting for topic, it is created by conv", "topic", topic, "wait", wait.String(), "error", err)
		}
		select {
		case <-wctx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if lastErr != nil {
				return fmt.Errorf("topic %s not found within %s, it is created by conv: %w", topic, wait, lastErr)
			}
			return fmt.Errorf("topic %s not found within %s, it is created by conv", topic, wait)
		case <-time.After(topicInterval):
		}
	}
}

// topicExists asks a broker directly whether the topic exists and has
// partitions.
func topicExists(ctx context.Context, meta kmsg.Requestor, topic string) (bool, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, meta)
	if err != nil {
		return false, fmt.Errorf("metadata: %w", err)
	}
	for _, t := range resp.Topics {
		if t.Topic == nil || *t.Topic != topic {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.LeaderNotAvailable) {
				return false, nil
			}
			return false, fmt.Errorf("metadata: %w", err)
		}
		return len(t.Partitions) > 0, nil
	}
	return false, nil
}
