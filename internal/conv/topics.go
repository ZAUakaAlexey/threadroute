package conv

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// topicPollInterval is the pause between two checks of WaitForTopics.
const topicPollInterval = time.Second

// WaitForTopics waits until every topic exists, for at most wait. conv does
// not create its input topics: normalize does. If conv starts first it waits
// a bounded time, then fails with an error that names the missing topics, so
// a wrong deployment order is visible at once.
//
// meta is usually a *kgo.Client. The metadata request goes to a broker
// directly and is not answered from the client's cache.
func WaitForTopics(ctx context.Context, meta kmsg.Requestor, wait time.Duration, topics ...string) error {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	ticker := time.NewTicker(topicPollInterval)
	defer ticker.Stop()
	// Until a check succeeds, every topic counts as missing.
	missing := slices.Sorted(slices.Values(topics))
	var lastErr error
	for {
		// One check may not take the whole wait: the client can hold a
		// request for a topic it does not know.
		actx, acancel := context.WithTimeout(ctx, topicPollInterval)
		m, err := missingTopics(actx, meta, topics)
		acancel()
		if err == nil {
			if len(m) == 0 {
				return nil
			}
			missing, lastErr = m, nil
		} else if ctx.Err() == nil {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if err := ctx.Err(); errors.Is(err, context.Canceled) {
				return err
			}
			if lastErr != nil {
				return fmt.Errorf("input topics %v not found after %s; they are created by normalize: %w", missing, wait, lastErr)
			}
			return fmt.Errorf("input topics %v do not exist after %s; they are created by normalize", missing, wait)
		case <-ticker.C:
		}
	}
}

func missingTopics(ctx context.Context, meta kmsg.Requestor, topics []string) ([]string, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	for _, t := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, meta)
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}
	found := map[string]bool{}
	for _, t := range resp.Topics {
		if t.Topic == nil {
			continue
		}
		err := kerr.ErrorForCode(t.ErrorCode)
		switch {
		case err == nil && len(t.Partitions) > 0:
			found[*t.Topic] = true
		case err == nil, errors.Is(err, kerr.UnknownTopicOrPartition), errors.Is(err, kerr.LeaderNotAvailable):
		default:
			return nil, fmt.Errorf("metadata of %s: %w", *t.Topic, err)
		}
	}
	var missing []string
	for _, t := range topics {
		if !found[t] {
			missing = append(missing, t)
		}
	}
	slices.Sort(missing)
	return missing, nil
}
