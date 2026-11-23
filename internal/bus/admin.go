package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TopicAdmin is the part of *kadm.Client that EnsureTopics uses.
type TopicAdmin interface {
	CreateTopic(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topic string) (kadm.CreateTopicResponse, error)
	UpdatePartitions(ctx context.Context, set int, topics ...string) (kadm.CreatePartitionsResponses, error)
	DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
	AlterTopicConfigs(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error)
}

// EnsureTopics makes every topic match its spec. It is safe to call at every
// service start and from several services at the same time.
//
//   - Each topic is created with the partitions, replication factor and
//     configs of its spec. "Topic already exists" is the signal to compare
//     the existing topic with the spec.
//   - A topic with fewer partitions than its spec is raised to the spec
//     count. This changes which partition a key maps to, so it is logged as
//     a warning.
//   - A topic with more partitions than its spec is an error, because the
//     count cannot be lowered.
//   - Each config in the spec is set to the spec value if it differs.
//     Configs that the spec does not mention are left alone.
//   - A different replication factor is logged as a warning and not changed.
//   - At the end it waits up to LeaderWait until every partition of every
//     ensured topic has a leader. A new topic or a new partition can have
//     no leader for a short time, and a client that uses it at once gets
//     UNKNOWN_TOPIC_OR_PARTITION or LEADER_NOT_AVAILABLE.
//
// The returned error names every topic that failed.
func EnsureTopics(ctx context.Context, client *kgo.Client, logger *slog.Logger, specs ...TopicSpec) error {
	return ensureTopics(ctx, kadm.NewClient(client), client, logger, specs...)
}

// ensureTopics is EnsureTopics with its dependencies as interfaces.
//
// Partition counts come from a metadata request sent with meta (a
// *kgo.Client), which kgo documents as a request issued to Kafka. kadm's
// ListTopics is not used, because kadm answers it from kgo's metadata cache
// (kgo.Client.RequestCachedMetadata, up to MetadataMinAge old), so a topic
// that another service just created or changed could look missing or
// unchanged. Configs come from DescribeConfigs, which is never cached.
func ensureTopics(ctx context.Context, admin TopicAdmin, meta kmsg.Requestor, logger *slog.Logger, specs ...TopicSpec) error {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	var errs []error
	for _, spec := range specs {
		if err := ensureTopic(ctx, admin, meta, logger, spec); err != nil {
			errs = append(errs, fmt.Errorf("topic %s: %w", spec.Name, err))
			continue
		}
		if err := waitForLeaders(ctx, meta, spec); err != nil {
			errs = append(errs, fmt.Errorf("topic %s: %w", spec.Name, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("ensure topics: %w", err)
	}
	return nil
}

func ensureTopic(ctx context.Context, admin TopicAdmin, meta kmsg.Requestor, logger *slog.Logger, spec TopicSpec) error {
	if spec.Name == "" || spec.Partitions < 1 || spec.ReplicationFactor < 1 {
		return fmt.Errorf("invalid spec: %d partitions, replication factor %d", spec.Partitions, spec.ReplicationFactor)
	}

	resp, err := admin.CreateTopic(ctx, spec.Partitions, spec.ReplicationFactor, spec.Configs, spec.Name)
	if err == nil {
		logger.InfoContext(ctx, "topic created", "topic", spec.Name, "partitions", spec.Partitions)
		return nil
	}
	if !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create: %w%s", err, errMessage(resp.ErrMessage))
	}

	// The topic exists. Compare it with the spec once metadata lists it.
	layout, err := waitForTopic(ctx, meta, spec.Name, metadataWait, func(topicLayout) bool { return true })
	if err != nil {
		return fmt.Errorf("create reported that the topic exists: %w", err)
	}
	var errs []error
	if err := ensurePartitions(ctx, admin, meta, logger, spec, layout.partitions); err != nil {
		errs = append(errs, err)
	}
	if layout.replicas != int(spec.ReplicationFactor) {
		logger.WarnContext(ctx, "topic replication factor differs from the spec and is not changed",
			"topic", spec.Name, "replication_factor", layout.replicas, "spec_replication_factor", spec.ReplicationFactor)
	}
	if err := ensureConfigs(ctx, admin, logger, spec); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// topicLayout is the partition count and replication factor of a topic,
// and how many of its partitions have no leader.
type topicLayout struct {
	partitions int32
	replicas   int
	leaderless int
}

// describeTopic returns the topic layout and whether the topic exists, from
// a metadata request sent straight to a broker.
func describeTopic(ctx context.Context, meta kmsg.Requestor, topic string) (topicLayout, bool, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, meta)
	if err != nil {
		return topicLayout{}, false, fmt.Errorf("metadata: %w", err)
	}
	for _, t := range resp.Topics {
		if t.Topic == nil || *t.Topic != topic {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				return topicLayout{}, false, nil
			}
			return topicLayout{}, false, fmt.Errorf("metadata: %w", err)
		}
		l := topicLayout{partitions: int32(len(t.Partitions))}
		if len(t.Partitions) > 0 {
			l.replicas = len(t.Partitions[0].Replicas)
		}
		for _, p := range t.Partitions {
			if p.Leader < 0 || p.ErrorCode != 0 {
				l.leaderless++
			}
		}
		return l, true, nil
	}
	return topicLayout{}, false, nil
}

// Metadata can lag behind a change made by another client, because a
// broker spreads it asynchronously. waitForTopic asks again until ok returns
// true or the wait ends.
const (
	metadataWait     = 5 * time.Second
	metadataInterval = 250 * time.Millisecond
)

// LeaderWait bounds the wait for partition leaders at the end of
// EnsureTopics.
const LeaderWait = 30 * time.Second

func waitForTopic(ctx context.Context, meta kmsg.Requestor, topic string, wait time.Duration, ok func(topicLayout) bool) (topicLayout, error) {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	ticker := time.NewTicker(metadataInterval)
	defer ticker.Stop()
	for {
		layout, exists, err := describeTopic(ctx, meta, topic)
		if err == nil && exists && ok(layout) {
			return layout, nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return topicLayout{}, err
			}
			return topicLayout{}, fmt.Errorf("metadata did not show the expected topic state within %s: %w", wait, ctx.Err())
		case <-ticker.C:
		}
	}
}

// waitForLeaders waits until metadata shows the spec partition count and a
// leader for every partition, or until LeaderWait or ctx ends.
func waitForLeaders(ctx context.Context, meta kmsg.Requestor, spec TopicSpec) error {
	var last topicLayout
	_, err := waitForTopic(ctx, meta, spec.Name, LeaderWait, func(l topicLayout) bool {
		last = l
		return l.partitions >= spec.Partitions && l.leaderless == 0
	})
	if err != nil {
		return fmt.Errorf("wait for partition leaders: %d of %d partitions have no leader: %w", last.leaderless, last.partitions, err)
	}
	return nil
}

func ensurePartitions(ctx context.Context, admin TopicAdmin, meta kmsg.Requestor, logger *slog.Logger, spec TopicSpec, have int32) error {
	switch {
	case have == spec.Partitions:
		return nil
	case have > spec.Partitions:
		return fmt.Errorf("has %d partitions, more than the spec %d, and the count cannot be lowered", have, spec.Partitions)
	}

	resps, err := admin.UpdatePartitions(ctx, int(spec.Partitions), spec.Name)
	if err == nil {
		var resp kadm.CreatePartitionsResponse
		resp, err = resps.On(spec.Name, nil)
		if err == nil && resp.Err != nil {
			err = fmt.Errorf("%w%s", resp.Err, errMessage(resp.ErrMessage))
		}
	}
	if err != nil {
		// Another service may have raised the count at the same time. Then
		// the broker rejects our request, but the topic is as wanted.
		if _, werr := waitForTopic(ctx, meta, spec.Name, metadataWait, func(l topicLayout) bool {
			return l.partitions == spec.Partitions
		}); werr == nil {
			return nil
		}
		return fmt.Errorf("raise partitions from %d to %d: %w", have, spec.Partitions, err)
	}
	logger.WarnContext(ctx, "topic partition count raised, keys now map to different partitions",
		"topic", spec.Name, "from", have, "to", spec.Partitions)
	return nil
}

func ensureConfigs(ctx context.Context, admin TopicAdmin, logger *slog.Logger, spec TopicSpec) error {
	if len(spec.Configs) == 0 {
		return nil
	}
	rcs, err := admin.DescribeTopicConfigs(ctx, spec.Name)
	if err != nil {
		return fmt.Errorf("describe configs: %w", err)
	}
	rc, err := rcs.On(spec.Name, nil)
	if err != nil {
		return fmt.Errorf("describe configs: %w", err)
	}
	if rc.Err != nil {
		return fmt.Errorf("describe configs: %w%s", rc.Err, errMessage(rc.ErrMessage))
	}
	current := make(map[string]string, len(rc.Configs))
	for _, c := range rc.Configs {
		current[c.Key] = c.MaybeValue()
	}

	var alter []kadm.AlterConfig
	for _, key := range slices.Sorted(maps.Keys(spec.Configs)) {
		want := spec.Configs[key]
		if want == nil {
			continue
		}
		if got, ok := current[key]; ok && got == *want {
			continue
		}
		alter = append(alter, kadm.AlterConfig{Op: kadm.SetConfig, Name: key, Value: want})
		logger.InfoContext(ctx, "topic config differs from the spec, setting it",
			"topic", spec.Name, "config", key, "from", current[key], "to", *want)
	}
	if len(alter) == 0 {
		return nil
	}

	resps, err := admin.AlterTopicConfigs(ctx, alter, spec.Name)
	if err != nil {
		return fmt.Errorf("alter configs: %w", err)
	}
	resp, err := resps.On(spec.Name, nil)
	if err != nil {
		return fmt.Errorf("alter configs: %w", err)
	}
	if resp.Err != nil {
		return fmt.Errorf("alter configs: %w%s", resp.Err, errMessage(resp.ErrMessage))
	}
	return nil
}

func errMessage(msg string) string {
	if msg == "" {
		return ""
	}
	return ": " + msg
}
