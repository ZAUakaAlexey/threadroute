package bus

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
)

// countingAdmin wraps a TopicAdmin and counts the calls that change topics.
// A create that fails (for example because the topic exists) is not counted.
type countingAdmin struct {
	TopicAdmin
	creates, partitions, alters atomic.Int32
}

func (a *countingAdmin) CreateTopic(ctx context.Context, partitions int32, rf int16, configs map[string]*string, topic string) (kadm.CreateTopicResponse, error) {
	resp, err := a.TopicAdmin.CreateTopic(ctx, partitions, rf, configs, topic)
	if err == nil {
		a.creates.Add(1)
	}
	return resp, err
}

func (a *countingAdmin) UpdatePartitions(ctx context.Context, set int, topics ...string) (kadm.CreatePartitionsResponses, error) {
	a.partitions.Add(1)
	return a.TopicAdmin.UpdatePartitions(ctx, set, topics...)
}

func (a *countingAdmin) AlterTopicConfigs(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
	a.alters.Add(1)
	return a.TopicAdmin.AlterTopicConfigs(ctx, configs, topics...)
}

// assertTopicMatchesSpec checks the topic with a new admin client, so no
// cached metadata hides a change.
func assertTopicMatchesSpec(t *testing.T, c *kfake.Cluster, spec TopicSpec) {
	t.Helper()
	ctx := testContext(t)
	adm := newTestAdmin(t, c)
	details, err := adm.ListTopics(ctx, spec.Name)
	if err != nil {
		t.Fatalf("ListTopics() error = %v", err)
	}
	d := details[spec.Name]
	if d.Err != nil {
		t.Fatalf("topic %s: %v", spec.Name, d.Err)
	}
	if got := int32(len(d.Partitions)); got != spec.Partitions {
		t.Errorf("topic %s has %d partitions, want %d", spec.Name, got, spec.Partitions)
	}
	if got := d.Partitions.NumReplicas(); got != int(spec.ReplicationFactor) {
		t.Errorf("topic %s has replication factor %d, want %d", spec.Name, got, spec.ReplicationFactor)
	}
	got := describeConfigs(t, adm, spec.Name)
	for k, v := range spec.Configs {
		if got[k] != *v {
			t.Errorf("topic %s config %s = %q, want %q", spec.Name, k, got[k], *v)
		}
	}
}

func describeConfigs(t *testing.T, adm *kadm.Client, topic string) map[string]string {
	t.Helper()
	rcs, err := adm.DescribeTopicConfigs(testContext(t), topic)
	if err != nil {
		t.Fatalf("DescribeTopicConfigs() error = %v", err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil || rc.Err != nil {
		t.Fatalf("DescribeTopicConfigs(%s) error = %v, %v", topic, err, rc.Err)
	}
	out := map[string]string{}
	for _, c := range rc.Configs {
		out[c.Key] = c.MaybeValue()
	}
	return out
}

func TestEnsureTopicsCreatesAndIsIdempotent(t *testing.T) {
	c := newTestCluster(t)
	cl := newTestClient(t, c)
	specs := testSpecs(t)
	ctx := testContext(t)

	first := &countingAdmin{TopicAdmin: kadm.NewClient(cl)}
	if err := ensureTopics(ctx, first, cl, discardLogger(), specs...); err != nil {
		t.Fatalf("first EnsureTopics() error = %v", err)
	}
	if got := first.creates.Load(); got != 3 {
		t.Errorf("first call created %d topics, want 3", got)
	}
	for _, s := range specs {
		assertTopicMatchesSpec(t, c, s)
	}

	// The same client: its metadata cache still says the topics are
	// missing, which must not matter.
	second := &countingAdmin{TopicAdmin: kadm.NewClient(cl)}
	if err := ensureTopics(ctx, second, cl, discardLogger(), specs...); err != nil {
		t.Fatalf("second EnsureTopics() error = %v", err)
	}
	if n, p, a := second.creates.Load(), second.partitions.Load(), second.alters.Load(); n+p+a != 0 {
		t.Errorf("second call made changes: %d creates, %d partition updates, %d config alters", n, p, a)
	}
}

func TestEnsureTopicsConcurrent(t *testing.T) {
	c := newTestCluster(t)
	specs := testSpecs(t)
	ctx := testContext(t)

	const callers = 4
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		cl := newTestClient(t, c)
		wg.Go(func() {
			errs[i] = EnsureTopics(ctx, cl, discardLogger(), specs...)
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: EnsureTopics() error = %v", i, err)
		}
	}
	for _, s := range specs {
		assertTopicMatchesSpec(t, c, s)
	}
}

func TestEnsureTopicsPartitions(t *testing.T) {
	tests := []struct {
		name     string
		existing int32
		wantErr  bool
	}{
		{name: "lower count is raised", existing: 3},
		{name: "equal count is kept", existing: PartitionsRaw},
		{name: "higher count is an error", existing: PartitionsRaw + 2, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCluster(t)
			if err := c.CreateTopic(TopicRaw, tt.existing, nil); err != nil {
				t.Fatalf("CreateTopic() error = %v", err)
			}
			spec := testSpecs(t)[0]
			logs := &bufferLogger{}

			err := EnsureTopics(testContext(t), newTestClient(t, c), logs.logger(), spec)
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), TopicRaw) {
					t.Fatalf("EnsureTopics() error = %v, want an error naming %s", err, TopicRaw)
				}
				return
			}
			if err != nil {
				t.Fatalf("EnsureTopics() error = %v", err)
			}
			assertTopicMatchesSpec(t, c, spec)
			raised := strings.Contains(logs.String(), "partition count raised")
			if raised != (tt.existing < spec.Partitions) {
				t.Errorf("warning logged = %v, want %v; logs:\n%s", raised, tt.existing < spec.Partitions, logs.String())
			}
		})
	}
}

func TestEnsureTopicsRestoresConfigs(t *testing.T) {
	c := newTestCluster(t)
	adm := newTestAdmin(t, c)
	spec := testSpecs(t)[0]
	ctx := testContext(t)
	cl := newTestClient(t, c)
	if err := EnsureTopics(ctx, cl, discardLogger(), spec); err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}

	// Change one config from the spec and set one the spec does not have.
	resps, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: ConfigRetentionMs, Value: kadm.StringPtr("1000")},
		{Op: kadm.SetConfig, Name: "max.message.bytes", Value: kadm.StringPtr("2097152")},
	}, TopicRaw)
	if err == nil {
		for _, r := range resps {
			if r.Err != nil {
				err = r.Err
			}
		}
	}
	if err != nil {
		t.Fatalf("AlterTopicConfigs() error = %v", err)
	}

	counting := &countingAdmin{TopicAdmin: adm}
	if err := ensureTopics(ctx, counting, cl, discardLogger(), spec); err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}
	if got := counting.alters.Load(); got != 1 {
		t.Errorf("alter calls = %d, want 1", got)
	}
	assertTopicMatchesSpec(t, c, spec)
	if got := describeConfigs(t, adm, TopicRaw)["max.message.bytes"]; got != "2097152" {
		t.Errorf("config not in the spec was changed: max.message.bytes = %q, want %q", got, "2097152")
	}
}

func TestEnsureTopicsNamesEveryFailedTopic(t *testing.T) {
	c := newTestCluster(t)
	for _, topic := range []string{TopicRaw, TopicCursor} {
		if err := c.CreateTopic(topic, 20, nil); err != nil {
			t.Fatalf("CreateTopic(%s) error = %v", topic, err)
		}
	}
	err := EnsureTopics(testContext(t), newTestClient(t, c), discardLogger(), testSpecs(t)...)
	if err == nil {
		t.Fatal("EnsureTopics() error = nil, want error")
	}
	for _, topic := range []string{TopicRaw, TopicCursor} {
		if !strings.Contains(err.Error(), "topic "+topic+":") {
			t.Errorf("error %q does not name %s", err, topic)
		}
	}
	if strings.Contains(err.Error(), "topic "+TopicEvents+":") {
		t.Errorf("error %q names %s, which did not fail", err, TopicEvents)
	}
}

func TestEnsureTopicsInvalidSpec(t *testing.T) {
	c := newTestCluster(t)
	err := EnsureTopics(testContext(t), newTestClient(t, c), discardLogger(), TopicSpec{Name: "synthetic.topic"})
	if err == nil || !strings.Contains(err.Error(), "synthetic.topic") {
		t.Errorf("EnsureTopics() error = %v, want an error naming synthetic.topic", err)
	}
}

// TestEnsureTopicsUpgradesCursorConfigs covers a cursor topic created by an
// older version, with only cleanup.policy set: the next start must bring
// it to the current spec.
func TestEnsureTopicsUpgradesCursorConfigs(t *testing.T) {
	c := newTestCluster(t)
	if err := c.CreateTopic(TopicCursor, PartitionsCursor, map[string]string{ConfigCleanupPolicy: CleanupCompact}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	spec := CursorTopicSpec()
	ctx := testContext(t)
	cl := newTestClient(t, c)

	first := &countingAdmin{TopicAdmin: kadm.NewClient(cl)}
	if err := ensureTopics(ctx, first, cl, discardLogger(), spec); err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}
	if got := first.alters.Load(); got != 1 {
		t.Errorf("alter calls = %d, want 1", got)
	}
	assertTopicMatchesSpec(t, c, spec)

	second := &countingAdmin{TopicAdmin: kadm.NewClient(cl)}
	if err := ensureTopics(ctx, second, cl, discardLogger(), spec); err != nil {
		t.Fatalf("second EnsureTopics() error = %v", err)
	}
	if got := second.alters.Load(); got != 0 {
		t.Errorf("second call alter calls = %d, want 0", got)
	}
}
