package bus

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// leaderlessMetadata makes the fake cluster answer the first n metadata
// requests for topic alone with partitions that have no leader, as a real
// broker can do right after a create. n < 0 means every request. It returns
// how many requests were answered that way.
func leaderlessMetadata(t *testing.T, c *kfake.Cluster, topic string, partitions int32, n int) *atomic.Int32 {
	t.Helper()
	host, portStr, err := net.SplitHostPort(c.ListenAddrs()[0])
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	var answered atomic.Int32
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.MetadataRequest)
		if len(req.Topics) != 1 || req.Topics[0].Topic == nil || *req.Topics[0].Topic != topic {
			return nil, nil, false
		}
		if n >= 0 && int(answered.Load()) >= n {
			return nil, nil, false
		}
		answered.Add(1)

		resp := req.ResponseKind().(*kmsg.MetadataResponse)
		b := kmsg.NewMetadataResponseBroker()
		b.NodeID, b.Host, b.Port = 0, host, int32(port)
		resp.Brokers = append(resp.Brokers, b)
		rt := kmsg.NewMetadataResponseTopic()
		rt.Topic = kmsg.StringPtr(topic)
		for p := range partitions {
			rp := kmsg.NewMetadataResponseTopicPartition()
			rp.Partition = p
			rp.Leader = -1
			rp.ErrorCode = kerr.LeaderNotAvailable.Code
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
		return resp, nil, true
	})
	return &answered
}

func TestEnsureTopicsWaitsForLeaders(t *testing.T) {
	c := newTestCluster(t)
	spec := CursorTopicSpec()
	answered := leaderlessMetadata(t, c, spec.Name, spec.Partitions, 3)

	if err := EnsureTopics(testContext(t), newTestClient(t, c), discardLogger(), spec); err != nil {
		t.Fatalf("EnsureTopics() error = %v", err)
	}
	if got := answered.Load(); got != 3 {
		t.Errorf("EnsureTopics returned after %d leaderless answers, want it to wait through 3", got)
	}
}

func TestEnsureTopicsLeaderWaitRespectsContext(t *testing.T) {
	c := newTestCluster(t)
	spec := CursorTopicSpec()
	leaderlessMetadata(t, c, spec.Name, spec.Partitions, -1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	err := EnsureTopics(ctx, newTestClient(t, c), discardLogger(), spec)
	if err == nil || !strings.Contains(err.Error(), "no leader") {
		t.Fatalf("EnsureTopics() error = %v, want a no leader error", err)
	}
	if took := time.Since(start); took > LeaderWait/2 {
		t.Errorf("EnsureTopics took %s, want it to stop with the context", took)
	}
}
