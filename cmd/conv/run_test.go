package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/conv"
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// freeAddr returns a local address that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func testConfig(t *testing.T, brokers []string, redisAddr string) conv.Config {
	t.Helper()
	cfg, err := conv.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	cfg.KafkaBrokers = brokers
	cfg.RedisAddr = redisAddr
	cfg.MetricsAddr = freeAddr(t)
	cfg.InputTopicWait = 2 * time.Second
	cfg.SweepInterval = 50 * time.Millisecond
	return cfg
}

func TestRunServesAndStops(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1),
		kfake.SeedTopics(bus.PartitionsEvents, bus.TopicEvents),
		kfake.SeedTopics(bus.PartitionsControl, bus.TopicControl))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mr := miniredis.RunT(t)
	cfg := testConfig(t, c.ListenAddrs(), mr.Addr())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, discard(), prometheus.NewRegistry()) }()

	base := "http://" + cfg.MetricsAddr
	deadline := time.Now().Add(20 * time.Second)
	for {
		if code, _ := httpGet(t, base+"/healthz"); code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("healthz never returned 200, run: %v", <-done)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, body := httpGet(t, base+"/metrics"); !strings.Contains(body, "threadroute_conv_conversations_open") {
		t.Error("metrics do not include the conv gauges")
	}

	// conv created its output topic.
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := conv.WaitForTopics(context.Background(), cl, time.Second, bus.TopicConversations); err != nil {
		t.Errorf("conversations topic: %v", err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run() after cancel = %v, want nil", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not stop")
	}
}

func TestRunFailsWithoutInputTopics(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mr := miniredis.RunT(t)
	cfg := testConfig(t, c.ListenAddrs(), mr.Addr())

	err = run(context.Background(), cfg, discard(), prometheus.NewRegistry())
	if err == nil || !strings.Contains(err.Error(), bus.TopicEvents) || !strings.Contains(err.Error(), "normalize") {
		t.Errorf("run() = %v, want an error that names the missing input topics", err)
	}
}
