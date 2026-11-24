package gateway

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// countConn is a Conn that drops what it gets and counts frames and bytes.
type countConn struct {
	in     chan []byte
	closed chan struct{}
	once   sync.Once
	frames *atomic.Int64
	bytes  *atomic.Int64
}

func (c *countConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case b := <-c.in:
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, io.EOF
	}
}

func (c *countConn) Write(_ context.Context, b []byte) error {
	c.frames.Add(1)
	c.bytes.Add(int64(len(b)))
	return nil
}

func (c *countConn) Ping(context.Context) error { return nil }

func (c *countConn) Close(gwapi.CloseCode) error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// TestManySupervisorClients is a sanity number, not a load test: 1,000
// supervisor clients at the demo level on one machine, a steady stream
// of events, and the time from a tick until every client has written its
// update.
func TestManySupervisorClients(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	const clients = 1000
	e := newEnv(t, func(c *Config) { c.MaxConnections, c.MaxConnsPerAddr = clients+1, clients+1 })
	// A world of 2,000 open conversations held by 200 operators.
	const convs = 2000
	var world []Item
	for i := range 200 {
		world = append(world, ri(opEv(event.TypeOperatorOnline, fmt.Sprintf("sim-op-%04d", i), 1, t0, event.PresenceOnline), t0))
	}
	co := func(i int) string { return fmt.Sprintf("did:plc:synthcompany%013d", i%300) }
	for i := range convs {
		op := fmt.Sprintf("sim-op-%04d", i%200)
		world = append(world,
			ci(convEv(event.TypeOpened, co(i), customer(i), 1, 1, t0), t0),
			ri(rt(event.TypeQueued, co(i), customer(i), 1, 1, t0, "", ""), t0),
			ri(rt(event.TypeOffered, co(i), customer(i), 1, 2, t0, op, fmt.Sprintf("of-%d", i)), t0))
	}
	e.apply(world...)
	seqs := make([]uint64, convs)
	for i := range seqs {
		seqs[i] = 1
	}
	next := func(n int) []Item {
		out := make([]Item, 0, n)
		for range n {
			i := int(seqs[0]*7919) % convs
			seqs[i]++
			seqs[0]++
			out = append(out, ci(convEv(event.TypeMessageAdded, co(i), customer(i), 1, seqs[i], t0), t0))
		}
		return out
	}
	var rows int
	e.g.view.Read(func(rd Reader) { rd.Visible(func(string) { rows++ }) })

	var frames, bytes atomic.Int64
	before := heapInUse()
	for i := range clients {
		c := &countConn{in: make(chan []byte, 1), closed: make(chan struct{}), frames: &frames, bytes: &bytes}
		e.wg.Go(func() { e.g.Serve(e.ctx, c, fmt.Sprintf("10.0.%d.%d", i/250, i%250)) })
		b, _ := gwapi.EncodeClient(gwapi.Hello{Version: 1, View: gwapi.ViewSupervisor})
		c.in <- b
	}
	// welcome, status and a snapshot of 4 frames (500 rows each).
	snapStart := time.Now()
	for frames.Load() < 6*clients {
		if time.Since(snapStart) > time.Minute {
			t.Fatalf("%d frames after a minute", frames.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapTook := time.Since(snapStart)
	after := heapInUse()
	perClient := float64(int64(after)-int64(before)) / clients

	// 40 ticks, each with 25 new events (100 events per second).
	var worst, total time.Duration
	for range 40 {
		e.apply(next(25)...)
		want := frames.Load() + clients
		start := time.Now()
		e.g.Tick(e.clock.Now())
		deadline := time.Now().Add(10 * time.Second)
		for frames.Load() < want && time.Now().Before(deadline) {
			time.Sleep(100 * time.Microsecond)
		}
		d := time.Since(start)
		total += d
		worst = max(worst, d)
	}
	t.Logf("%d visible conversations, 200 operators; %d first states sent in %s", rows, clients, snapTook.Round(time.Millisecond))
	t.Logf("%d supervisor clients: about %.0f KiB per client after the snapshot; tick until all wrote: mean %s, worst %s; %d frames, %.1f MiB sent",
		clients, perClient/1024, (total / 40).Round(time.Microsecond), worst.Round(time.Microsecond),
		frames.Load(), float64(bytes.Load())/(1<<20))
	if worst > 2*time.Second {
		t.Errorf("a tick took %s", worst)
	}
}
