package gateway

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// trendWorld builds a view with known numbers at t0: two conversations
// waiting (queued 90 and 30 seconds before t0), one offered (queued 120
// seconds before t0, the longest wait), one assigned, three resolves in
// the last minute and one older, one team of two and one released team,
// and three operators online and one offline.
func trendWorld(e *env) {
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	var items []Item
	for i, op := range []string{"sim-op-0001", "sim-op-0002", "sim-op-0003"} {
		items = append(items, ri(opEv(event.TypeOperatorOnline, op, uint64(i+1), at(-300), event.PresenceOnline), at(-300)))
	}
	items = append(items, ri(opEv(event.TypeOperatorOffline, "sim-op-0004", 1, at(-300), event.PresenceOffline), at(-300)))
	waitAt := map[int]int{1: -90, 2: -30, 3: -120, 4: -200}
	for n := 1; n <= 4; n++ {
		cust := customer(n)
		items = append(items,
			ci(convEv(event.TypeOpened, testCompany, cust, 1, 1, at(waitAt[n])), at(waitAt[n])),
			ri(rt(event.TypeQueued, testCompany, cust, 1, 1, at(waitAt[n]), "", ""), at(waitAt[n])))
	}
	items = append(items,
		ri(rt(event.TypeOffered, testCompany, customer(3), 1, 2, at(-120), "sim-op-0001", "of-3"), at(-5)),
		ri(rt(event.TypeAccepted, testCompany, customer(4), 1, 2, at(-150), "sim-op-0002", "of-4"), at(-150)))
	// Resolves of earlier conversations: three in the last minute, one
	// before it.
	for n, s := range []int{-10, -30, -59, -70} {
		cust := customer(100 + n)
		items = append(items,
			ri(rt(event.TypeAccepted, testCompany2, cust, 1, 1, at(s-5), "sim-op-0003", fmt.Sprintf("of-r%d", n)), at(s-5)),
			ri(rt(event.TypeResolved, testCompany2, cust, 1, 2, at(s), "sim-op-0003", fmt.Sprintf("of-r%d", n)), at(s)))
	}
	items = append(items,
		ri(teamEv(event.TypeTeamFormed, testCompany, 1, at(-100), "sim-op-0001", "sim-op-0002"), at(-100)),
		ri(teamEv(event.TypeTeamFormed, testCompany2, 1, at(-100), "sim-op-0003"), at(-100)),
		ri(teamEv(event.TypeTeamReleased, testCompany2, 2, at(-50), "sim-op-0003"), at(-50)))
	e.apply(items...)
}

func TestTrendPointNumbers(t *testing.T) {
	e := newEnv(t)
	trendWorld(e)
	var p gwapi.TrendPoint
	var totals gwapi.Totals
	var teams, members uint64
	var oldest int64
	e.g.view.Read(func(rd Reader) {
		p = rd.TrendPoint(t0, 2)
		totals = rd.Totals(t0)
		rd.Teams(func(co string) {
			row, _ := rd.Team(co)
			teams++
			members += uint64(len(row.Members))
		})
		for _, l := range rd.QueueLangs() {
			if q, ok := rd.Queue(l); ok && q.OldestQueuedAtUS > 0 && (oldest == 0 || q.OldestQueuedAtUS < oldest) {
				oldest = q.OldestQueuedAtUS
			}
		}
	})
	want := gwapi.TrendPoint{
		SampledAtUS: t0.UnixMicro(), Waiting: 2, LongestWaitUS: 2 * 120_000_000, ResolvedLastMinute: 3,
		Teams: 1, OperatorsInTeams: 2, OperatorsOnline: 3,
	}
	if p != want {
		t.Fatalf("point %+v\nwant  %+v", p, want)
	}
	// The same numbers as the supervisor state.
	if p.Waiting != totals.Conversations.Waiting || p.OperatorsOnline != totals.OperatorsOnline ||
		p.Teams != teams || p.OperatorsInTeams != members || p.LongestWaitUS != 2*(t0.UnixMicro()-oldest) {
		t.Fatalf("point %+v does not agree with totals %+v, teams %d/%d, oldest %d", p, totals, teams, members, oldest)
	}
	// Later, the old resolves leave the window.
	e.g.view.Read(func(rd Reader) { p = rd.TrendPoint(t0.Add(time.Minute), 1) })
	if p.ResolvedLastMinute != 0 {
		t.Fatalf("resolves a minute later %d", p.ResolvedLastMinute)
	}
}

// TestTrendStartsAfterCatchUp: no point while the gateway reads the topics
// from the beginning, then one per call, sent to supervisors only.
func TestTrendStartsAfterCatchUp(t *testing.T) {
	e := newEnv(t)
	e.g.SetStatus(gwapi.Status{Stale: true, Reason: gwapi.StaleCatchingUp})
	src := e.g.NewSource(SourceOptions{ConversationsTopic: "c", RoutingTopic: "r"})
	src.apply(t0, listed("c", 0), merge(listed("c", 3), listed("r", 0)), nil)
	sup, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
	op, _ := e.connect(operatorHello(), "10.0.0.2")
	trendWorld(e)
	if e.g.RecordTrend(t0) || len(e.g.trendHistory()) != 0 {
		t.Fatal("a point while catching up")
	}
	src.mu.Lock()
	src.pos[topicPartition{"c", 0}] = 3
	src.mu.Unlock()
	src.apply(t0.Add(time.Second), listed("c", 0), merge(listed("c", 3), listed("r", 0)), nil)
	if e.g.Status().Stale {
		t.Fatalf("status %+v", e.g.Status())
	}
	if !e.g.RecordTrend(t0.Add(10 * time.Second)) {
		t.Fatal("no point after the catch-up")
	}
	aud := gwapi.Audience{Level: gwapi.LevelDemo, View: gwapi.ViewSupervisor}
	m, err := gwapi.DecodeServer(sup.nextOf(t, gwapi.TypeTrend), aud)
	if err != nil {
		t.Fatal(err)
	}
	if p := m.(gwapi.TrendUpdate).Point; p.OperatorsOnline != 3 || p.Waiting != 2 {
		t.Fatalf("point %+v", p)
	}
	for _, b := range op.drain(100 * time.Millisecond) {
		if frameType(t, b) == gwapi.TypeTrend {
			t.Fatal("an operator got a trend point")
		}
	}
	// A new supervisor gets the history in its Welcome and only later
	// points as messages.
	late, w := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.3")
	if w.Trend == nil || len(w.Trend.Points) != 1 {
		t.Fatalf("welcome trend %+v", w.Trend)
	}
	e.g.RecordTrend(t0.Add(20 * time.Second))
	var got []int64
	for _, b := range late.drain(150 * time.Millisecond) {
		if frameType(t, b) == gwapi.TypeTrend {
			m, _ := gwapi.DecodeServer(b, aud)
			got = append(got, m.(gwapi.TrendUpdate).Point.SampledAtUS)
		}
	}
	if len(got) != 1 || got[0] != t0.Add(20*time.Second).UnixMicro() {
		t.Fatalf("trend messages %v", got)
	}
	// Stale again: no point.
	e.g.SetStatus(gwapi.Status{Stale: true, Reason: gwapi.StaleLagging, LagUS: 11_000_000})
	if e.g.RecordTrend(t0.Add(30 * time.Second)) {
		t.Fatal("a point while lagging")
	}
	if v := testutil.ToFloat64(e.g.m.trendPoints); v != 2 {
		t.Errorf("points metric %v", v)
	}
}

func TestTrendHistoryLimits(t *testing.T) {
	e := newEnv(t)
	now := t0
	for range 400 {
		now = now.Add(gwapi.TrendInterval)
		if !e.g.RecordTrend(now) {
			t.Fatal("no point")
		}
	}
	h := e.g.trendHistory()
	if len(h) != gwapi.MaxTrendPoints || h[len(h)-1].SampledAtUS != now.UnixMicro() ||
		h[0].SampledAtUS != now.Add(-time.Duration(gwapi.MaxTrendPoints-1)*gwapi.TrendInterval).UnixMicro() {
		t.Fatalf("%d points from %d to %d", len(h), h[0].SampledAtUS, h[len(h)-1].SampledAtUS)
	}
	// A clock that goes back gives no point.
	if e.g.RecordTrend(now) || e.g.RecordTrend(now.Add(-time.Second)) {
		t.Fatal("a point that is not newer")
	}
	// After a long pause only the points of the last hour stay.
	now = now.Add(gwapi.TrendHistory - gwapi.TrendInterval)
	e.g.RecordTrend(now)
	if h := e.g.trendHistory(); len(h) != 2 {
		t.Fatalf("%d points after a pause", len(h))
	}
	// A Welcome with the full history passes validation.
	_, w := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
	if w.Trend == nil || len(w.Trend.Points) != 2 {
		t.Fatalf("welcome %+v", w.Trend)
	}
}

// BenchmarkTrendPoint measures one point on a view of about 2,500 open
// conversations, as in the last measurement of the tick.
func BenchmarkTrendPoint(b *testing.B) {
	v := NewView(time.Hour)
	at := t0
	var items []Item
	for i := range 200 {
		items = append(items, ri(opEv(event.TypeOperatorOnline, fmt.Sprintf("sim-op-%04d", i), 1, at, event.PresenceOnline), at))
	}
	for i := range 2500 {
		cust := customer(i)
		co := fmt.Sprintf("did:plc:synthcompany%014d", i%300)
		items = append(items,
			ci(convEv(event.TypeOpened, co, cust, 1, 1, at), at),
			ri(rt(event.TypeQueued, co, cust, 1, 1, at, "", ""), at))
		if i%3 == 0 {
			op := fmt.Sprintf("sim-op-%04d", i%200)
			items = append(items, ri(rt(event.TypeAccepted, co, cust, 1, 2, at, op, fmt.Sprintf("of-%d", i)), at))
		}
	}
	for i := range 20 {
		items = append(items, ri(teamEv(event.TypeTeamFormed, fmt.Sprintf("did:plc:synthcompany%014d", i), 1, at,
			fmt.Sprintf("sim-op-%04d", i*2), fmt.Sprintf("sim-op-%04d", i*2+1)), at))
	}
	v.ApplyBatch(items)
	now := at.Add(time.Minute)
	b.ResetTimer()
	for b.Loop() {
		v.Read(func(rd Reader) { _ = rd.TrendPoint(now, 1) })
	}
}
