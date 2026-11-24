package gateway

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

const (
	opA = "sim-op-0001"
	opB = "web-synthoperator01"
)

// stream is the events of one family and key, in order.
type stream []Item

// history returns the streams of a synthetic world: two conversations of
// one company over two sessions, two operators and a team.
func history() []stream {
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	var out []stream
	for i, cust := range []string{customer(1), customer(2)} {
		op := []string{opA, opB}[i]
		of := func(n int) string { return fmt.Sprintf("of-%d-%d", i, n) }
		c := stream{
			ci(convEv(event.TypeOpened, testCompany, cust, 1, 1, at(1)), at(1)),
			ci(convEv(event.TypeMessageAdded, testCompany, cust, 1, 2, at(2)), at(2)),
			ci(convEv(event.TypeAnswered, testCompany, cust, 1, 3, at(30)), at(30)),
			ci(convEv(event.TypeMessageRemoved, testCompany, cust, 1, 4, at(31)), at(31)),
			ci(convEv(event.TypeClosed, testCompany, cust, 1, 5, at(100)), at(100)),
			ci(convEv(event.TypeOpened, testCompany, cust, 2, 6, at(200)), at(200)),
			ci(convEv(event.TypeMessageAdded, testCompany, cust, 2, 7, at(201)), at(201)),
		}
		requeue := rt(event.TypeQueued, testCompany, cust, 1, 4, at(13), "", "")
		requeue.QueueReason, requeue.QueuedAt = event.QueueOfferExpired, at(2)
		requeue.PreviousOperatorID, requeue.PreviousOfferID = op, of(1)
		sla := rt(event.TypeSLAMissed, testCompany, cust, 1, 7, at(80), "", "")
		sla.SLATarget, sla.OperatorID, sla.OfferID, sla.Measured, sla.Target = event.SLAFirstResponse, op, of(2), 2*time.Minute, 2*time.Minute
		reopened := rt(event.TypeQueued, testCompany, cust, 2, 11, at(202), "", "")
		reopened.QueueReason = event.QueueReopened
		r := stream{
			ri(rt(event.TypeQueued, testCompany, cust, 1, 1, at(2), "", ""), at(2)),
			ri(rt(event.TypeOffered, testCompany, cust, 1, 2, at(3), op, of(1)), at(3)),
			ri(rt(event.TypeOfferExpired, testCompany, cust, 1, 3, at(13), op, of(1)), at(13)),
			ri(requeue, at(13)),
			ri(rt(event.TypeOffered, testCompany, cust, 1, 5, at(14), op, of(2)), at(14)),
			ri(rt(event.TypeAccepted, testCompany, cust, 1, 6, at(20), op, of(2)), at(20)),
			ri(sla, at(80)),
			ri(rt(event.TypeReplied, testCompany, cust, 1, 8, at(90), op, of(2)), at(90)),
			ri(rt(event.TypeResolved, testCompany, cust, 1, 9, at(95), op, of(2)), at(95)),
			ri(rt(event.TypeEnded, testCompany, cust, 1, 10, at(100), "", ""), at(100)),
			ri(reopened, at(202)),
			ri(rt(event.TypeOffered, testCompany, cust, 2, 12, at(203), op, of(3)), at(203)),
			ri(rt(event.TypeAccepted, testCompany, cust, 2, 13, at(205), op, of(3)), at(205)),
		}
		out = append(out, c, r)
	}
	for _, op := range []string{opA, opB} {
		out = append(out, stream{
			ri(opEv(event.TypeOperatorUpdated, op, 1, at(0), event.PresenceOffline), at(0)),
			ri(opEv(event.TypeOperatorOnline, op, 2, at(1), event.PresenceOnline), at(1)),
			ri(opEv(event.TypeOperatorUpdated, op, 3, at(150), event.PresenceOnline, "de", "en", "und"), at(150)),
		})
	}
	out = append(out, stream{
		ri(teamEv(event.TypeTeamFormed, testCompany2, 1, at(5), opA), at(5)),
		ri(teamEv(event.TypeTeamResized, testCompany2, 2, at(50), opA, opB), at(50)),
		ri(teamEv(event.TypeTeamReleased, testCompany2, 3, at(150), opA, opB), at(150)),
		ri(teamEv(event.TypeTeamFormed, testCompany2, 4, at(250), opB), at(250)),
	})
	return out
}

// interleave merges the streams in a random order that keeps the order
// inside each stream, after dropping the first skip[i] items of stream i.
func interleave(rng *rand.Rand, streams []stream, skip []int) []Item {
	pos := make([]int, len(streams))
	for i := range streams {
		pos[i] = min(skip[i], len(streams[i]))
	}
	var out []Item
	for {
		var open []int
		for i := range streams {
			if pos[i] < len(streams[i]) {
				open = append(open, i)
			}
		}
		if len(open) == 0 {
			return out
		}
		i := open[rng.IntN(len(open))]
		out = append(out, streams[i][pos[i]])
		pos[i]++
	}
}

// snapshotJSON returns the supervisor snapshot and every operator
// snapshot at the full level, and checks that each one also encodes at
// the demo level.
func snapshotJSON(t *testing.T, v *View) string {
	t.Helper()
	ps := NewPseudonyms(t0)
	timing := Timing{SpeedFactor: 1, FirstResponseTarget: 2 * time.Minute}
	out := ""
	v.Read(func(rd Reader) {
		totals := rd.Totals(t0.Add(300 * time.Second))
		for _, level := range []gwapi.Level{gwapi.LevelFull, gwapi.LevelDemo} {
			aud := gwapi.Audience{Level: level, View: gwapi.ViewSupervisor}
			b := &builder{aud: aud, ps: ps, timing: timing, frameRows: 3}
			frames := b.snapshot(rd, totals)
			var seq uint64
			stamp(frames, &seq, t0)
			for _, f := range frames {
				enc, err := gwapi.EncodeServer(f, aud)
				if err != nil {
					t.Fatalf("supervisor %s: %v", level, err)
				}
				if level == gwapi.LevelFull {
					out += string(enc) + "\n"
				}
			}
			for _, op := range []string{opA, opB} {
				oaud := gwapi.Audience{Level: level, View: gwapi.ViewOperator}
				ob := &builder{aud: oaud, ps: ps, timing: timing, frameRows: 3, operatorID: op,
					me: func() gwapi.OperatorRow {
						return gwapi.OperatorRow{ID: op, Kind: gwapi.KindOf(op), Languages: []string{"en"}, Capacity: 2, Presence: gwapi.PresenceOffline}
					}}
				if gwapi.KindOf(op) != gwapi.KindHuman {
					continue
				}
				frames := ob.snapshot(rd, totals)
				stamp(frames, &seq, t0)
				for _, f := range frames {
					enc, err := gwapi.EncodeServer(f, oaud)
					if err != nil {
						t.Fatalf("operator %s: %v", level, err)
					}
					if level == gwapi.LevelFull {
						out += string(enc) + "\n"
					}
				}
			}
		}
	})
	return out
}

// TestViewAnyOrder applies suffixes of the history in many orders: every
// row is valid at both levels, and the order of arrival between keys does
// not change the result.
func TestViewAnyOrder(t *testing.T) {
	streams := history()
	rng := rand.New(rand.NewPCG(1, 2))
	for round := range 300 {
		skip := make([]int, len(streams))
		if round > 0 {
			for i := range skip {
				// Missing first events: up to the whole stream.
				skip[i] = rng.IntN(len(streams[i]) + 1)
			}
		}
		var want string
		for order := range 4 {
			v := NewView(time.Hour)
			items := interleave(rng, streams, skip)
			// Every item twice: repeats are skipped.
			if order%2 == 1 {
				items = append(items, items...)
			}
			v.ApplyBatch(items)
			got := snapshotJSON(t, v)
			if order == 0 {
				want = got
				continue
			}
			if got != want {
				t.Fatalf("round %d skip %v: order %d gives another view:\n%s\nwant:\n%s", round, skip, order, got, want)
			}
		}
	}
}

func TestViewLifecycle(t *testing.T) {
	v := NewView(time.Hour)
	var items []Item
	for _, s := range history() {
		items = append(items, s...)
	}
	v.ApplyBatch(items)
	cust1 := convKey(testCompany, customer(1))
	v.Read(func(rd Reader) {
		row, ok := rd.Conversation(cust1)
		if !ok || row.State != gwapi.StateAssigned || row.Session != 2 || row.OperatorID != opA || row.AssignmentMissed || row.FirstResponseMissed {
			t.Errorf("row %+v %v", row, ok)
		}
		op, ok := rd.Operator(opA)
		if !ok || op.Load != 1 || op.Offered != 0 || op.Presence != gwapi.PresenceOnline || len(op.Languages) != 3 {
			t.Errorf("operator %+v", op)
		}
		if _, ok := rd.Team(testCompany2); !ok {
			t.Error("team missing")
		}
		q, ok := rd.Queue("en")
		if !ok || q.Waiting != 0 || q.Offered != 0 {
			t.Errorf("queue %+v", q)
		}
		tot := rd.Totals(t0.Add(205 * time.Second))
		if tot.Conversations.Assigned != 2 || tot.OperatorsOnline != 2 || tot.HumansOnline != 1 || tot.SlotsUsed != 2 ||
			tot.CompaniesActive != 1 || tot.MissedFirstResponse != 2 || tot.WaitP50US != 5_000_000 {
			t.Errorf("totals %+v", tot)
		}
		if tot.MessagesPerSecond != 0.4 {
			t.Errorf("messages per second %v, want 4 messages in 10 s", tot.MessagesPerSecond)
		}
	})
}

// TestViewStates checks the derived state after each step of one
// session, including the fallbacks for missing first events.
func TestViewStates(t *testing.T) {
	at := t0
	key := convKey(testCompany, testCustomer)
	tests := []struct {
		name  string
		items []Item
		state gwapi.ConversationState
		ok    bool
	}{
		{"conv only", []Item{ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, at), at)}, gwapi.StateOpen, true},
		{"routing only is hidden from supervisors", []Item{ri(rt(event.TypeQueued, testCompany, testCustomer, 1, 1, at, "", ""), at)}, "", false},
		{"queued", []Item{
			ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, at), at),
			ri(rt(event.TypeQueued, testCompany, testCustomer, 1, 1, at, "", ""), at),
		}, gwapi.StateWaiting, true},
		{"reply without accept shows the assignment", []Item{
			ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, at), at),
			ri(rt(event.TypeReplied, testCompany, testCustomer, 1, 7, at, opA, "of-1"), at),
		}, gwapi.StateAssigned, true},
		{"routing of an older session does not count", []Item{
			ci(convEv(event.TypeOpened, testCompany, testCustomer, 2, 9, at), at),
			ri(rt(event.TypeAccepted, testCompany, testCustomer, 1, 7, at, opA, "of-1"), at),
		}, gwapi.StateOpen, true},
		{"closed", []Item{
			ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, at), at),
			ci(convEv(event.TypeClosed, testCompany, testCustomer, 1, 2, at), at),
		}, "", false},
		{"ended by the router", []Item{
			ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, at), at),
			ri(rt(event.TypeEnded, testCompany, testCustomer, 1, 3, at, "", ""), at),
		}, "", false},
		{"removed after close keeps it closed", []Item{
			ci(convEv(event.TypeClosed, testCompany, testCustomer, 1, 4, at), at),
			ci(convEv(event.TypeMessageRemoved, testCompany, testCustomer, 1, 5, at), at),
		}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewView(time.Hour)
			v.ApplyBatch(tt.items)
			v.Read(func(rd Reader) {
				row, ok := rd.Conversation(key)
				if ok != tt.ok || row.State != tt.state {
					t.Errorf("row %+v %v, want state %q %v", row, ok, tt.state, tt.ok)
				}
			})
			snapshotJSON(t, v)
		})
	}
}

func TestViewSkipsRepeatsAndRestartsConvSequence(t *testing.T) {
	v := NewView(time.Hour)
	key := convKey(testCompany, testCustomer)
	open := convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, t0)
	res := v.ApplyBatch([]Item{
		ci(open, t0), ci(open, t0),
		ci(convEv(event.TypeClosed, testCompany, testCustomer, 3, 9, t0), t0),
	})
	if res[0] != ResultApplied || res[1] != ResultSkipped || res[2] != ResultApplied {
		t.Fatalf("results %v", res)
	}
	// conv forgot the conversation and starts again (decision 0009).
	if got := v.ApplyBatch([]Item{ci(open, t0.Add(time.Hour))}); got[0] != ResultApplied {
		t.Fatalf("restart %v", got)
	}
	v.Read(func(rd Reader) {
		if row, ok := rd.Conversation(key); !ok || row.Session != 1 {
			t.Errorf("after restart %+v %v", row, ok)
		}
	})
	// An opened event 1/1 for an open conversation is a repeat.
	if got := v.ApplyBatch([]Item{ci(open, t0.Add(time.Hour))}); got[0] != ResultSkipped {
		t.Fatalf("repeat of an open conversation %v", got)
	}
}

func TestViewForgets(t *testing.T) {
	v := NewView(time.Hour)
	cust := testCustomer
	v.ApplyBatch([]Item{
		ci(convEv(event.TypeOpened, testCompany, cust, 1, 1, t0), t0),
		ci(convEv(event.TypeClosed, testCompany, cust, 1, 2, t0), t0),
		ri(opEv(event.TypeOperatorOffline, opB, 1, t0, event.PresenceOffline), t0),
		ri(opEv(event.TypeOperatorOffline, opA, 1, t0, event.PresenceOffline), t0),
		ri(teamEv(event.TypeTeamReleased, testCompany2, 3, t0, opA), t0),
		ci(convEv(event.TypeOpened, testCompany2, cust, 1, 1, t0), t0),
	})
	v.Sweep(t0.Add(59 * time.Minute))
	if s := v.Sizes(); s["conversations"] != 2 || s["operators"] != 2 || s["teams"] != 1 {
		t.Fatalf("before the forget time: %v", s)
	}
	v.Sweep(t0.Add(61 * time.Minute))
	// The open conversation and the simulated operator stay.
	if s := v.Sizes(); s["conversations"] != 1 || s["operators"] != 1 || s["teams"] != 0 {
		t.Fatalf("after the forget time: %v", s)
	}
	v.Sweep(t0.Add(27 * time.Hour))
	if s := v.Sizes(); s["conversations"] != 0 {
		t.Fatalf("an open conversation without events for 27 h: %v", s)
	}
}

// syntheticDay returns n events over 24 hours: conversations that open,
// get messages, are routed and close after a few minutes.
func syntheticDay(n int) []Item {
	var items []Item
	ops := 200
	for i := 0; i < ops; i++ {
		id := fmt.Sprintf("sim-op-%04d", i)
		items = append(items, ri(opEv(event.TypeOperatorOnline, id, 1, t0, event.PresenceOnline), t0))
	}
	step := 24 * time.Hour / time.Duration(n/12)
	for c := 0; len(items) < n; c++ {
		at := t0.Add(time.Duration(c) * step)
		co := fmt.Sprintf("did:plc:synthcompany%013d", c%500)
		cu := customer(c)
		op := fmt.Sprintf("sim-op-%04d", c%ops)
		of := fmt.Sprintf("of-%d", c)
		s := func(d time.Duration) time.Time { return at.Add(d) }
		items = append(items,
			ci(convEv(event.TypeOpened, co, cu, 1, 1, s(0)), s(0)),
			ri(rt(event.TypeQueued, co, cu, 1, 1, s(time.Second), "", ""), s(time.Second)),
			ri(rt(event.TypeOffered, co, cu, 1, 2, s(2*time.Second), op, of), s(2*time.Second)),
			ri(rt(event.TypeAccepted, co, cu, 1, 3, s(5*time.Second), op, of), s(5*time.Second)),
			ci(convEv(event.TypeMessageAdded, co, cu, 1, 2, s(time.Minute)), s(time.Minute)),
			ci(convEv(event.TypeAnswered, co, cu, 1, 3, s(2*time.Minute)), s(2*time.Minute)),
			ri(rt(event.TypeReplied, co, cu, 1, 4, s(2*time.Minute), op, of), s(2*time.Minute)),
			ci(convEv(event.TypeMessageAdded, co, cu, 1, 4, s(3*time.Minute)), s(3*time.Minute)),
			ri(rt(event.TypeResolved, co, cu, 1, 5, s(4*time.Minute), op, of), s(4*time.Minute)),
			ci(convEv(event.TypeMessageRemoved, co, cu, 1, 5, s(5*time.Minute)), s(5*time.Minute)),
			ci(convEv(event.TypeClosed, co, cu, 1, 6, s(34*time.Minute)), s(34*time.Minute)),
			ri(rt(event.TypeEnded, co, cu, 1, 6, s(34*time.Minute), "", ""), s(34*time.Minute)),
		)
	}
	return items
}

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// TestViewMemoryOfADay applies a synthetic day of 300,000 events and
// reports the memory of the view. With the forget rule only the last hour
// of closed conversations stays.
func TestViewMemoryOfADay(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	items := syntheticDay(300_000)
	before := heapInUse()
	v := NewView(time.Hour)
	start := time.Now()
	for i := 0; i < len(items); i += 2000 {
		v.ApplyBatch(items[i:min(i+2000, len(items))])
		v.takeChanges()
	}
	took := time.Since(start)
	after := heapInUse()
	sizes := v.Sizes()
	runtime.KeepAlive(items)
	t.Logf("synthetic day: %d events applied in %s; view rows %v; heap of the view about %.1f MiB",
		len(items), took.Round(time.Millisecond), sizes, float64(int64(after)-int64(before))/(1<<20))
	// 25,000 conversations a day, about 1,000 per hour; the rows of the
	// last hour plus the forget time stay.
	if sizes["conversations"] > 3000 {
		t.Errorf("%d conversation rows: the forget rule does not bound memory", sizes["conversations"])
	}
	if int64(after)-int64(before) > 64<<20 {
		t.Errorf("view uses %d bytes", int64(after)-int64(before))
	}
}

func BenchmarkViewApply(b *testing.B) {
	items := syntheticDay(60_000)
	b.ResetTimer()
	for b.Loop() {
		v := NewView(time.Hour)
		for i := 0; i < len(items); i += 2000 {
			v.ApplyBatch(items[i:min(i+2000, len(items))])
			v.takeChanges()
		}
	}
	b.ReportMetric(float64(len(items)), "events/op")
}

// TestSnapshotJSONStable is a guard for snapshotJSON: two views of the
// same events give the same text.
func TestSnapshotJSONStable(t *testing.T) {
	var items []Item
	for _, s := range history() {
		items = append(items, s...)
	}
	a, b := NewView(time.Hour), NewView(time.Hour)
	a.ApplyBatch(items)
	b.ApplyBatch(items)
	ja, jb := snapshotJSON(t, a), snapshotJSON(t, b)
	if ja != jb || !json.Valid([]byte(firstLine(ja))) {
		t.Fatal("snapshots differ")
	}
}

func firstLine(s string) string {
	for i := range len(s) {
		if s[i] == '\n' {
			return s[:i]
		}
	}
	return s
}
