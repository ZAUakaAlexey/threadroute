package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/ZAUakaAlexey/threadroute/internal/conv"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// The fixture conformance test: for every server fixture of the protocol
// package the gateway makes a message of the same type and the same shape
// (the same set of JSON field paths) from synthetic input, and the
// message passes validation for the audience and survives a round trip.

type fixtureEntry struct {
	File      string      `json:"file"`
	Direction string      `json:"direction"`
	Type      string      `json:"type"`
	Level     gwapi.Level `json:"level"`
	View      gwapi.View  `json:"view"`
}

const testdata = "../gwapi/testdata"

func loadFixtures(t *testing.T) []fixtureEntry {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(testdata, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var idx []fixtureEntry
	if err := json.Unmarshal(b, &idx); err != nil {
		t.Fatal(err)
	}
	return idx
}

// shape returns the field paths of a JSON value. List items share one
// path, so a list adds the fields of all its items.
func shape(t *testing.T, b []byte) []string {
	t.Helper()
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	set := map[string]bool{}
	var walk func(prefix string, v any)
	walk = func(prefix string, v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, val := range x {
				p := prefix + "." + k
				set[p] = true
				walk(p, val)
			}
		case []any:
			for _, item := range x {
				walk(prefix+"[]", item)
			}
		}
	}
	walk("", v)
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// conformanceView is the synthetic world of the state fixtures.
func conformanceView(op string) *View {
	v := NewView(time.Hour)
	at := t0
	cA, cB, cC := customer(11), customer(12), customer(13)
	v.ApplyBatch([]Item{
		ri(opEv(event.TypeOperatorOnline, op, 2, at, event.PresenceOnline), at),
		ri(opEv(event.TypeOperatorUpdated, "sim-op-0002", 1, at, event.PresenceOffline, "de", "en", "und"), at),
		ci(convEv(event.TypeOpened, testCompany, cA, 1, 1, at), at),
		ri(rt(event.TypeQueued, testCompany, cA, 1, 1, at, "", ""), at),
		ri(rt(event.TypeAccepted, testCompany, cA, 1, 2, at.Add(time.Second), op, "of-101"), at),
		ci(convEv(event.TypeOpened, testCompany, cB, 1, 1, at), at),
		ri(rt(event.TypeQueued, testCompany, cB, 1, 1, at, "", ""), at),
		ri(rt(event.TypeOffered, testCompany, cB, 1, 2, at, op, "of-102"), at),
		ci(convEv(event.TypeOpened, testCompany, cC, 1, 1, at), at),
	})
	team := rt(event.TypeQueued, testCompany, cC, 1, 1, at, "", "")
	team.TeamQueue = true
	sla := rt(event.TypeSLAMissed, testCompany, cC, 1, 2, at, "", "")
	sla.TeamQueue = true
	member := opEv(event.TypeOperatorUpdated, op, 3, at, event.PresenceOnline)
	member.DedicatedCompanyID = testCompany
	v.ApplyBatch([]Item{
		ri(team, at), ri(sla, at),
		ri(teamEv(event.TypeTeamFormed, testCompany, 1, at, op), at),
		ri(member, at),
	})
	return v
}

func TestFixtureConformance(t *testing.T) {
	makers := map[string]func(t *testing.T) []byte{
		"server_welcome_operator_demo.json": func(t *testing.T) []byte {
			e := newEnv(t)
			c := newFakeConn()
			e.wg.Go(func() { e.g.Serve(e.ctx, c, "10.0.0.1") })
			c.send(t, operatorHello())
			return c.nextOf(t, gwapi.TypeWelcome)
		},
		"server_welcome_supervisor_full.json": func(t *testing.T) []byte {
			e := newEnv(t)
			c := newFakeConn()
			e.wg.Go(func() { e.g.Serve(e.ctx, c, "10.0.0.1") })
			c.send(t, gwapi.Hello{Version: 1, View: gwapi.ViewSupervisor, Token: "full-token"})
			return c.nextOf(t, gwapi.TypeWelcome)
		},
		"server_status_fresh.json": func(t *testing.T) []byte {
			e := newEnv(t)
			c, _ := e.connect(gwapi.Hello{View: gwapi.ViewOperator, Languages: []string{"en"}}, "10.0.0.1")
			return c.nextOf(t, gwapi.TypeStatus)
		},
		"server_status_stale.json": func(t *testing.T) []byte {
			e := newEnv(t)
			c, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
			c.nextOf(t, gwapi.TypeState)
			src := e.g.NewSource(SourceOptions{})
			src.apply(t0, kadm.ListedOffsets{}, kadm.ListedOffsets{}, nil)
			src.apply(t0.Add(12500*time.Millisecond), nil, nil, os.ErrDeadlineExceeded)
			return c.nextOf(t, gwapi.TypeStatus)
		},
		"server_state_operator_snapshot_demo.json": func(t *testing.T) []byte {
			return buildState(t, gwapi.LevelDemo, gwapi.ViewOperator, true, nil)
		},
		"server_state_operator_update_demo.json": func(t *testing.T) []byte {
			return buildState(t, gwapi.LevelDemo, gwapi.ViewOperator, false, func(v *View) *dirty {
				cA, cB := customer(11), customer(12)
				v.ApplyBatch([]Item{
					ri(rt(event.TypeReplied, testCompany, cA, 1, 3, t0, opB, "of-101"), t0),
					ri(rt(event.TypeOfferExpired, testCompany, cB, 1, 3, t0, opB, "of-102"), t0),
				})
				d := newDirty()
				d.me = true
				d.convs[convKey(testCompany, cA)] = struct{}{}
				d.convs[convKey(testCompany, cB)] = struct{}{}
				return d
			})
		},
		"server_state_supervisor_snapshot_full.json": func(t *testing.T) []byte {
			return buildState(t, gwapi.LevelFull, gwapi.ViewSupervisor, true, nil)
		},
		"server_state_supervisor_update_demo.json": func(t *testing.T) []byte {
			return buildState(t, gwapi.LevelDemo, gwapi.ViewSupervisor, false, func(v *View) *dirty {
				cA, cB := customer(11), customer(12)
				v.ApplyBatch([]Item{
					ri(rt(event.TypeResolved, testCompany, cA, 1, 3, t0, opB, "of-101"), t0),
					ci(convEv(event.TypeClosed, testCompany, cB, 1, 2, t0), t0),
				})
				d := newDirty()
				d.convs[convKey(testCompany, cA)] = struct{}{}
				d.convs[convKey(testCompany, cB)] = struct{}{}
				d.ops[opB] = struct{}{}
				d.teams[testCompany] = struct{}{}
				d.totals = true
				return d
			})
		},
		"server_messages_snapshot_full.json":  func(t *testing.T) []byte { return watchFrame(t, "full-token", true, false) },
		"server_messages_snapshot_demo.json":  func(t *testing.T) []byte { return watchFrame(t, "", true, false) },
		"server_messages_snapshot_empty.json": func(t *testing.T) []byte { return watchFrame(t, "", false, false) },
		"server_messages_update_full.json":    func(t *testing.T) []byte { return watchFrame(t, "full-token", true, true) },
		"server_message_removed_full.json":    func(t *testing.T) []byte { return removedFrame(t, "full-token") },
		"server_message_removed_demo.json":    func(t *testing.T) []byte { return removedFrame(t, "") },
		"server_result_done.json": func(t *testing.T) []byte {
			return actionFrame(t, nil, gwapi.Action{RequestID: "r", Action: gwapi.ActionOnline})
		},
		"server_result_already_done.json": func(t *testing.T) []byte {
			return actionFrame(t, func(a *fakeAPI) { a.result = opapi.ResultAlreadyDone }, gwapi.Action{RequestID: "r", Action: gwapi.ActionOnline})
		},
		"server_result_rejected_router.json": func(t *testing.T) []byte {
			return actionFrame(t, func(a *fakeAPI) {
				a.err = &opapi.Error{Status: 404, ErrorResponse: opapi.ErrorResponse{Code: opapi.CodeOperatorNotFound}}
			}, gwapi.Action{RequestID: "r", Action: gwapi.ActionOnline})
		},
		"server_result_rejected_gateway.json": func(t *testing.T) []byte {
			return actionFrame(t, nil, gwapi.Action{RequestID: "r", Action: gwapi.ActionAccept, OfferID: "of-404"})
		},
	}
	seen := 0
	for _, f := range loadFixtures(t) {
		if f.Direction != "server" {
			continue
		}
		seen++
		t.Run(f.File, func(t *testing.T) {
			mk, ok := makers[f.File]
			if !ok {
				t.Fatalf("no conformance case for %s", f.File)
			}
			got := mk(t)
			want, err := os.ReadFile(filepath.Join(testdata, f.File))
			if err != nil {
				t.Fatal(err)
			}
			aud := gwapi.Audience{Level: f.Level, View: f.View}
			m, err := gwapi.DecodeServer(got, aud)
			if err != nil {
				t.Fatalf("decode %s: %v", got, err)
			}
			if string(m.ServerType()) != f.Type {
				t.Fatalf("type %s, want %s", m.ServerType(), f.Type)
			}
			again, err := gwapi.EncodeServer(m, aud)
			if err != nil || !sameJSON(t, got, again) {
				t.Fatalf("round trip %s / %s: %v", got, again, err)
			}
			if gs, ws := shape(t, got), shape(t, want); !slices.Equal(gs, ws) {
				t.Errorf("shape\n got %v\nwant %v\nframe %s", gs, ws, got)
			}
		})
	}
	if seen != len(makers) {
		t.Errorf("%d server fixtures, %d cases", seen, len(makers))
	}
}

func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if json.Unmarshal(a, &va) != nil || json.Unmarshal(b, &vb) != nil {
		return false
	}
	ja, _ := json.Marshal(va)
	jb, _ := json.Marshal(vb)
	return string(ja) == string(jb)
}

// buildState builds a state of the conformance world with the gateway's
// builder: a snapshot, or the update of the dirty keys that change
// returns after it changed the view.
func buildState(t *testing.T, level gwapi.Level, view gwapi.View, snap bool, change func(v *View) *dirty) []byte {
	t.Helper()
	v := conformanceView(opB)
	if view == gwapi.ViewOperator {
		// The operator fixtures show an operator outside a team.
		v.ApplyBatch([]Item{ri(opEv(event.TypeOperatorUpdated, opB, 4, t0, event.PresenceOnline), t0)})
	}
	aud := gwapi.Audience{Level: level, View: view}
	b := &builder{aud: aud, ps: NewPseudonyms(t0), timing: Timing{SpeedFactor: 1, FirstResponseTarget: 2 * time.Minute}, frameRows: 100}
	if view == gwapi.ViewOperator {
		b.operatorID = opB
		b.me = func() gwapi.OperatorRow { return gwapi.OperatorRow{} }
	}
	var st gwapi.State
	var seq uint64
	if snap {
		v.Read(func(rd Reader) {
			frames := b.snapshot(rd, rd.Totals(t0.Add(time.Second)))
			stamp(frames, &seq, t0)
			st = frames[0]
		})
	} else {
		d := change(v)
		seq = 6
		v.Read(func(rd Reader) {
			var ok bool
			st, ok = b.update(rd, d, rd.Totals(t0.Add(time.Second)))
			if !ok {
				t.Fatal("empty update")
			}
			frames := []gwapi.State{st}
			stamp(frames, &seq, t0)
			st = frames[0]
		})
	}
	out, err := gwapi.EncodeServer(st, aud)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// watchSetup connects a supervisor and watches one conversation with
// three stored messages (or none).
func watchSetup(t *testing.T, token string, withText bool) (*env, *fakeConn, string) {
	t.Helper()
	e := newEnv(t)
	if withText {
		e.text.put(testCompany, testCustomer, conv.StoredMessage{ID: "synth-m-1", Direction: event.FromCustomer, SourceTime: t0, Text: "synthetic one"})
		e.text.put(testCompany, testCustomer, conv.StoredMessage{ID: "synth-m-2", Direction: event.FromCompany, SourceTime: t0.Add(time.Second), Text: "synthetic two"})
		e.text.put(testCompany, testCustomer, conv.StoredMessage{ID: "synth-m-3", Direction: event.FromCustomer, SourceTime: t0.Add(2 * time.Second), Text: "synthetic three, café", Edited: true})
	}
	c, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: token}, "10.0.0.1")
	e.apply(ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, t0), t0))
	key := convKey(testCompany, testCustomer)
	id := key
	if token == "" {
		id = e.g.ps.Of(key)
	}
	c.send(t, gwapi.Watch{RequestID: "w", ConversationID: id})
	decodeResult(t, c.nextOf(t, gwapi.TypeResult))
	return e, c, id
}

func watchFrame(t *testing.T, token string, withText, update bool) []byte {
	t.Helper()
	e, c, _ := watchSetup(t, token, withText)
	snap := c.nextOf(t, gwapi.TypeMessages)
	if !update {
		return snap
	}
	e.text.put(testCompany, testCustomer, conv.StoredMessage{ID: "synth-m-4", Direction: event.FromCustomer, SourceTime: t0.Add(time.Minute), Text: "synthetic four"})
	added := convEv(event.TypeMessageAdded, testCompany, testCustomer, 1, 2, t0)
	added.MessageID = "synth-m-4"
	e.apply(ci(added, t0))
	e.g.Tick(e.clock.Now())
	return c.nextOf(t, gwapi.TypeMessages)
}

func removedFrame(t *testing.T, token string) []byte {
	t.Helper()
	e, c, _ := watchSetup(t, token, true)
	c.nextOf(t, gwapi.TypeMessages)
	rm := convEv(event.TypeMessageRemoved, testCompany, testCustomer, 1, 2, t0)
	rm.MessageID = "synth-m-1"
	e.apply(ci(rm, t0))
	return c.nextOf(t, gwapi.TypeMessageRemoved)
}

func actionFrame(t *testing.T, setup func(*fakeAPI), a gwapi.Action) []byte {
	t.Helper()
	e := newEnv(t)
	c, _ := e.connect(operatorHello(), "10.0.0.1")
	if setup != nil {
		e.api.mu.Lock()
		setup(e.api)
		e.api.mu.Unlock()
	}
	c.send(t, a)
	return c.nextOf(t, gwapi.TypeResult)
}

// TestEveryServerMessageValidates checks that each frame a session sends
// passes the protocol validation of its audience; Encode would refuse it
// otherwise, so a refused frame shows as a close with 1011.
func TestEveryServerMessageValidates(t *testing.T) {
	e := newEnv(t)
	conns := map[*fakeConn]gwapi.Audience{}
	for _, h := range []gwapi.Hello{
		{View: gwapi.ViewSupervisor}, {View: gwapi.ViewSupervisor, Token: "full-token"},
		operatorHello(), {View: gwapi.ViewOperator, Languages: []string{"en"}, Token: "full-token"},
	} {
		c, w := e.connect(h, "10.0.0.1")
		conns[c] = gwapi.Audience{Level: w.Level, View: w.View}
	}
	for _, s := range history() {
		e.apply(s...)
		e.g.Tick(e.clock.Now())
	}
	for c, aud := range conns {
		for _, f := range c.drain(200 * time.Millisecond) {
			if _, err := gwapi.DecodeServer(f, aud); err != nil {
				t.Errorf("%v: %s: %v", aud, f, err)
			}
		}
		if code := c.closeCode(); code != 0 {
			t.Errorf("%v closed with %d", aud, code)
		}
	}
	if !strings.Contains(strings.Join(shape(t, []byte(`{"a":[{"b":1},{"c":2}]}`)), ","), ".a[].c") {
		t.Fatal("shape helper")
	}
}
