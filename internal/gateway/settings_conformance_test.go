package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// The examples of decision 0014 live in testdata/settings with their own
// index, whose entries also name the right of the connection. This test
// covers every entry of that index the way TestFixtureConformance covers
// the main one: a server example must have the shape of a message the
// gateway makes from synthetic input for the same audience, and a client
// example must be answered with a Result, not a close.

const settingsTestdata = testdata + "/settings"

type settingsFixtureEntry struct {
	fixtureEntry
	MayChangeSettings bool `json:"may_change_settings"`
}

func loadSettingsFixtures(t *testing.T) []settingsFixtureEntry {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(settingsTestdata, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var idx []settingsFixtureEntry
	if err := json.Unmarshal(b, &idx); err != nil {
		t.Fatal(err)
	}
	return idx
}

// standWelcome connects with hello to a gateway with testStand and two
// trend points, and returns the Welcome.
func standWelcome(t *testing.T, hello gwapi.Hello) []byte {
	t.Helper()
	e, _ := newStandEnv(t, routerStand, testStand())
	trendWorld(e)
	e.g.RecordTrend(t0)
	e.g.RecordTrend(t0.Add(gwapi.TrendInterval))
	c := newFakeConn()
	e.wg.Go(func() { e.g.Serve(e.ctx, c, "10.0.0.1") })
	hello.Version = gwapi.ProtocolVersion
	c.send(t, hello)
	return c.nextOf(t, gwapi.TypeWelcome)
}

// standPush connects with hello, lets the router change the set and returns
// the settings message of the poll that follows.
func standPush(t *testing.T, stand *opapi.StandState, hello gwapi.Hello, mod func(r *fakeRouter)) []byte {
	t.Helper()
	e, r := newStandEnv(t, routerStand, stand)
	c, _ := e.connect(hello, "10.0.0.1")
	mod(r)
	if _, err := e.g.RefreshSettings(t.Context()); err != nil {
		t.Fatal(err)
	}
	return c.nextOf(t, gwapi.TypeSettings)
}

// standResult sends a change on a connection made with hello and returns
// the Result.
func standResult(t *testing.T, kind routerKind, stand *opapi.StandState, hello gwapi.Hello, setup func(r *fakeRouter), req gwapi.ChangeSettings) []byte {
	t.Helper()
	e, r := newStandEnv(t, kind, stand)
	c, _ := e.connect(hello, "10.0.0.1")
	if setup != nil {
		setup(r)
	}
	c.send(t, req)
	return c.nextOf(t, gwapi.TypeResult)
}

func TestSettingsFixtureConformance(t *testing.T) {
	sup := gwapi.Hello{View: gwapi.ViewSupervisor}
	right := gwapi.Hello{View: gwapi.ViewSupervisor, Token: settingsToken}
	maxShare := func(r *fakeRouter) { r.change(func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 }, "user:9") }
	share := change("req-settings", testStand(), func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 })
	makers := map[string]func(t *testing.T) []byte{
		"server_welcome_supervisor_settings_demo.json": func(t *testing.T) []byte { return standWelcome(t, sup) },
		"server_welcome_supervisor_settings_full_may_change.json": func(t *testing.T) []byte {
			return standWelcome(t, gwapi.Hello{View: gwapi.ViewSupervisor, Token: fullSettingsToken})
		},
		"server_welcome_operator_settings_demo.json":  func(t *testing.T) []byte { return standWelcome(t, operatorHello()) },
		"server_settings_update_demo.json":            func(t *testing.T) []byte { return standPush(t, testStand(), sup, maxShare) },
		"server_settings_update_may_change_demo.json": func(t *testing.T) []byte { return standPush(t, testStand(), right, maxShare) },
		"server_settings_first_start_demo.json": func(t *testing.T) []byte {
			return standPush(t, firstStartStand(testTeams), operatorHello(), func(r *fakeRouter) {
				teams := testTeams
				teams.FormLoad = 25
				r.set(firstStartStand(teams))
			})
		},
		"server_trend_demo.json": func(t *testing.T) []byte {
			e := newEnv(t)
			c, _ := e.connect(sup, "10.0.0.1")
			trendWorld(e)
			e.g.RecordTrend(t0)
			return c.nextOf(t, gwapi.TypeTrend)
		},
		"server_result_settings_conflict.json": func(t *testing.T) []byte {
			req := share
			req.BaseVersion = 2
			return standResult(t, routerStand, testStand(), right, nil, req)
		},
		"server_result_settings_invalid.json": func(t *testing.T) []byte {
			return standResult(t, routerStand, testStand(), right, nil,
				change("req-settings", testStand(), func(s *opapi.StandSettings) { s.Teams.ReleaseLoad = 30; s.Sim.Capacity = 25 }))
		},
		"server_result_settings_too_soon.json": func(t *testing.T) []byte {
			return standResult(t, routerStand, testStand(), right,
				func(r *fakeRouter) { r.setLastChange(time.Now().Add(-3200 * time.Millisecond)) }, share)
		},
		"server_result_settings_not_allowed.json": func(t *testing.T) []byte {
			return standResult(t, routerStand, testStand(), sup, nil, share)
		},
		"server_result_settings_off.json": func(t *testing.T) []byte {
			return standResult(t, routerOff, nil, right, nil, share)
		},
	}
	seen := 0
	for _, f := range loadSettingsFixtures(t) {
		want, err := os.ReadFile(filepath.Join(settingsTestdata, f.File))
		if err != nil {
			t.Fatal(err)
		}
		if f.Direction == "client" {
			t.Run(f.File, func(t *testing.T) { clientSettingsFixture(t, f, want) })
			continue
		}
		seen++
		t.Run(f.File, func(t *testing.T) {
			mk, ok := makers[f.File]
			if !ok {
				t.Fatalf("no conformance case for %s", f.File)
			}
			got := mk(t)
			aud := gwapi.Audience{Level: f.Level, View: f.View, MayChangeSettings: f.MayChangeSettings}
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

// clientSettingsFixture sends a client example as it is, on a connection
// that may change settings, and expects one Result for its request ID.
func clientSettingsFixture(t *testing.T, f settingsFixtureEntry, raw []byte) {
	t.Helper()
	m, err := gwapi.DecodeClient(raw)
	if err != nil || string(m.ClientType()) != f.Type {
		t.Fatalf("decode %s: %v", raw, err)
	}
	cs := m.(gwapi.ChangeSettings)
	e, _ := newStandEnv(t, routerStand, testStand())
	c, w := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: settingsToken}, "10.0.0.1")
	c.in <- raw
	res, _ := untilResult(t, c, audOf(w), cs.RequestID)
	if res.Outcome == gwapi.OutcomeRejected && res.Code == gwapi.CodeInternal {
		t.Fatalf("result %+v", res)
	}
	if code := c.closeCode(); code != 0 {
		t.Fatalf("closed with %d", code)
	}
}
