package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// untilResult reads frames until the Result of id and returns it with the
// frames before it. Every frame must pass validation for aud.
func untilResult(t *testing.T, c *fakeConn, aud gwapi.Audience, id string) (gwapi.Result, []gwapi.ServerMessage) {
	t.Helper()
	var before []gwapi.ServerMessage
	for {
		b := c.next(t)
		m, err := gwapi.DecodeServer(b, aud)
		if err != nil {
			t.Fatalf("frame %s: %v", b, err)
		}
		if r, ok := m.(gwapi.Result); ok && r.RequestID == id {
			return r, before
		}
		before = append(before, m)
	}
}

// settingsUpdates returns the SettingsUpdate messages of ms.
func settingsUpdates(ms []gwapi.ServerMessage) []gwapi.SettingsUpdate {
	var out []gwapi.SettingsUpdate
	for _, m := range ms {
		if u, ok := m.(gwapi.SettingsUpdate); ok {
			out = append(out, u)
		}
	}
	return out
}

// drainSettings reads the frames of a short time and returns the
// SettingsUpdate messages among them.
func drainSettings(t *testing.T, c *fakeConn, aud gwapi.Audience) []gwapi.SettingsUpdate {
	t.Helper()
	var ms []gwapi.ServerMessage
	for _, b := range c.drain(150 * time.Millisecond) {
		m, err := gwapi.DecodeServer(b, aud)
		if err != nil {
			t.Fatalf("frame %s: %v", b, err)
		}
		ms = append(ms, m)
	}
	return settingsUpdates(ms)
}

func audOf(w gwapi.Welcome) gwapi.Audience {
	may := w.Settings != nil && w.Settings.MayChange
	return gwapi.Audience{Level: w.Level, View: w.View, MayChangeSettings: may}
}

// change returns a ChangeSettings of the current set of st with mod
// applied.
func change(id string, st *opapi.StandState, mod func(s *opapi.StandSettings)) gwapi.ChangeSettings {
	next := cloneSet(st.Current)
	if mod != nil {
		mod(&next)
	}
	return gwapi.ChangeSettings{RequestID: id, BaseVersion: st.Version, Settings: next}
}

func TestWelcomeSettingsFollowTheRight(t *testing.T) {
	e, _ := newStandEnv(t, routerStand, testStand())
	tests := []struct {
		name  string
		hello gwapi.Hello
		level gwapi.Level
		may   bool
	}{
		{"anonymous supervisor", gwapi.Hello{View: gwapi.ViewSupervisor}, gwapi.LevelDemo, false},
		{"full level without the right", gwapi.Hello{View: gwapi.ViewSupervisor, Token: fullToken}, gwapi.LevelFull, false},
		{"demo level with the right", gwapi.Hello{View: gwapi.ViewSupervisor, Token: settingsToken}, gwapi.LevelDemo, true},
		{"full level with the right", gwapi.Hello{View: gwapi.ViewSupervisor, Token: fullSettingsToken}, gwapi.LevelFull, true},
		{"operator with the right", gwapi.Hello{View: gwapi.ViewOperator, Languages: []string{"en"}, Token: settingsToken}, gwapi.LevelDemo, true},
		{"operator without the right", operatorHello(), gwapi.LevelDemo, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, w := e.connect(tt.hello, "10.0.0.1")
			defer c.Close(gwapi.CloseNormal)
			if w.Level != tt.level || w.Settings == nil || w.Settings.MayChange != tt.may {
				t.Fatalf("welcome level %s settings %+v", w.Level, w.Settings)
			}
			s := w.Settings
			if s.Version != 3 || len(s.Changes) != 3 || s.Bounds != opapi.Bounds() || s.Current.Sim == nil || s.Current.Sim.Capacity != 6 {
				t.Fatalf("settings %+v", s)
			}
			for _, ch := range s.Changes {
				if tt.may && ch.By == "" || !tt.may && ch.By != "" {
					t.Errorf("change %d by %q with the right %v", ch.Version, ch.By, tt.may)
				}
			}
			if (w.Trend != nil) != (tt.hello.View == gwapi.ViewSupervisor) {
				t.Errorf("trend %+v for view %s", w.Trend, tt.hello.View)
			}
			if w.Trend != nil && (w.Trend.Points == nil || w.Trend.IntervalMS != 10000) {
				t.Errorf("trend %+v", w.Trend)
			}
		})
	}
}

// countHandler counts the log records at warning level and above.
type countHandler struct {
	mu sync.Mutex
	n  int
}

func (h *countHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		h.mu.Lock()
		h.n++
		h.mu.Unlock()
	}
	return nil
}
func (h *countHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countHandler) WithGroup(string) slog.Handler      { return h }

func (h *countHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

// TestRouterWithoutStandSettings: a router from before decision 0014 (no
// stand block, a bare 405 on PUT) and one that offers none give a gateway
// that works as before: no settings anywhere, no warning, settings_off
// without a router call.
func TestRouterWithoutStandSettings(t *testing.T) {
	for name, kind := range map[string]routerKind{"old": routerOld, "off": routerOff} {
		t.Run(name, func(t *testing.T) {
			logs := &countHandler{}
			e := newEnv(t)
			e.g.logger = slog.New(logs)
			r := newFakeRouter(t, kind, e.api.settings, nil)
			e.api.mu.Lock()
			e.api.router = r
			e.api.mu.Unlock()
			if _, err := e.g.RefreshSettings(t.Context()); err != nil {
				t.Fatal(err)
			}
			sup, ws := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: settingsToken}, "10.0.0.1")
			op, wo := e.connect(operatorHello(), "10.0.0.2")
			if ws.Settings != nil || wo.Settings != nil {
				t.Fatalf("settings in welcome: %+v %+v", ws.Settings, wo.Settings)
			}
			aud := gwapi.Audience{Level: gwapi.LevelDemo, View: gwapi.ViewSupervisor, MayChangeSettings: true}
			sup.send(t, change("c1", testStand(), nil))
			if res, _ := untilResult(t, sup, aud, "c1"); res.Code != gwapi.CodeSettingsOff {
				t.Fatalf("result %+v", res)
			}
			if puts := r.putRequests(); len(puts) != 0 {
				t.Fatalf("the router was asked: %+v", puts)
			}
			for range 3 {
				if _, err := e.g.RefreshSettings(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if got := drainSettings(t, op, gwapi.Audience{Level: gwapi.LevelDemo, View: gwapi.ViewOperator}); len(got) != 0 {
				t.Fatalf("settings messages %+v", got)
			}
			// The client decodes the answer of the router as settings_off.
			_, err := r.client.PutSettings(t.Context(), opapi.SettingsChangeRequest{Settings: testStand().Current, By: "user:1"})
			if code := opapiCode(err); code != opapi.CodeSettingsOff {
				t.Fatalf("PUT error %v", err)
			}
			if v := testutil.ToFloat64(e.g.m.standVersion); v != -1 {
				t.Errorf("version metric %v", v)
			}
			if n := logs.count(); n != 0 {
				t.Errorf("%d warnings or errors", n)
			}
			if sup.closeCode() != 0 || op.closeCode() != 0 {
				t.Errorf("closed %d %d", sup.closeCode(), op.closeCode())
			}
		})
	}
}

func opapiCode(err error) opapi.ErrorCode {
	if se, ok := errors.AsType[*opapi.SettingsError](err); ok {
		return se.Code
	}
	return ""
}

func TestChangeSettingsOutcomes(t *testing.T) {
	right := gwapi.Hello{View: gwapi.ViewSupervisor, Token: settingsToken}
	tests := []struct {
		name  string
		kind  routerKind
		stand func() *opapi.StandState
		hello gwapi.Hello
		// setup runs before the request.
		setup func(r *fakeRouter)
		req   func(st *opapi.StandState) gwapi.ChangeSettings
		want  gwapi.Result
		// fields are the wanted field names of an invalid request.
		fields []string
		// asked reports whether the router gets the PUT.
		asked bool
		// pushed is the version of a settings message before the Result,
		// 0 for none.
		pushed int64
	}{
		{name: "done", req: func(st *opapi.StandState) gwapi.ChangeSettings {
			return change("r", st, func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 })
		}, want: gwapi.Result{Outcome: gwapi.OutcomeDone}, asked: true, pushed: 4},
		{name: "already done", req: func(st *opapi.StandState) gwapi.ChangeSettings { return change("r", st, nil) },
			want: gwapi.Result{Outcome: gwapi.OutcomeAlreadyDone}, asked: true},
		{name: "invalid values", req: func(st *opapi.StandState) gwapi.ChangeSettings {
			return change("r", st, func(s *opapi.StandSettings) { s.Teams.ReleaseLoad = 30; s.Sim.Capacity = 25 })
		}, want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeInvalidRequest},
			// A value out of its bounds comes before a rule between fields.
			fields: []string{opapi.FieldSimCapacity, opapi.FieldTeamReleaseLoad}},
		{name: "sim part missing", req: func(st *opapi.StandState) gwapi.ChangeSettings {
			return change("r", st, func(s *opapi.StandSettings) { s.Sim = nil })
		}, want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeInvalidRequest}, fields: []string{opapi.FieldSim}},
		{name: "sim part before a simulator report", stand: func() *opapi.StandState { return firstStartStand(testTeams) },
			req: func(st *opapi.StandState) gwapi.ChangeSettings {
				return change("r", st, func(s *opapi.StandSettings) { sim := testSim; s.Sim = &sim })
			}, want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeInvalidRequest}, fields: []string{opapi.FieldSim}},
		{name: "invalid by the router", req: func(st *opapi.StandState) gwapi.ChangeSettings {
			// A base above the held version goes to the router, which
			// finds the sim part missing.
			c := change("r", st, func(s *opapi.StandSettings) { s.Sim = nil })
			c.BaseVersion = 4
			return c
		}, setup: func(r *fakeRouter) { r.change(func(s *opapi.StandSettings) { s.Teams.MinSize = 3 }, "user:1") },
			want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeInvalidRequest}, fields: []string{opapi.FieldSim}, asked: true},
		{name: "conflict seen by the gateway", req: func(st *opapi.StandState) gwapi.ChangeSettings {
			c := change("r", st, func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 })
			c.BaseVersion = 2
			return c
		}, want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeSettingsConflict}},
		{name: "conflict seen by the router", req: func(st *opapi.StandState) gwapi.ChangeSettings {
			return change("r", st, func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 })
		}, setup: func(r *fakeRouter) { r.change(func(s *opapi.StandSettings) { s.Teams.MinSize = 3 }, "user:1") },
			want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeSettingsConflict}, asked: true, pushed: 4},
		{name: "too soon", req: func(st *opapi.StandState) gwapi.ChangeSettings {
			return change("r", st, func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 })
		}, setup: func(r *fakeRouter) { r.setLastChange(time.Now().Add(-3200 * time.Millisecond)) },
			want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeSettingsTooSoon}, asked: true},
		{name: "not allowed", hello: gwapi.Hello{View: gwapi.ViewSupervisor, Token: fullToken},
			req:  func(st *opapi.StandState) gwapi.ChangeSettings { return change("r", st, nil) },
			want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeNotAllowed}},
		{name: "off", kind: routerOff, stand: func() *opapi.StandState { return nil },
			req:  func(*opapi.StandState) gwapi.ChangeSettings { return change("r", testStand(), nil) },
			want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeSettingsOff}},
		{name: "router unavailable", req: func(st *opapi.StandState) gwapi.ChangeSettings {
			return change("r", st, func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 })
		}, setup: func(r *fakeRouter) { r.setFail(opapi.CodeUnavailable) },
			want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeUnavailable}, asked: true},
		{name: "router gone", req: func(st *opapi.StandState) gwapi.ChangeSettings {
			return change("r", st, func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 })
		}, setup: func(r *fakeRouter) { r.srv.Close() },
			want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeUnavailable}},
		{name: "router error", req: func(st *opapi.StandState) gwapi.ChangeSettings {
			return change("r", st, func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 })
		}, setup: func(r *fakeRouter) { r.setFail(opapi.CodeInternal) },
			want: gwapi.Result{Outcome: gwapi.OutcomeRejected, Code: gwapi.CodeInternal}, asked: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stand := testStand()
			if tt.stand != nil {
				stand = tt.stand()
			}
			e, r := newStandEnv(t, tt.kind, stand)
			hello := right
			if tt.hello.View != "" {
				hello = tt.hello
			}
			c, w := e.connect(hello, "10.0.0.1")
			other, wo := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.2")
			if tt.setup != nil {
				tt.setup(r)
			}
			held := stand
			if s := e.g.settings.Load(); s.Stand != nil {
				held = s.Stand
			}
			c.send(t, tt.req(held))
			res, before := untilResult(t, c, audOf(w), "r")
			if res.Outcome != tt.want.Outcome || res.Code != tt.want.Code {
				t.Fatalf("result %+v, want %+v", res, tt.want)
			}
			var names []string
			for _, f := range res.Fields {
				names = append(names, f.Field)
			}
			if !slices.Equal(names, tt.fields) {
				t.Errorf("fields %v, want %v", names, tt.fields)
			}
			if tt.want.Code == gwapi.CodeSettingsTooSoon && (res.RetryAfterMS < 6000 || res.RetryAfterMS > 6800) {
				t.Errorf("retry after %d ms", res.RetryAfterMS)
			}
			if asked := len(r.putRequests()) > 0; asked != tt.asked {
				t.Errorf("router asked %v, want %v", asked, tt.asked)
			}
			ups := settingsUpdates(before)
			switch {
			case tt.pushed == 0 && len(ups) != 0:
				t.Errorf("settings before the result: %+v", ups)
			case tt.pushed != 0 && (len(ups) != 1 || ups[0].Settings.Version != tt.pushed):
				t.Errorf("settings before the result %+v, want version %d", ups, tt.pushed)
			}
			// The other connections get the same version at once.
			got := drainSettings(t, other, audOf(wo))
			if tt.pushed != 0 && (len(got) != 1 || got[0].Settings.Version != tt.pushed) {
				t.Errorf("other connection got %+v", got)
			}
			if tt.pushed == 0 && len(got) != 0 {
				t.Errorf("other connection got %+v", got)
			}
			if c.closeCode() != 0 {
				t.Errorf("closed with %d", c.closeCode())
			}
		})
	}
}

// TestChangeSettingsPush: a change reaches every connection at once, each
// with its own right: "by" only for a connection that may change settings.
func TestChangeSettingsPush(t *testing.T) {
	e, r := newStandEnv(t, routerStand, testStand())
	a, wa := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: settingsToken}, "10.0.0.1")
	b, wb := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: fullToken}, "10.0.0.2")
	op, wop := e.connect(operatorHello(), "10.0.0.3")
	a.send(t, change("r", testStand(), func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 }))
	res, before := untilResult(t, a, audOf(wa), "r")
	if res.Outcome != gwapi.OutcomeDone || len(settingsUpdates(before)) != 1 {
		t.Fatalf("result %+v after %+v", res, before)
	}
	mine := settingsUpdates(before)[0].Settings
	if mine.Version != 4 || mine.Changes[0].By != "user:9" || !mine.MayChange || mine.Current.Teams.MaxShare != 0.3 {
		t.Fatalf("own settings %+v", mine)
	}
	for _, x := range []struct {
		c *fakeConn
		w gwapi.Welcome
	}{{b, wb}, {op, wop}} {
		got := drainSettings(t, x.c, audOf(x.w))
		if len(got) != 1 || got[0].Settings.Version != 4 || got[0].Settings.MayChange || got[0].Settings.Changes[0].By != "" {
			t.Fatalf("%s got %+v", x.w.View, got)
		}
	}
	// The next poll finds the same set and pushes nothing.
	if _, err := e.g.RefreshSettings(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := drainSettings(t, b, audOf(wb)); len(got) != 0 {
		t.Fatalf("pushed again: %+v", got)
	}
	// A change by someone else reaches every connection with the poll.
	r.change(func(s *opapi.StandSettings) { s.Teams.MinSize = 3 }, "user:1")
	if _, err := e.g.RefreshSettings(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := drainSettings(t, b, audOf(wb)); len(got) != 1 || got[0].Settings.Version != 5 {
		t.Fatalf("after the poll %+v", got)
	}
	if v := testutil.ToFloat64(e.g.m.standVersion); v != 5 {
		t.Errorf("version metric %v", v)
	}
	if v := testutil.ToFloat64(e.g.m.settingsChanges.WithLabelValues("done")); v != 1 {
		t.Errorf("changes metric %v", v)
	}
}

// TestByLabel: the gateway builds the label, from the site user or the
// local role, never from client input.
func TestByLabel(t *testing.T) {
	t.Run("site user", func(t *testing.T) {
		e, r := newStandEnv(t, routerStand, testStand())
		c, w := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: fullSettingsToken}, "10.0.0.1")
		c.send(t, change("r", testStand(), func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 }))
		if res, _ := untilResult(t, c, audOf(w), "r"); res.Outcome != gwapi.OutcomeDone {
			t.Fatalf("result %+v", res)
		}
		if puts := r.putRequests(); len(puts) != 1 || puts[0].By != "user:10" {
			t.Fatalf("puts %+v", puts)
		}
	})
	t.Run("local role", func(t *testing.T) {
		e, r := newStandEnv(t, routerStand, testStand(), func(c *Config) {
			c.LocalRole, c.SettingsRoles = "guest", []string{"guest"}
			c.AllowedOrigins = []string{"http://127.0.0.1:5173"}
		})
		c, w := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
		if w.Level != gwapi.LevelDemo || !w.Settings.MayChange {
			t.Fatalf("welcome %+v", w)
		}
		c.send(t, change("r", testStand(), func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 }))
		if res, _ := untilResult(t, c, audOf(w), "r"); res.Outcome != gwapi.OutcomeDone {
			t.Fatalf("result %+v", res)
		}
		if puts := r.putRequests(); len(puts) != 1 || puts[0].By != ByLocal {
			t.Fatalf("puts %+v", puts)
		}
	})
	if (Identity{Level: gwapi.LevelDemo}).by() != "" {
		t.Error("an anonymous identity has a label")
	}
}

func TestChangeSettingsRateLimit(t *testing.T) {
	e, r := newStandEnv(t, routerStand, testStand())
	c, w := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: settingsToken}, "10.0.0.1")
	bad := change("", testStand(), func(s *opapi.StandSettings) { s.Teams.FormLoad = 1 })
	var last gwapi.Result
	for i := range gwapi.ActionBurst + 1 {
		bad.RequestID = "r" + string(rune('a'+i))
		c.send(t, bad)
		last, _ = untilResult(t, c, audOf(w), bad.RequestID)
	}
	if last.Code != gwapi.CodeRateLimited {
		t.Fatalf("last %+v", last)
	}
	if len(r.putRequests()) != 0 {
		t.Fatal("the router was asked")
	}
}

// TestSettingsOldReadDropped: a read that started before a newer swap
// cannot put the older set back.
func TestSettingsOldReadDropped(t *testing.T) {
	e, r := newStandEnv(t, routerStand, testStand())
	early := e.g.settingsSeq.Add(1)
	old, err := r.client.SettingsWithChanges(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r.change(func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 }, "user:1")
	if _, err := e.g.RefreshSettings(t.Context()); err != nil {
		t.Fatal(err)
	}
	e.g.applySettings(old, early)
	if v := e.g.settings.Load().Stand.Version; v != 4 {
		t.Fatalf("version %d after an old read", v)
	}
}

// TestSettingsPushRules: new defaults at the same version are pushed, and
// a block that appears later reaches a connection whose Welcome had none.
func TestSettingsPushRules(t *testing.T) {
	e, r := newStandEnv(t, routerStand, firstStartStand(testTeams))
	c, w := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
	teams := testTeams
	teams.FormLoad = 25
	r.set(firstStartStand(teams))
	if _, err := e.g.RefreshSettings(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := drainSettings(t, c, audOf(w))
	if len(got) != 1 || got[0].Settings.Version != 0 || got[0].Settings.Defaults.Teams.FormLoad != 25 {
		t.Fatalf("defaults push %+v", got)
	}

	off := newEnv(t)
	plain, wp := off.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
	s := off.api.settings
	s.Stand = testStand()
	off.g.SetSettings(s)
	if got := drainSettings(t, plain, audOf(wp)); len(got) != 1 || got[0].Settings.Version != 3 {
		t.Fatalf("a connection without a block got %+v", got)
	}
	_, later := off.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.2")
	if later.Settings == nil || later.Settings.Version != 3 {
		t.Fatalf("a new connection %+v", later.Settings)
	}
}

// TestRealRouterAnswersDecode checks the JSON of the fake router once
// against the opapi types, so the fake does not drift from the contract.
func TestRealRouterAnswersDecode(t *testing.T) {
	_, r := newStandEnv(t, routerStand, testStand())
	s, err := r.client.SettingsWithChanges(t.Context())
	if err != nil || s.Stand == nil || len(s.Stand.Changes) != 3 {
		t.Fatalf("%+v %v", s.Stand, err)
	}
	plain, err := r.client.Settings(t.Context())
	if err != nil || plain.Stand == nil || plain.Stand.Changes != nil {
		t.Fatalf("%+v %v", plain.Stand, err)
	}
	b, _ := json.Marshal(s)
	if len(b) > opapi.MaxSettingsBodyBytes {
		t.Fatalf("%d bytes", len(b))
	}
}
