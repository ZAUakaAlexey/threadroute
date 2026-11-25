//go:build integration

package routersvc

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// testStandSettings: a started service seeds the defaults of the code and
// offers the stand settings of decision 0014 through the operator API, read
// and changed with the real client, and a restart keeps the stored set.
func testStandSettings(t *testing.T, h *harness) {
	sc := h.newScenario(t)
	r := sc.start()
	ctx := t.Context()
	api := r.api

	s, err := api.SettingsWithChanges(ctx)
	if err != nil || s.Stand == nil || s.Stand.Version != 1 || !s.Stand.Current.Equal(opapi.DefaultStandSettings()) ||
		len(s.Stand.Changes) != 1 || s.Stand.Changes[0].By != opapi.ByDefaults {
		t.Fatalf("Settings() = %+v, %v", s.Stand, err)
	}
	if v := counterOf(t, r, "threadroute_router_settings_changes_total", "defaults"); v != 1 {
		t.Errorf("settings_changes_total{source=defaults} = %v, want 1", v)
	}
	set := s.Stand.Current
	set.Teams.FormLoad = 30
	put, err := api.PutSettings(ctx, opapi.SettingsChangeRequest{Settings: set, BaseVersion: 1, By: "test-user-0001"})
	if err != nil || put.Result != opapi.ResultDone || put.Settings.Stand.Version != 2 {
		t.Fatalf("PutSettings() = %+v, %v", put, err)
	}
	full, err := api.SettingsWithChanges(ctx)
	if err != nil || len(full.Stand.Changes) != 2 || full.Stand.Changes[0].By != "test-user-0001" ||
		full.Stand.Changes[1].By != opapi.ByDefaults {
		t.Fatalf("SettingsWithChanges() = %+v, %v", full.Stand, err)
	}
	if got := r.svc.Core().Config().TeamFormLoad; got != 30 {
		t.Errorf("form load in use = %d, want 30", got)
	}
	if v := gaugeOf(t, r, "threadroute_router_settings_version"); v != 2 {
		t.Errorf("settings_version = %v, want 2", v)
	}
	if v := counterOf(t, r, "threadroute_router_settings_changes_total", "console"); v != 1 {
		t.Errorf("settings_changes_total{source=console} = %v, want 1", v)
	}

	// A restart keeps the stored set and seeds nothing.
	r.stop(t)
	again := sc.start()
	s, err = again.api.Settings(ctx)
	if err != nil || s.Stand.Version != 2 || s.Stand.Current.Teams.FormLoad != 30 || again.svc.Core().Config().TeamFormLoad != 30 {
		t.Fatalf("Settings() after a restart = %+v, %v", s.Stand, err)
	}
	if v := counterOf(t, again, "threadroute_router_settings_changes_total", "defaults"); v != 0 {
		t.Errorf("settings_changes_total{source=defaults} after a restart = %v, want 0", v)
	}
}

// testTeamTrim: a change that lowers the team share through the operator
// API trims the teams at the next check of the team loop (decision 0009,
// "A trim after the cap was lowered"), and the three trim counters count
// it. Teams have one member and the cap is off, so two companies with 3
// active conversations each form two teams on 4 online operators,
// whichever check forms which. A share of 0.25 gives a cap of 1: one team
// stays and the other is released, so the totals do not depend on the
// timing of the loop.
func testTeamTrim(t *testing.T, h *harness) {
	sc := h.newScenario(t)
	sc.rcfg.AcceptTimeout = longAcceptTimeout
	sc.rcfg.TeamReleaseAfter = opapi.MaxTeamReleaseAfter
	sc.rcfg.TeamFormLoad, sc.rcfg.TeamReleaseLoad, sc.rcfg.TeamMinSize, sc.rcfg.TeamMaxSize = 3, 1, 1, 1
	sc.rcfg.TeamMaxShare = 1
	r := sc.start()
	ctx := t.Context()
	for i := 1; i <= 4; i++ {
		sc.operator(opID(i), 4, "en")
	}
	rd := sc.reader()
	cos := []string{company(1), company(2)}
	for i, co := range cos {
		for j := range 3 {
			sc.produce(newStream(co, customer(i*10+j)).opened())
		}
	}
	for _, co := range cos {
		rd.waitEvent(co, "team formed", isType(event.TypeTeamFormed))
	}

	s, err := r.api.Settings(ctx)
	if err != nil || s.Stand == nil {
		t.Fatalf("Settings() = %+v, %v", s.Stand, err)
	}
	set := s.Stand.Current
	set.Teams.MaxShare = 0.25
	put, err := r.api.PutSettings(ctx, opapi.SettingsChangeRequest{Settings: set, BaseVersion: s.Stand.Version, By: "test-user-0001"})
	if err != nil || put.Result != opapi.ResultDone {
		t.Fatalf("PutSettings() = %+v, %v", put, err)
	}
	waitFor(t, 30*time.Second, "a trim", func() bool { return testutil.ToFloat64(r.svc.m.teamTrims) == 1 })
	released := rd.waitEvent(cos[1], "team released", isType(event.TypeTeamReleased))
	if released.ReleaseReason != event.TeamCapLowered {
		t.Errorf("team_released = %+v, want the reason %s", released, event.TeamCapLowered)
	}
	if got := testutil.ToFloat64(r.svc.m.trimOps); got != 1 {
		t.Errorf("team_trim_operators_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(r.svc.m.trimReleases); got != 1 {
		t.Errorf("team_trim_releases_total = %v, want 1", got)
	}
	// Later checks of the loop do not trim again.
	time.Sleep(2 * time.Second)
	if got := testutil.ToFloat64(r.svc.m.teamTrims); got != 1 {
		t.Errorf("team_trims_total after more checks = %v, want 1", got)
	}
	if n := sc.count(`SELECT count(*) FROM stand_settings WHERE trim_version IS NOT NULL`); n != 0 {
		t.Errorf("trim requests left = %d, want 0", n)
	}
}

// counterOf returns the value of a counter of the service registry with
// one label value.
func counterOf(t *testing.T, r *running, name, label string) float64 {
	t.Helper()
	mfs, err := r.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetValue() == label {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("metric %s{%s} not found", name, label)
	return 0
}

// gaugeOf returns the value of an unlabelled gauge of the service registry.
func gaugeOf(t *testing.T, r *running, name string) float64 {
	t.Helper()
	mfs, err := r.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name && len(mf.GetMetric()) == 1 {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}
