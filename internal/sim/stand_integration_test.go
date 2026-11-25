//go:build integration

package sim

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

// TestIntegrationStandSettings runs the simulator against a real router
// with stand settings (decision 0014): the router seeds the defaults of the
// code, the simulator starts with the stored set (not with its config), a
// change from the console is applied on the run, and after a restart the
// stored values still win. The simulator never writes the settings.
func TestIntegrationStandSettings(t *testing.T) {
	env := startRouterWith(t, nil)
	client, err := opclient.New(env.apiURL)
	if err != nil {
		t.Fatal(err)
	}
	b := DefaultBehaviour()
	b.Operators = 10
	b.Languages = Spread{{Lang: "en", Count: 2}, {Lang: "ja", Count: 1}}
	b.Capacity = 2
	// Every operator online for the whole test.
	b.ShiftOnline, b.ShiftOffline = 1000*time.Hour, time.Nanosecond
	b.Seed = 7

	online := func(capacity int) int64 {
		return env.count(`SELECT count(*) FROM operators WHERE presence = 'online' AND id LIKE 'sim-op-%' AND capacity = $1`, capacity)
	}
	set, err := client.SettingsWithChanges(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	def := opapi.DefaultStandSettings()
	if set.Stand == nil || set.Stand.Version != 1 || !set.Stand.Current.Equal(def) || !set.Stand.Defaults.Equal(def) ||
		len(set.Stand.Changes) != 1 || set.Stand.Changes[0].By != opapi.ByDefaults {
		t.Fatalf("stand after the seed: %+v", set.Stand)
	}

	next := set.Stand.Current
	nextSim := *next.Sim
	// change stores a sim part with the operator count and the capacity.
	change := func(base int64, operators, capacity int) {
		t.Helper()
		v := nextSim
		v.Operators, v.Capacity = operators, capacity
		next.Sim = &v
		// The router takes one change in 10 seconds.
		var resp opapi.SettingsChangeResponse
		var err error
		itWait(t, 30*time.Second, "a change that is not too soon", func() bool {
			resp, err = client.PutSettings(t.Context(), opapi.SettingsChangeRequest{Settings: next, BaseVersion: base, By: "it-test"})
			se, ok := errors.AsType[*opapi.SettingsError](err)
			return !ok || se.Code != opapi.CodeSettingsTooSoon
		})
		if err != nil || resp.Result != opapi.ResultDone {
			t.Fatalf("PutSettings(%d operators) = %+v, %v", operators, resp.Result, err)
		}
		nextSim = v
	}
	// The stored set differs from the config of the simulator (10
	// operators, capacity 2) and from the defaults.
	change(1, 12, 2)
	sim := env.startSimWith(b)
	itWait(t, 30*time.Second, "12 operators online with capacity 2", func() bool { return online(2) == 12 })
	if got := testutil.ToFloat64(sim.svc.m.settingsVersion); got != 2 {
		t.Errorf("version metric %v, want 2", got)
	}

	change(2, 13, 3)
	itWait(t, 30*time.Second, "13 operators online with capacity 3", func() bool { return online(3) == 13 })
	itWait(t, 10*time.Second, "version metric 3", func() bool { return testutil.ToFloat64(sim.svc.m.settingsVersion) == 3 })
	if got := testutil.ToFloat64(sim.svc.m.settingsChanges); got != 1 {
		t.Errorf("changes metric %v, want 1", got)
	}

	// A restart reads the stored set again.
	sim.stop(t)
	itWait(t, 30*time.Second, "every operator offline after the stop", func() bool {
		return env.count(`SELECT count(*) FROM operators WHERE presence = 'online'`) == 0
	})
	sim2 := env.startSimWith(b)
	itWait(t, 30*time.Second, "13 operators online with capacity 3 after the restart", func() bool { return online(3) == 13 })
	set, err = client.SettingsWithChanges(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if set.Stand == nil || set.Stand.Version != 3 || *set.Stand.Current.Sim != nextSim ||
		*set.Stand.Defaults.Sim != opapi.DefaultSimSettings() || len(set.Stand.Changes) != 3 {
		t.Errorf("stand after the restart: %+v", set.Stand)
	}
	for _, c := range set.Stand.Changes {
		if c.By == opapi.BySimulator {
			t.Errorf("the simulator wrote a change: %+v", c)
		}
	}

	// The languages stored in the router never change: a lower count, a
	// restart at it, then a higher count (decision 0014, point 3A). Each
	// operator has the language of its number.
	order := b.Languages.langOrder(MaxOperators)
	langs := map[string]string{}
	checkLangs := func(stage string, want int) {
		t.Helper()
		got := env.simLangs()
		if len(got) != want {
			t.Errorf("%s: %d operators stored, want %d", stage, len(got), want)
		}
		for id, l := range got {
			var num int
			if _, err := fmt.Sscanf(id, "sim-op-%04d", &num); err != nil {
				t.Fatal(err)
			}
			if l != order[num-1] {
				t.Errorf("%s: %s has %s, want %s", stage, id, l, order[num-1])
			}
			if old, ok := langs[id]; ok && old != l {
				t.Errorf("%s: %s changed from %s to %s", stage, id, old, l)
			}
			langs[id] = l
		}
	}
	checkLangs("after the first restart", 13)
	// The router allows counts from 10 on.
	change(3, 11, 3)
	itWait(t, 30*time.Second, "11 operators online", func() bool {
		return online(3) == 11 && env.count(`SELECT count(*) FROM operators WHERE presence = 'online'`) == 11
	})
	checkLangs("after 11", 13)

	sim2.stop(t)
	itWait(t, 30*time.Second, "every operator offline after the second stop", func() bool {
		return env.count(`SELECT count(*) FROM operators WHERE presence = 'online'`) == 0
	})
	sim3 := env.startSimWith(b)
	itWait(t, 30*time.Second, "11 operators online after the restart at 11", func() bool { return online(3) == 11 })
	// The numbers 12 and 13 of the earlier run count as put aside, and the
	// 404 of number 14 that ends the search is not a failed call.
	if got := testutil.ToFloat64(sim3.svc.m.operatorsAside); got != 2 {
		t.Errorf("put aside metric after the restart at 11: %v, want 2", got)
	}
	if got := testutil.ToFloat64(sim3.svc.m.calls.WithLabelValues(actionOffline, resultFailed)); got != 0 {
		t.Errorf("failed offline calls after the restart: %v, want 0", got)
	}
	checkLangs("after the restart at 11", 13)

	change(4, 16, 3)
	itWait(t, 30*time.Second, "16 operators online", func() bool { return online(3) == 16 })
	checkLangs("after 16", 16)
	if got := testutil.ToFloat64(sim3.svc.m.operatorsAside); got != 0 {
		t.Errorf("put aside metric at 16: %v, want 0", got)
	}
}

// simLangs returns the language of the spread of every simulated operator
// stored in the router, without "und".
func (env *itEnv) simLangs() map[string]string {
	env.t.Helper()
	rows, err := env.pool.Query(env.t.Context(),
		`SELECT operator_id, lang FROM operator_languages WHERE operator_id LIKE 'sim-op-%' AND lang <> 'und'`)
	if err != nil {
		env.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, lang string
		if err := rows.Scan(&id, &lang); err != nil {
			env.t.Fatal(err)
		}
		if old, ok := out[id]; ok {
			env.t.Errorf("%s has two languages besides und: %s and %s", id, old, lang)
		}
		out[id] = lang
	}
	if err := rows.Err(); err != nil {
		env.t.Fatal(err)
	}
	return out
}
