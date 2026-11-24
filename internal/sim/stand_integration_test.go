//go:build integration

package sim

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

// TestIntegrationStandSettings runs the simulator against a real router
// with stand settings (decision 0014): the first report becomes version 1,
// a change from the console is applied on the run, and after a restart the
// stored values win over the environment.
func TestIntegrationStandSettings(t *testing.T) {
	env := startRouter(t)
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
	sim := env.startSimWith(b)
	itWait(t, 30*time.Second, "10 operators online with capacity 2", func() bool { return online(2) == 10 })
	set, err := client.Settings(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if set.Stand == nil || set.Stand.Version != 1 || set.Stand.Current.Sim == nil ||
		*set.Stand.Current.Sim != simOf(b, set.CapacityDefault) {
		t.Fatalf("stand after the first report: %+v", set.Stand)
	}

	next := set.Stand.Current
	nextSim := *next.Sim
	nextSim.Operators, nextSim.Capacity = 13, 3
	next.Sim = &nextSim
	resp, err := client.PutSettings(t.Context(), opapi.SettingsChangeRequest{Settings: next, BaseVersion: 1, By: "it-test"})
	if err != nil || resp.Result != opapi.ResultDone {
		t.Fatalf("PutSettings() = %+v, %v", resp.Result, err)
	}
	itWait(t, 30*time.Second, "13 operators online with capacity 3", func() bool { return online(3) == 13 })
	itWait(t, 10*time.Second, "version metric 2", func() bool { return testutil.ToFloat64(sim.svc.m.settingsVersion) == 2 })
	if got := testutil.ToFloat64(sim.svc.m.settingsChanges); got != 1 {
		t.Errorf("changes metric %v, want 1", got)
	}

	// A restart reports the environment again; the stored set wins.
	sim.stop(t)
	itWait(t, 30*time.Second, "every operator offline after the stop", func() bool {
		return env.count(`SELECT count(*) FROM operators WHERE presence = 'online'`) == 0
	})
	env.startSimWith(b)
	itWait(t, 30*time.Second, "13 operators online with capacity 3 after the restart", func() bool { return online(3) == 13 })
	set, err = client.Settings(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if set.Stand == nil || set.Stand.Version != 2 || *set.Stand.Current.Sim != nextSim ||
		*set.Stand.Defaults.Sim != simOf(b, set.CapacityDefault) {
		t.Errorf("stand after the restart: %+v", set.Stand)
	}
}
