package routersvc

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

type fakeStandSource struct{ snap router.StandSnapshot }

func (f *fakeStandSource) StandSnapshot() router.StandSnapshot { return f.snap }

func TestSettingsCollector(t *testing.T) {
	teams := opapi.TeamSettings{FormLoad: 20, ReleaseLoad: 8, ReleaseAfterUS: 900_000_000, MinSize: 2, MaxSize: 20, MaxShare: 0.5}
	src := &fakeStandSource{snap: router.StandSnapshot{Enabled: true, Version: 3, Current: opapi.StandSettings{Teams: teams},
		ConsoleChanges: 2, DefaultsChanges: 1}}
	reg := prometheus.NewRegistry()
	if err := reg.Register(newSettingsCollector(src)); err != nil {
		t.Fatal(err)
	}
	const head = `
# HELP threadroute_router_settings_changes_total Stand settings changes applied by this process, by source (console, or defaults for the defaults stored at start).
# TYPE threadroute_router_settings_changes_total counter
threadroute_router_settings_changes_total{source="console"} 2
threadroute_router_settings_changes_total{source="defaults"} 1
# HELP threadroute_router_settings_version Version of the stand settings in use, 0 when the router does not offer them.
# TYPE threadroute_router_settings_version gauge
threadroute_router_settings_version 3
`
	want := head + `# HELP threadroute_router_settings_value Values of the stand settings in use, by field name (decision 0014). Durations are microseconds of model time.
# TYPE threadroute_router_settings_value gauge
threadroute_router_settings_value{name="teams.form_load"} 20
threadroute_router_settings_value{name="teams.max_share"} 0.5
threadroute_router_settings_value{name="teams.max_size"} 20
threadroute_router_settings_value{name="teams.min_size"} 2
threadroute_router_settings_value{name="teams.release_after_us"} 9e+08
threadroute_router_settings_value{name="teams.release_load"} 8
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want)); err != nil {
		t.Error(err)
	}

	// With a sim part its values appear too, and a scrape reads the
	// snapshot of that moment.
	sim := opapi.SimSettings{Operators: 200, Capacity: 4, HandlingMedianUS: 45_000_000, ResolveMedianUS: 180_000_000}
	src.snap.Current.Sim = &sim
	src.snap.Version = 4
	n, err := testutil.GatherAndCount(reg, "threadroute_router_settings_value")
	if err != nil || n != len(opapi.SettingsFields) {
		t.Errorf("settings_value series = %d, %v; want %d", n, err, len(opapi.SettingsFields))
	}
	if err := testutil.GatherAndCompare(reg, strings.NewReader(strings.Replace(head, "version 3", "version 4", 1)),
		"threadroute_router_settings_version", "threadroute_router_settings_changes_total"); err != nil {
		t.Error(err)
	}
}
