package routersvc

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

// standSource is what the stand settings metrics read. *router.Service has
// it.
type standSource interface {
	StandSnapshot() router.StandSnapshot
}

// settingsCollector exports the stand settings in use (decision 0014,
// "Rules for measurements"). It reads the snapshot of the routing core at
// every scrape, so the values are the ones in use at that moment and need
// no update call:
//
//   - settings_value{name}: each value of the current set, by its field
//     name of opapi.SettingsFields. The sim values appear once a simulator
//     reported.
//   - settings_version: the version of the current set, 0 while nothing is
//     stored or when the router does not offer stand settings.
//   - settings_changes_total{source}: the changes this process applied, by
//     source (console or simulator).
type settingsCollector struct {
	src     standSource
	value   *prometheus.Desc
	version *prometheus.Desc
	changes *prometheus.Desc
}

func newSettingsCollector(src standSource) *settingsCollector {
	name := func(n string) string { return prometheus.BuildFQName(metricsNamespace, metricsSubsystem, n) }
	return &settingsCollector{
		src: src,
		value: prometheus.NewDesc(name("settings_value"),
			"Values of the stand settings in use, by field name (decision 0014). Durations are microseconds of model time.",
			[]string{"name"}, nil),
		version: prometheus.NewDesc(name("settings_version"),
			"Version of the stand settings in use, 0 while nothing is stored or when the router does not offer them.",
			nil, nil),
		changes: prometheus.NewDesc(name("settings_changes_total"),
			"Stand settings changes applied by this process, by source (console or simulator).",
			[]string{"source"}, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *settingsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.value
	ch <- c.version
	ch <- c.changes
}

// Collect implements prometheus.Collector.
func (c *settingsCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.src.StandSnapshot()
	for _, v := range router.SettingsValues(snap.Current) {
		ch <- prometheus.MustNewConstMetric(c.value, prometheus.GaugeValue, v.Value, v.Field)
	}
	ch <- prometheus.MustNewConstMetric(c.version, prometheus.GaugeValue, float64(snap.Version))
	ch <- prometheus.MustNewConstMetric(c.changes, prometheus.CounterValue, float64(snap.ConsoleChanges), router.SourceConsole)
	ch <- prometheus.MustNewConstMetric(c.changes, prometheus.CounterValue, float64(snap.SimulatorChanges), router.SourceSimulator)
}
