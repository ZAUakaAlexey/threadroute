package gateway

import (
	"slices"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// The trend strip (decision 0014, point 5A): one point every
// gwapi.TrendInterval, the last gwapi.TrendHistory kept in memory, taken
// from the view of the gateway, the same source as the supervisor state.
// A gateway restart loses the history.

// trendIntervalMS is gwapi.TrendInterval in milliseconds, for Welcome.
var trendIntervalMS = int(gwapi.TrendInterval.Milliseconds())

// RecordTrend takes one trend point at now, keeps it and sends it to every
// supervisor client. It takes no point while the data is stale: during the
// catch-up after a start the view is not the live state yet, and while the
// topics cannot be read or lag it would show old numbers as new. It reports
// whether it took a point.
func (g *Gateway) RecordTrend(now time.Time) bool {
	if g.Status().Stale {
		return false
	}
	speed := g.timing().SpeedFactor
	var p gwapi.TrendPoint
	g.view.Read(func(rd Reader) { p = rd.TrendPoint(now, speed) })

	g.pushMu.Lock()
	defer g.pushMu.Unlock()
	// Points must be ordered by time, or every later Welcome fails its
	// validation. A clock that went back gives no point.
	if n := len(g.trend); n > 0 && p.SampledAtUS <= g.trend[n-1].SampledAtUS {
		return false
	}
	g.trend = append(g.trend, p)
	oldest := p.SampledAtUS - gwapi.TrendHistory.Microseconds()
	drop := 0
	for drop < len(g.trend) && (g.trend[drop].SampledAtUS <= oldest || len(g.trend)-drop > gwapi.MaxTrendPoints) {
		drop++
	}
	g.trend = slices.Delete(g.trend, 0, drop)
	g.m.trendPoints.Inc()
	g.fanOut(func(c *client) bool { return c.aud.View == gwapi.ViewSupervisor },
		func(gwapi.Audience) gwapi.ServerMessage { return gwapi.TrendUpdate{Point: p} })
	return true
}

// trendHistory returns a copy of the kept points, oldest first.
func (g *Gateway) trendHistory() []gwapi.TrendPoint {
	g.pushMu.Lock()
	defer g.pushMu.Unlock()
	return append([]gwapi.TrendPoint{}, g.trend...)
}

// TrendPoint returns the trend point of the view at the machine time now.
// It reads the same rows as the supervisor state: Waiting is
// Totals.Conversations.Waiting, LongestWaitUS follows the oldest queue time
// of the queue rows, Teams and OperatorsInTeams follow the team rows, and
// OperatorsOnline is Totals.OperatorsOnline.
func (rd Reader) TrendPoint(now time.Time, speed float64) gwapi.TrendPoint {
	v := rd.v
	nowUS := now.UnixMicro()
	p := gwapi.TrendPoint{SampledAtUS: nowUS, Waiting: v.counts[gwapi.StateWaiting]}
	var oldest int64
	for _, keys := range v.queued {
		for key := range keys {
			if r := v.convs[key]; r != nil && r.queuedAtUS > 0 && (oldest == 0 || r.queuedAtUS < oldest) {
				oldest = r.queuedAtUS
			}
		}
	}
	if oldest > 0 {
		if speed <= 0 {
			speed = 1
		}
		p.LongestWaitUS = max(int64(float64(nowUS-oldest)*speed), 0)
	}
	p.ResolvedLastMinute = v.resolvedLastMinute(nowUS)
	for _, t := range v.teams {
		if !t.released {
			p.Teams++
			p.OperatorsInTeams += uint64(len(t.members))
		}
	}
	for _, o := range v.ops {
		if o.presence == event.PresenceOnline {
			p.OperatorsOnline++
		}
	}
	return p
}

// resolveWindow is the window of TrendPoint.ResolvedLastMinute, in
// seconds.
const resolveWindow = 60

// countResolve counts one resolve at the machine time atUS of the router.
// The caller holds the write lock.
func (v *View) countResolve(atUS int64) {
	sec := atUS / 1_000_000
	b := &v.resolves[sec%int64(len(v.resolves))]
	switch {
	case sec < b.sec:
		// Older than the bucket holds: outside the window anyway.
		return
	case sec > b.sec:
		b.sec, b.n = sec, 0
	}
	b.n++
}

// resolvedLastMinute counts the resolves of the last resolveWindow seconds
// before nowUS. The caller holds a lock.
func (v *View) resolvedLastMinute(nowUS int64) uint64 {
	sec := nowUS / 1_000_000
	var n int64
	for _, b := range v.resolves {
		if b.sec > sec-resolveWindow && b.sec <= sec {
			n += b.n
		}
	}
	return uint64(n)
}
