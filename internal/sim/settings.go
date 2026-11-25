package sim

import (
	"context"
	"fmt"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

// Stand settings of decision 0014, the simulator side (points 2A and 3A).
// The router keeps the sim part, and the simulator only reads it: at start
// from the settings it read anyway, and then every
// opapi.SettingsPollInterval, applying a new sim part on the run.

// standMode is how the simulator takes its sim values.
type standMode uint8

const (
	// standOff: the router offers no stand settings, or its stand block has
	// no sim part. The simulator runs with the sim values of
	// Config.Behaviour, the defaults of the code. It still reads the
	// settings every poll interval, to notice that they came on.
	standOff standMode = iota
	// standLive: the simulator runs with the stored sim part and applies
	// every new one.
	standLive
)

// stand is the state of the stand settings. Only the start and then the
// poll goroutine touch it.
type stand struct {
	mode standMode
	// version and sim are the stand version and the sim part last applied
	// in standLive.
	version int64
	sim     opapi.SimSettings
	// note is the last problem of the poll that was logged, so a poll every
	// few seconds does not repeat a log line.
	note string
	// outage is the outage clock of the poll. It is apart from the one of
	// the workers, so a poll that works never hides operator calls that
	// fail, and the other way round.
	outage outageClock
}

// simOf returns the sim part of a behaviour. A capacity of 0 takes
// capacityDefault.
func simOf(b Behaviour, capacityDefault int) opapi.SimSettings {
	c := b.Capacity
	if c == 0 {
		c = capacityDefault
	}
	return opapi.SimSettings{
		Operators:        b.Operators,
		Capacity:         c,
		HandlingMedianUS: b.HandlingMedian.Microseconds(),
		ResolveMedianUS:  b.ResolveMedian.Microseconds(),
		MissedOfferShare: b.MissedOfferShare,
	}
}

// startStand returns the mode the simulator starts in, from the router
// settings read at start: standLive with the stored sim part when the stand
// block has one, and standOff otherwise, with one warning.
func (s *Service) startStand(settings opapi.Settings) *stand {
	st := &stand{}
	if settings.Stand == nil || settings.Stand.Current.Sim == nil {
		s.logger.Warn("the router offers no stand settings; running with the default sim values of the code and checking each poll whether they come on",
			"poll", s.pollEvery.String(), "defaults", simAttrs(simOf(s.cfg.Behaviour, settings.CapacityDefault)))
		return st
	}
	st.mode, st.version, st.sim = standLive, settings.Stand.Version, *settings.Stand.Current.Sim
	s.logger.Info("running with the stored sim values", "version", st.version, "stored", simAttrs(st.sim))
	return st
}

// startBehaviour returns the behaviour the engine starts with:
// Config.Behaviour, with the stored sim part in standLive. When the stored
// count is below the number of languages, the count of Config.Behaviour
// stays, with a warning.
func (s *Service) startBehaviour(st *stand) Behaviour {
	b := s.cfg.Behaviour
	if st.mode != standLive {
		return b
	}
	b.Capacity = st.sim.Capacity
	b.HandlingMedian = st.sim.HandlingMedian()
	b.ResolveMedian = st.sim.ResolveMedian()
	b.MissedOfferShare = st.sim.MissedOfferShare
	if err := b.Languages.Validate(st.sim.Operators); err != nil {
		s.logger.Warn("the stored operator count does not fit the language spread; the default count stays",
			"stored", st.sim.Operators, "operators", b.Operators, "languages", b.Languages.String(), "error", err)
		return b
	}
	b.Operators = st.sim.Operators
	return b
}

// pollStand reads the settings every poll interval until ctx ends, and
// applies a new sim part. It returns an error that wraps ErrOutage when the
// poll has had no answer of the router for the outage limit.
func (s *Service) pollStand(ctx context.Context, eng *engine, st *stand) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.clock.After(s.pollEvery):
		}
		if err := s.pollOnce(ctx, eng, st); err != nil {
			return err
		}
	}
}

// pollOnce reads the settings once and acts on the answer. A failed read
// never changes the applied values.
func (s *Service) pollOnce(ctx context.Context, eng *engine, st *stand) error {
	actx, cancel := context.WithTimeout(ctx, opapi.SettingsPollInterval)
	set, err := s.api.Settings(actx)
	cancel()
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return s.pollFailed(st, "settings poll", err)
	}
	st.outage.reset()
	if set.Stand == nil || set.Stand.Current.Sim == nil {
		if st.mode == standLive {
			s.note(st, "the router has no stand settings now; the applied sim values stay")
		} else {
			s.note(st, "")
		}
		return nil
	}
	s.note(st, "")
	if st.mode == standOff {
		s.logger.Info("stand settings came on at the router; running with the stored sim values")
	}
	if st.mode == standOff || set.Stand.Version != st.version {
		s.applyStand(eng, st, set.Stand.Version, *set.Stand.Current.Sim)
	}
	return nil
}

// pollFailed handles an error of the poll. No answer and CodeUnavailable
// count on the outage clock of the poll; any other error answer is logged
// once.
func (s *Service) pollFailed(st *stand, what string, err error) error {
	code := opclient.Code(err)
	if code == "" || code == opapi.CodeUnavailable {
		if failing := st.outage.failing(s.clock.Now()); failing >= s.cfg.OutageLimit {
			return fmt.Errorf("%s: %w: no answer for %s (limit %s): %w",
				what, ErrOutage, failing.Round(time.Millisecond), s.cfg.OutageLimit, err)
		}
		s.note(st, what+" got no answer; the applied sim values stay", "error", err)
		return nil
	}
	st.outage.reset()
	s.note(st, what+" failed; the applied sim values stay", "code", code, "error", err)
	return nil
}

// note logs a problem of the poll once, at warning level, and once more at
// info level when the poll is fine again (msg is empty).
func (s *Service) note(st *stand, msg string, args ...any) {
	switch msg {
	case st.note:
		return
	case "":
		s.logger.Info("stand settings poll is fine again")
	default:
		s.logger.Warn(msg, args...)
	}
	st.note = msg
}

// applyStand applies a sim part with its version. The same values as the
// applied ones only update the version: a change of the teams part also
// changes the version.
func (s *Service) applyStand(eng *engine, st *stand, version int64, sim opapi.SimSettings) {
	wasLive := st.mode == standLive
	st.mode, st.version = standLive, version
	s.m.settingsVersion.Set(float64(version))
	if wasLive && sim == st.sim {
		return
	}
	st.sim = sim
	before := s.inEffect(eng)
	r := eng.applySim(sim)
	after := s.inEffect(eng)
	s.setValues(after)
	if r.countErr != nil {
		s.logger.Warn("the operator count was not applied, the other sim values were", "version", version, "error", r.countErr)
	}
	if r.full {
		s.logger.Warn("not every operator could be created: the operator numbers ran out", "max", MaxOperators)
	}
	if after == before {
		return
	}
	s.m.settingsChanges.Inc()
	s.logger.Info("sim values changed", "version", version, "before", simAttrs(before), "after", simAttrs(after),
		"put_aside", r.putAside, "back", r.back, "created", r.created, "capacity_puts", r.capacityPuts)
}

// inEffect returns the sim values the engine runs with, with the router
// default for a capacity of 0.
func (s *Service) inEffect(eng *engine) opapi.SimSettings {
	v := eng.inEffect()
	if v.Capacity == 0 {
		v.Capacity = s.capacityDefault
	}
	return v
}

// setValues sets the value gauges of the sim fields.
func (s *Service) setValues(v opapi.SimSettings) {
	for _, f := range []struct {
		name string
		v    float64
	}{
		{opapi.FieldSimOperators, float64(v.Operators)},
		{opapi.FieldSimCapacity, float64(v.Capacity)},
		{opapi.FieldSimHandlingMedian, float64(v.HandlingMedianUS)},
		{opapi.FieldSimResolveMedian, float64(v.ResolveMedianUS)},
		{opapi.FieldSimMissedOfferShare, v.MissedOfferShare},
	} {
		s.m.settingsValue.WithLabelValues(f.name).Set(f.v)
	}
}

// simAttrs returns sim values as log attributes.
func simAttrs(v opapi.SimSettings) map[string]any {
	return map[string]any{
		"operators":          v.Operators,
		"capacity":           v.Capacity,
		"handling_median":    v.HandlingMedian().String(),
		"resolve_median":     v.ResolveMedian().String(),
		"missed_offer_share": v.MissedOfferShare,
	}
}
