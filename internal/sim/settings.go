package sim

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

// Stand settings of decision 0014, the simulator side (points 2A and 3A).
// The router keeps the sim part. At start the simulator reports the values
// of its environment; from then on it reads the settings every
// opapi.SettingsPollInterval and applies a new sim part on the run.

// standMode is how the simulator takes its sim values.
type standMode uint8

const (
	// standOff: the router offers no stand settings, or refused the
	// report. The simulator runs from its environment. It still reads the
	// settings every poll interval, only to notice that they came on: then
	// it reports and runs with the stored values.
	standOff standMode = iota
	// standLive: the simulator runs with the stored sim part and applies
	// every new one.
	standLive
	// standInvalid: the environment is outside the bounds of opapi. The
	// simulator runs from its environment, as before decision 0014, and
	// does not read the settings again.
	standInvalid
)

// stand is the state of the stand settings. Only the start and then the
// poll goroutine touch it.
type stand struct {
	mode standMode
	// env is the sim part of the environment, with the router default for
	// a capacity of 0. It is what the simulator reports.
	env opapi.SimSettings
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

// startStand reports the sim part of the environment, after the router
// settings were read, and returns the mode the simulator starts in:
//
//   - The environment is outside the bounds: standInvalid, nothing is
//     reported.
//   - The settings have no stand block, or the report gets
//     CodeSettingsOff: standOff.
//   - The report is done or already done: standLive with the stored
//     current sim part, which wins over the environment.
//   - No answer or CodeUnavailable: retried like the settings at start,
//     up to StartWait, then the start fails.
//   - Any other error answer: standOff, and the poll reports again.
func (s *Service) startStand(ctx context.Context, settings opapi.Settings) (*stand, error) {
	st := &stand{env: simOf(s.cfg.Behaviour, settings.CapacityDefault)}
	if fes := st.env.FieldErrors(); len(fes) > 0 {
		st.mode = standInvalid
		s.logInvalid(fes, nil)
		return st, nil
	}
	if settings.Stand == nil {
		s.logger.Info("the router offers no stand settings; running from the environment and checking each poll whether they come on",
			"poll", s.pollEvery.String())
		return st, nil
	}
	deadline := s.clock.Now().Add(s.cfg.StartWait)
	for attempt := 1; ; attempt++ {
		resp, err := s.report(ctx, st.env)
		if err == nil {
			stored := *resp.Settings.Stand.Current.Sim
			st.mode, st.version, st.sim = standLive, resp.Settings.Stand.Version, stored
			s.logger.Info("sim values reported; running with the stored sim values",
				"result", resp.Result, "version", st.version, "stored", simAttrs(stored),
				"differs_from_environment", stored != st.env)
			return st, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		switch code := opclient.Code(err); code {
		case opapi.CodeSettingsOff:
			s.logger.Info("the router does not offer stand settings; running from the environment", "error", err)
			return st, nil
		case opapi.CodeInvalidRequest:
			st.mode = standInvalid
			s.logInvalid(nil, err)
			return st, nil
		case "", opapi.CodeUnavailable:
			// Retried below.
		default:
			s.logger.Warn("the router refused the report of the sim values; running from the environment, the next poll reports again",
				"code", code, "error", err)
			return st, nil
		}
		if !s.clock.Now().Add(startPause).Before(deadline) {
			return nil, fmt.Errorf("sim values not reported within %s, after %d attempts: %w", s.cfg.StartWait, attempt, err)
		}
		if attempt == 1 {
			s.logger.Warn("waiting for the router to take the report of the sim values", "wait", s.cfg.StartWait.String(), "error", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.clock.After(startPause):
		}
	}
}

// report sends the sim part of the environment once. A successful answer
// always has a stored sim part.
func (s *Service) report(ctx context.Context, env opapi.SimSettings) (opapi.SimDefaultsResponse, error) {
	actx, cancel := context.WithTimeout(ctx, startAttempt)
	defer cancel()
	resp, err := s.api.PutSimDefaults(actx, opapi.SimDefaultsRequest{Sim: env})
	if err == nil && (resp.Settings.Stand == nil || resp.Settings.Stand.Current.Sim == nil) {
		// opclient checks this; another API may not.
		err = &opapi.SettingsError{Status: opapi.CodeInternal.Status(), SettingsErrorResponse: opapi.SettingsErrorResponse{
			ErrorResponse: opapi.ErrorResponse{Code: opapi.CodeInternal, Message: "answer has no stored sim part"},
		}}
	}
	return resp, err
}

// logInvalid logs that the environment is outside the bounds, with the
// fields: from the local check (fes) or from the answer of the router
// (err).
func (s *Service) logInvalid(fes []opapi.FieldError, err error) {
	if se, ok := errors.AsType[*opapi.SettingsError](err); ok {
		fes = se.Fields
	}
	fields := make([]string, len(fes))
	for i, fe := range fes {
		fields[i] = fe.Error()
	}
	args := []any{"fields", fields}
	if err != nil {
		args = append(args, "error", err)
	}
	s.logger.Warn("the sim values of the environment are outside the bounds of the stand settings; "+
		"running from the environment without live changes", args...)
}

// startBehaviour returns the behaviour the engine starts with: the
// environment, with the stored sim part in standLive. When the language
// spread cannot be scaled to the stored count, the count of the
// environment stays, with a warning.
func (s *Service) startBehaviour(st *stand) Behaviour {
	b := s.cfg.Behaviour
	if st.mode != standLive {
		return b
	}
	b.Capacity = st.sim.Capacity
	b.HandlingMedian = st.sim.HandlingMedian()
	b.ResolveMedian = st.sim.ResolveMedian()
	b.MissedOfferShare = st.sim.MissedOfferShare
	if _, err := b.Languages.Scale(st.sim.Operators); err != nil {
		s.logger.Warn("the stored operator count does not fit the language spread; the count of the environment stays",
			"stored", st.sim.Operators, "operators", b.Operators, "languages", b.Languages.String(), "error", err)
		return b
	}
	b.Operators = st.sim.Operators
	return b
}

// pollStand reads the settings every poll interval until ctx ends, and
// applies a new sim part. It returns an error that wraps ErrOutage when the
// poll has had no answer of the router for the outage limit. It ends when
// the mode becomes standInvalid.
func (s *Service) pollStand(ctx context.Context, eng *engine, st *stand) error {
	for st.mode != standInvalid {
		select {
		case <-ctx.Done():
			return nil
		case <-s.clock.After(s.pollEvery):
		}
		if err := s.pollOnce(ctx, eng, st); err != nil {
			return err
		}
	}
	return nil
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
	switch st.mode {
	case standOff:
		if set.Stand == nil {
			return nil
		}
		return s.reportOnPoll(ctx, eng, st)
	case standLive:
		switch {
		case set.Stand == nil:
			s.note(st, "the router has no stand settings now; the applied sim values stay")
		case set.Stand.Current.Sim == nil:
			// The store has no sim part, for example a new database: the
			// simulator reports as at a start.
			return s.reportOnPoll(ctx, eng, st)
		default:
			s.note(st, "")
			if set.Stand.Version != st.version {
				s.applyStand(eng, st, set.Stand.Version, *set.Stand.Current.Sim)
			}
		}
	}
	return nil
}

// reportOnPoll reports the sim part of the environment from the poll and
// applies the stored sim part of the answer.
func (s *Service) reportOnPoll(ctx context.Context, eng *engine, st *stand) error {
	resp, err := s.report(ctx, st.env)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		switch code := opclient.Code(err); code {
		case opapi.CodeInvalidRequest:
			st.mode = standInvalid
			s.logInvalid(nil, err)
			return nil
		case "", opapi.CodeUnavailable:
			return s.pollFailed(st, "sim values report", err)
		default:
			s.note(st, "the router refused the report of the sim values; the next poll reports again", "code", code, "error", err)
			return nil
		}
	}
	s.note(st, "")
	if st.mode == standOff {
		s.logger.Info("stand settings came on at the router; running with the stored sim values")
	}
	stand := resp.Settings.Stand
	s.applyStand(eng, st, stand.Version, *stand.Current.Sim)
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
