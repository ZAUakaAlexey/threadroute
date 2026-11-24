package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// # Stand settings (decision 0014)
//
// The team numbers of the Config can change while the router runs. The
// Service keeps the numbers in use in one snapshot (rules) and swaps the
// whole snapshot in one step, so each team check reads one consistent set:
// CheckTeams loads it once and passes it down.
//
// The stored set lives in PostgreSQL (internal/pg, 00005_stand_settings.sql)
// with its version and a change log. The instance that applies a change
// swaps its snapshot after the commit. Every instance also reads the stored
// row at each team check and on each read of the settings, and swaps to a
// newer version, so a second router converges within one team interval.
// A snapshot is only ever replaced by one with a higher version.
//
// No team rule changed for this: the rules of decision 0009 read the new
// numbers from the next check on (decision 0014, point 4A). A team above a
// lower share stops growing; a team above a lower max size shrinks by the
// normal size check; a raised number takes effect at the next check.
//
// The Service offers stand settings only after LoadSettings, and only when
// the team values of its environment are within the bounds of opapi. A
// Service that does not offer them runs with its environment values as
// before decision 0014 and never touches the settings tables.

// rules is one consistent set of the routing numbers in use.
type rules struct {
	// cfg is the configuration with the team part of current.
	cfg Config
	// version is the version of current; 0 while nothing is stored.
	version int64
	// current is the current stand settings. The sim part is absent until
	// a simulator reported. Treat it as read-only.
	current opapi.StandSettings
}

// config returns the configuration in use. The fields that the stand
// settings do not change (speed factor, capacity, timeouts and targets)
// are the same in every snapshot.
func (s *Service) config() Config { return s.rules.Load().cfg }

// Change sources of the stand settings, for the metrics.
const (
	// SourceConsole is a change by PUT /v1/settings.
	SourceConsole = "console"
	// SourceSimulator is the first report of a simulator that became the
	// current sim part.
	SourceSimulator = "simulator"
)

// StandSnapshot is the stand settings in use by this Service, for the
// metrics.
type StandSnapshot struct {
	// Enabled reports whether the Service offers stand settings.
	Enabled bool
	// Version and Current are the set in use. When Enabled is false,
	// Version is 0 and Current holds the environment values.
	Version int64
	Current opapi.StandSettings
	// ConsoleChanges and SimulatorChanges count the changes this Service
	// applied, by source.
	ConsoleChanges, SimulatorChanges int64
}

// StandSnapshot returns the stand settings in use. It is safe for
// concurrent use.
func (s *Service) StandSnapshot() StandSnapshot {
	r := s.rules.Load()
	return StandSnapshot{
		Enabled: s.standOn.Load(), Version: r.version, Current: cloneSettings(r.current),
		ConsoleChanges: s.consoleChanges.Load(), SimulatorChanges: s.simChanges.Load(),
	}
}

// FieldValue is one value of a stand settings set.
type FieldValue struct {
	// Field is a name from opapi.SettingsFields.
	Field string
	Value float64
}

// SettingsValues returns the values of a set in the order of
// opapi.SettingsFields. The sim values are left out when the set has no
// sim part.
func SettingsValues(set opapi.StandSettings) []FieldValue {
	t := set.Teams
	out := []FieldValue{
		{opapi.FieldTeamFormLoad, float64(t.FormLoad)},
		{opapi.FieldTeamReleaseLoad, float64(t.ReleaseLoad)},
		{opapi.FieldTeamReleaseAfter, float64(t.ReleaseAfterUS)},
		{opapi.FieldTeamMinSize, float64(t.MinSize)},
		{opapi.FieldTeamMaxSize, float64(t.MaxSize)},
		{opapi.FieldTeamMaxShare, t.MaxShare},
	}
	if sim := set.Sim; sim != nil {
		out = append(out,
			FieldValue{opapi.FieldSimOperators, float64(sim.Operators)},
			FieldValue{opapi.FieldSimCapacity, float64(sim.Capacity)},
			FieldValue{opapi.FieldSimHandlingMedian, float64(sim.HandlingMedianUS)},
			FieldValue{opapi.FieldSimResolveMedian, float64(sim.ResolveMedianUS)},
			FieldValue{opapi.FieldSimMissedOfferShare, sim.MissedOfferShare},
		)
	}
	return out
}

// teamsOf returns the team part of a configuration.
func teamsOf(c Config) opapi.TeamSettings {
	return opapi.TeamSettings{
		FormLoad:       c.TeamFormLoad,
		ReleaseLoad:    c.TeamReleaseLoad,
		ReleaseAfterUS: c.TeamReleaseAfter.Microseconds(),
		MinSize:        c.TeamMinSize,
		MaxSize:        c.TeamMaxSize,
		MaxShare:       c.TeamMaxShare,
	}
}

// withTeams returns the configuration with the team part replaced.
func (c Config) withTeams(t opapi.TeamSettings) Config {
	c.TeamFormLoad = t.FormLoad
	c.TeamReleaseLoad = t.ReleaseLoad
	c.TeamReleaseAfter = t.ReleaseAfter()
	c.TeamMinSize = t.MinSize
	c.TeamMaxSize = t.MaxSize
	c.TeamMaxShare = t.MaxShare
	return c
}

// cloneSettings returns a copy that shares no memory with set.
func cloneSettings(set opapi.StandSettings) opapi.StandSettings {
	if set.Sim != nil {
		sim := *set.Sim
		set.Sim = &sim
	}
	return set
}

// standRow is the row of stand_settings, decoded. At version 0 nothing is
// stored, and current holds the team values of the environment of this
// router.
type standRow struct {
	version     int64
	current     opapi.StandSettings
	simDefaults *opapi.SimSettings
	lastPutAt   *time.Time
}

// errBadStored marks a stored row that cannot be read or breaks the
// bounds. Only a hand edit of the database can make one.
var errBadStored = errors.New("stored stand settings are not valid")

// decodeStored decodes a row of stand_settings. env is the team part of
// the environment: the defaults of the teams part, and the current set at
// version 0. The result is checked with the rules of opapi.StandState.
func decodeStored(row pgdb.StandSetting, env opapi.TeamSettings) (standRow, error) {
	st := standRow{version: row.Version, current: opapi.StandSettings{Teams: env}, lastPutAt: row.LastPutAt}
	if row.CurrentSet != nil {
		// A fresh value, so a field missing in the JSON stays 0 and fails
		// the bounds instead of taking the environment value.
		var current opapi.StandSettings
		if err := json.Unmarshal(row.CurrentSet, &current); err != nil {
			return standRow{}, fmt.Errorf("%w: current set: %w", errBadStored, err)
		}
		st.current = current
	}
	if row.DefaultsSim != nil {
		st.simDefaults = new(opapi.SimSettings)
		if err := json.Unmarshal(row.DefaultsSim, st.simDefaults); err != nil {
			return standRow{}, fmt.Errorf("%w: sim defaults: %w", errBadStored, err)
		}
	}
	if err := st.state(env, nil).Validate(); err != nil {
		return standRow{}, fmt.Errorf("%w: %w", errBadStored, err)
	}
	return st, nil
}

// encode returns the columns current_set and defaults_sim. A nil slice is
// NULL.
func (st standRow) encode() (current, simDefaults []byte, err error) {
	if st.version > 0 {
		if current, err = json.Marshal(st.current); err != nil {
			return nil, nil, fmt.Errorf("encode current set: %w", err)
		}
	}
	if st.simDefaults != nil {
		if simDefaults, err = json.Marshal(st.simDefaults); err != nil {
			return nil, nil, fmt.Errorf("encode sim defaults: %w", err)
		}
	}
	return current, simDefaults, nil
}

// state returns the stand settings block of the answers.
func (st standRow) state(env opapi.TeamSettings, changes []opapi.SettingsChange) opapi.StandState {
	return opapi.StandState{
		Version:  st.version,
		Current:  cloneSettings(st.current),
		Defaults: cloneSettings(opapi.StandSettings{Teams: env, Sim: st.simDefaults}),
		Changes:  changes,
	}
}

// changeStep is the outcome of the checks of a change that need the stored
// set: steps 4 to 8 of opapi.PatternPutSettings.
type changeStep int

const (
	stepApply changeStep = iota
	stepConflict
	stepSimPart
	stepSame
	stepTooSoon
)

// checkChange runs steps 4 to 7 of opapi.PatternPutSettings on the stored
// set at the time now, in the order of the contract. The request already
// passed its own FieldErrors (step 3). It returns the first step that
// fails, or stepApply. With stepSimPart it returns the field error, and
// with stepTooSoon the wait in whole milliseconds.
func checkChange(st standRow, req opapi.SettingsChangeRequest, now time.Time) (changeStep, opapi.FieldError, int64) {
	if req.BaseVersion != st.version {
		return stepConflict, opapi.FieldError{}, 0
	}
	switch has, want := req.Settings.Sim != nil, st.current.Sim != nil; {
	case has && !want:
		return stepSimPart, opapi.FieldError{
			Field: opapi.FieldSim, Reason: "must be absent: no simulator has reported its values yet",
		}, 0
	case !has && want:
		return stepSimPart, opapi.FieldError{Field: opapi.FieldSim, Reason: "is not set"}, 0
	}
	if req.Settings.Equal(st.current) {
		return stepSame, opapi.FieldError{}, 0
	}
	if st.lastPutAt != nil {
		if wait, ok := retryAfterMS(now.Sub(*st.lastPutAt)); !ok {
			return stepTooSoon, opapi.FieldError{}, wait
		}
	}
	return stepApply, opapi.FieldError{}, 0
}

// retryAfterMS checks the time since the last change by PUT against
// opapi.SettingsChangeInterval. ok is true when the interval has passed.
// Otherwise wait is the rest of the interval in whole milliseconds,
// rounded up, between 1 and the interval. A negative elapsed time (the
// clocks of two routers differ a little) waits the whole interval.
func retryAfterMS(elapsed time.Duration) (wait int64, ok bool) {
	if elapsed >= opapi.SettingsChangeInterval {
		return 0, true
	}
	rest := opapi.SettingsChangeInterval - min(max(elapsed, 0), opapi.SettingsChangeInterval)
	ms := int64(math.Ceil(float64(rest) / float64(time.Millisecond)))
	return min(max(ms, 1), opapi.SettingsChangeInterval.Milliseconds()), false
}

// planSimDefaults applies a simulator report to the stored set: the
// values always become the sim defaults, and also the current sim part
// when the current set has none. That second case is a change: the version
// goes up by one and fields lists it for the log. changed reports whether
// anything differs from st.
func planSimDefaults(st standRow, sim opapi.SimSettings) (next standRow, fields []opapi.FieldChange, changed bool) {
	next = st
	next.current = cloneSettings(st.current)
	defaults := sim
	next.simDefaults = &defaults
	if st.current.Sim == nil {
		current := sim
		next.current.Sim = &current
		next.version++
		fields = opapi.Diff(st.current, next.current)
	}
	changed = fields != nil || st.simDefaults == nil || *st.simDefaults != sim
	return next, fields, changed
}

// settingsErr returns the error answer of a stand settings call.
func settingsErr(code opapi.ErrorCode, format string, args ...any) *opapi.SettingsError {
	return &opapi.SettingsError{
		Status: code.Status(),
		SettingsErrorResponse: opapi.SettingsErrorResponse{
			ErrorResponse: opapi.ErrorResponse{Code: code, Message: fmt.Sprintf(format, args...)},
		},
	}
}

func settingsOff() *opapi.SettingsError {
	return settingsErr(opapi.CodeSettingsOff, "this router does not offer stand settings")
}

func invalidFields(fes []opapi.FieldError) *opapi.SettingsError {
	e := settingsErr(opapi.CodeInvalidRequest, "the settings are not valid")
	e.Fields = fes
	return e
}

// LoadSettings reads the stored stand settings at start and applies them.
// Call it once, before the Service serves the operator API. When the team
// values of the environment are outside the bounds of opapi, or the stored
// row is not valid, the Service does not offer stand settings and keeps
// its environment values; both are logged, neither is an error. Only a
// database error is.
func (s *Service) LoadSettings(ctx context.Context) error {
	if fes := s.envTeams.FieldErrors(); len(fes) > 0 {
		s.logger.WarnContext(ctx, "stand settings are off: team values of the environment are outside the bounds",
			"error", opapi.JoinFieldErrors(fes))
		return nil
	}
	st, err := s.readStored(ctx, pgdb.New(s.db))
	if errors.Is(err, errBadStored) {
		s.logger.ErrorContext(ctx, "stand settings are off: the stored row is not valid", "error", err)
		return nil
	}
	if err != nil {
		return fmt.Errorf("load stand settings: %w", err)
	}
	s.standOn.Store(true)
	s.apply(ctx, st)
	s.logger.InfoContext(ctx, "stand settings loaded", "version", st.version)
	return nil
}

// SyncSettings reads the stored row and applies a newer version. CheckTeams
// calls it, so every router converges within one team interval. A stored
// row that is not valid is logged and skipped.
func (s *Service) SyncSettings(ctx context.Context) error {
	if !s.standOn.Load() {
		return nil
	}
	st, err := s.readStored(ctx, pgdb.New(s.db))
	if errors.Is(err, errBadStored) {
		s.logger.WarnContext(ctx, "stand settings: the stored row is not valid, keeping the current set", "error", err)
		return nil
	}
	if err != nil {
		return fmt.Errorf("sync stand settings: %w", err)
	}
	s.apply(ctx, st)
	return nil
}

// readStored reads the stored row without a lock. No row is version 0.
func (s *Service) readStored(ctx context.Context, q *pgdb.Queries) (standRow, error) {
	row, err := q.GetStandSettings(ctx)
	if isNoRows(err) {
		return standRow{current: opapi.StandSettings{Teams: s.envTeams}}, nil
	}
	if err != nil {
		return standRow{}, fmt.Errorf("read stand settings: %w", err)
	}
	return decodeStored(row, s.envTeams)
}

// lockStored inserts the row when it is missing, locks it and decodes it.
func (s *Service) lockStored(ctx context.Context, q *pgdb.Queries) (standRow, error) {
	if err := q.EnsureStandSettings(ctx, s.now()); err != nil {
		return standRow{}, fmt.Errorf("ensure stand settings: %w", err)
	}
	row, err := q.LockStandSettings(ctx)
	if err != nil {
		return standRow{}, fmt.Errorf("lock stand settings: %w", err)
	}
	return decodeStored(row, s.envTeams)
}

// saveStored writes the locked row.
func saveStored(ctx context.Context, q *pgdb.Queries, st standRow, now time.Time) error {
	current, simDefaults, err := st.encode()
	if err != nil {
		return err
	}
	err = q.SaveStandSettings(ctx, pgdb.SaveStandSettingsParams{
		Version: st.version, CurrentSet: current, DefaultsSim: simDefaults, LastPutAt: st.lastPutAt, UpdatedAt: now,
	})
	if err != nil {
		return fmt.Errorf("save stand settings: %w", err)
	}
	return nil
}

// logChange adds one entry to the change log.
func logChange(ctx context.Context, q *pgdb.Queries, version int64, now time.Time, by string, fields []opapi.FieldChange) error {
	b, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("encode changed fields: %w", err)
	}
	err = q.InsertStandSettingsChange(ctx, pgdb.InsertStandSettingsChangeParams{
		Version: version, ChangedAt: now, ChangedBy: by, Fields: b,
	})
	if err != nil {
		return fmt.Errorf("log stand settings change: %w", err)
	}
	return nil
}

// recentChanges reads the newest entries of the change log up to version.
func recentChanges(ctx context.Context, q *pgdb.Queries, version int64) ([]opapi.SettingsChange, error) {
	if version == 0 {
		return nil, nil
	}
	rows, err := q.RecentStandSettingsChanges(ctx, pgdb.RecentStandSettingsChangesParams{
		MaxVersion: version, MaxRows: opapi.MaxSettingsChanges,
	})
	if err != nil {
		return nil, fmt.Errorf("read stand settings changes: %w", err)
	}
	out := make([]opapi.SettingsChange, len(rows))
	for i, r := range rows {
		out[i] = opapi.SettingsChange{ChangedAtUS: r.ChangedAt.UnixMicro(), Version: r.Version, By: r.ChangedBy}
		if err := json.Unmarshal(r.Fields, &out[i].Fields); err != nil {
			return nil, fmt.Errorf("%w: change %d: %w", errBadStored, r.Version, err)
		}
	}
	return out, nil
}

// answer returns the settings of the operator API with the stand settings
// block of st, and the change log when withChanges is set.
func (s *Service) answer(ctx context.Context, q *pgdb.Queries, st standRow, withChanges bool) (opapi.Settings, error) {
	var changes []opapi.SettingsChange
	if withChanges {
		var err error
		if changes, err = recentChanges(ctx, q, st.version); err != nil {
			return opapi.Settings{}, err
		}
	}
	out := s.config().Settings()
	stand := st.state(s.envTeams, changes)
	out.Stand = &stand
	return out, nil
}

// apply swaps the snapshot to st when st has a higher version than the
// snapshot. Concurrent calls end with the highest version.
func (s *Service) apply(ctx context.Context, st standRow) {
	for {
		cur := s.rules.Load()
		if st.version <= cur.version {
			return
		}
		next := &rules{cfg: cur.cfg.withTeams(st.current.Teams), version: st.version, current: cloneSettings(st.current)}
		if s.rules.CompareAndSwap(cur, next) {
			s.logger.InfoContext(ctx, "stand settings applied", "version", st.version)
			return
		}
	}
}

// ReadSettings returns the settings of the operator API
// (opapi.PatternSettings) with the stand settings block read from the
// database, and the change log when withChanges is set. A newer stored
// version is applied on the way. A Service that does not offer stand
// settings answers like Settings and reads nothing.
func (s *Service) ReadSettings(ctx context.Context, withChanges bool) (opapi.Settings, error) {
	if !s.standOn.Load() {
		return s.Settings(), nil
	}
	q := pgdb.New(s.db)
	st, err := s.readStored(ctx, q)
	if err != nil {
		return opapi.Settings{}, err
	}
	s.apply(ctx, st)
	return s.answer(ctx, q, st, withChanges)
}

// PutSettings changes the stand settings (opapi.PatternPutSettings). It
// runs steps 2 to 8 of the contract in their order; the caller decoded the
// body (step 1). An error answer of the contract is an *opapi.SettingsError;
// any other error is a database error.
//
// Steps 4 to 8 run in one transaction on the locked row, so the version
// check and the interval hold for several routers. The time is read from
// the clock after the lock is held.
func (s *Service) PutSettings(ctx context.Context, req opapi.SettingsChangeRequest) (opapi.SettingsChangeResponse, error) {
	if !s.standOn.Load() {
		return opapi.SettingsChangeResponse{}, settingsOff()
	}
	if fes := req.FieldErrors(); len(fes) > 0 {
		return opapi.SettingsChangeResponse{}, invalidFields(fes)
	}
	var (
		resp    opapi.SettingsChangeResponse
		applied *standRow
		fields  []opapi.FieldChange
	)
	err := s.inTx(ctx, func(q *pgdb.Queries) error {
		resp, applied, fields = opapi.SettingsChangeResponse{}, nil, nil
		st, err := s.lockStored(ctx, q)
		if err != nil {
			return err
		}
		now := s.now()
		step, fe, wait := checkChange(st, req, now)
		switch step {
		case stepConflict:
			cur, err := s.answer(ctx, q, st, true)
			if err != nil {
				return err
			}
			e := settingsErr(opapi.CodeSettingsConflict, "the base version %d is not the current version %d", req.BaseVersion, st.version)
			e.Settings = &cur
			return e
		case stepSimPart:
			return invalidFields([]opapi.FieldError{fe})
		case stepSame:
			cur, err := s.answer(ctx, q, st, true)
			resp = opapi.SettingsChangeResponse{Result: opapi.ResultAlreadyDone, Settings: cur}
			return err
		case stepTooSoon:
			e := settingsErr(opapi.CodeSettingsTooSoon, "the last change was less than %s ago", opapi.SettingsChangeInterval)
			e.RetryAfterMS = wait
			return e
		}
		next := st
		next.version++
		next.current = cloneSettings(req.Settings)
		next.lastPutAt = &now
		fields = opapi.Diff(st.current, next.current)
		if err := saveStored(ctx, q, next, now); err != nil {
			return err
		}
		if err := logChange(ctx, q, next.version, now, req.By, fields); err != nil {
			return err
		}
		cur, err := s.answer(ctx, q, next, true)
		resp = opapi.SettingsChangeResponse{Result: opapi.ResultDone, Settings: cur}
		applied = &next
		return err
	})
	if err != nil {
		return opapi.SettingsChangeResponse{}, err
	}
	if applied != nil {
		s.consoleChanges.Add(1)
		s.apply(ctx, *applied)
		s.logger.InfoContext(ctx, "stand settings changed", "source", SourceConsole, "version", applied.version,
			"fields", fieldNames(fields))
	}
	return resp, nil
}

// PutSimDefaults stores the values of a simulator environment
// (opapi.PatternPutSimDefaults). It is not limited by
// opapi.SettingsChangeInterval and does not start it. Errors are as for
// PutSettings.
func (s *Service) PutSimDefaults(ctx context.Context, req opapi.SimDefaultsRequest) (opapi.SimDefaultsResponse, error) {
	if !s.standOn.Load() {
		return opapi.SimDefaultsResponse{}, settingsOff()
	}
	if fes := req.FieldErrors(); len(fes) > 0 {
		return opapi.SimDefaultsResponse{}, invalidFields(fes)
	}
	var (
		resp    opapi.SimDefaultsResponse
		applied *standRow
	)
	err := s.inTx(ctx, func(q *pgdb.Queries) error {
		resp, applied = opapi.SimDefaultsResponse{}, nil
		st, err := s.lockStored(ctx, q)
		if err != nil {
			return err
		}
		next, fields, changed := planSimDefaults(st, req.Sim)
		if !changed {
			cur, err := s.answer(ctx, q, st, false)
			resp = opapi.SimDefaultsResponse{Result: opapi.ResultAlreadyDone, Settings: cur}
			return err
		}
		now := s.now()
		if err := saveStored(ctx, q, next, now); err != nil {
			return err
		}
		if fields != nil {
			if err := logChange(ctx, q, next.version, now, opapi.BySimulator, fields); err != nil {
				return err
			}
			applied = &next
		}
		cur, err := s.answer(ctx, q, next, false)
		resp = opapi.SimDefaultsResponse{Result: opapi.ResultDone, Settings: cur}
		return err
	})
	if err != nil {
		return opapi.SimDefaultsResponse{}, err
	}
	if applied != nil {
		s.simChanges.Add(1)
		s.apply(ctx, *applied)
		s.logger.InfoContext(ctx, "stand settings changed", "source", SourceSimulator, "version", applied.version)
	}
	return resp, nil
}

// fieldNames returns the field names of the changes, for a log line.
func fieldNames(fields []opapi.FieldChange) []string {
	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = f.Field
	}
	return out
}
