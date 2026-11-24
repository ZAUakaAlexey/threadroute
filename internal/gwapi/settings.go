package gwapi

import (
	"fmt"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// Stand settings and trends (decision 0014). The router keeps the stand
// settings (opapi.StandState); the gateway passes them on to every
// connection, with the bounds and the right of the connection, and keeps
// one hour of trend points in memory.
//
// Everything here is optional on the wire: a gateway without stand
// settings (an older one, or one whose router does not offer them) sends no
// Settings block, and a console then shows no settings and never sends
// ChangeSettings.

// Settings is the stand settings block of Welcome and SettingsUpdate. It is
// the same for every view and every level, except for the "by" labels and
// MayChange, which follow the right of the connection.
type Settings struct {
	// Version is the version of Current, opapi.StandState.Version. 0 means
	// that nothing is stored yet. ChangeSettings.BaseVersion takes it.
	Version int64 `json:"version"`
	// Current is the set in use. Current.Sim is absent until a simulator
	// has reported.
	Current opapi.StandSettings `json:"current"`
	// Defaults are the values of the environments, for "reset to
	// defaults". Defaults.Sim is absent exactly when Current.Sim is.
	Defaults opapi.StandSettings `json:"defaults"`
	// Bounds are the rules of every value, opapi.Bounds. The console takes
	// them from here and never hard-codes a number.
	Bounds opapi.StandBounds `json:"bounds"`
	// Changes are the newest changes, at most opapi.MaxSettingsChanges,
	// newest first. Never null: [] when there is none. The "by" of each
	// entry is set when MayChange is true, and absent ("") when it is
	// false.
	Changes []opapi.SettingsChange `json:"changes"`
	// MayChange reports that this connection may send ChangeSettings.
	MayChange bool `json:"may_change"`
}

func (s *Settings) validate(errs *fieldErrors, a Audience) {
	if s.MayChange != a.MayChangeSettings {
		errs.invalid("may_change", "is %v, the right of the audience is %v", s.MayChange, a.MayChangeSettings)
	}
	st := opapi.StandState{Version: s.Version, Current: s.Current, Defaults: s.Defaults}
	if err := st.Validate(); err != nil {
		*errs = append(*errs, err)
	}
	if err := s.Bounds.Validate(); err != nil {
		errs.invalid("bounds", "%v", err)
	}
	if s.Changes == nil {
		errs.missing("changes")
	}
	// The "by" label follows the right of the audience, not the flag of
	// the message, so a wrong flag cannot let it through.
	if err := opapi.ValidateChanges(s.Changes, s.Version, a.MayChangeSettings); err != nil {
		*errs = append(*errs, err)
	}
}

// Trend is the trend history block of Welcome: the points of the last
// TrendHistory, one every TrendInterval (decision 0014, point 5). The
// gateway keeps them in memory, so a gateway restart starts an empty
// history. The gateway sends it to ViewSupervisor connections; it is valid
// for every audience.
type Trend struct {
	// IntervalMS is TrendInterval in milliseconds of machine time.
	IntervalMS int `json:"interval_ms"`
	// Points are ordered by SampledAtUS, oldest first, at most
	// MaxTrendPoints. Never null: [] right after a gateway start.
	Points []TrendPoint `json:"points"`
}

func (t *Trend) validate(errs *fieldErrors) {
	if t.IntervalMS <= 0 {
		errs.missing("interval_ms")
	}
	switch {
	case t.Points == nil:
		errs.missing("points")
	case len(t.Points) > MaxTrendPoints:
		errs.invalid("points", "%d are more than %d", len(t.Points), MaxTrendPoints)
	}
	for i, p := range t.Points {
		errs.within(fmt.Sprintf("points[%d]", i), p.validate)
		if i > 0 && p.SampledAtUS <= t.Points[i-1].SampledAtUS {
			errs.invalid("points", "are not ordered by sampled_at_us at [%d]", i)
		}
	}
}

// TrendPoint is one sample of the trend strip. It holds counts and one
// duration only, never a key, an ID or a pseudonym, so it is the same at
// every level.
type TrendPoint struct {
	// SampledAtUS is the machine clock of the gateway when it took the
	// sample.
	SampledAtUS int64 `json:"sampled_at_us"`
	// Waiting counts the conversations in a queue without an offer, as
	// Totals.Conversations.Waiting.
	Waiting uint64 `json:"waiting"`
	// LongestWaitUS is how long the conversation that has waited longest
	// among the waiting and the offered ones has waited, in model time. 0
	// when none waits.
	LongestWaitUS int64 `json:"longest_wait_us"`
	// ResolvedLastMinute counts the resolves of the last 60 seconds of
	// machine time.
	ResolvedLastMinute uint64 `json:"resolved_last_minute"`
	// Teams counts the dedicated teams.
	Teams uint64 `json:"teams"`
	// OperatorsInTeams counts the members of all teams.
	OperatorsInTeams uint64 `json:"operators_in_teams"`
	// OperatorsOnline counts the online operators, as
	// Totals.OperatorsOnline.
	OperatorsOnline uint64 `json:"operators_online"`
}

func (p TrendPoint) validate(errs *fieldErrors) {
	if p.SampledAtUS <= 0 {
		errs.missing("sampled_at_us")
	}
	if p.LongestWaitUS < 0 {
		errs.invalid("longest_wait_us", "is negative")
	}
}

// SettingsUpdate tells a connection about a new Settings block: a new
// current set (a new version), or new defaults. The gateway sends it to
// every connection, each with its own MayChange and "by" rule. After a
// ChangeSettings that was applied, the connection that asked gets this
// message before its Result.
type SettingsUpdate struct {
	Settings Settings `json:"settings"`
}

// ServerType implements ServerMessage.
func (SettingsUpdate) ServerType() ServerType { return TypeSettings }

// Validate implements ServerMessage.
func (u SettingsUpdate) Validate(a Audience) error {
	var errs fieldErrors
	errs.within("settings", func(e *fieldErrors) { u.Settings.validate(e, a) })
	return errs.join()
}

// TrendUpdate carries one new trend point, every TrendInterval. The
// gateway sends it to ViewSupervisor connections, at every level. A point
// holds counts only, so it is valid for every audience.
type TrendUpdate struct {
	Point TrendPoint `json:"point"`
}

// ServerType implements ServerMessage.
func (TrendUpdate) ServerType() ServerType { return TypeTrend }

// Validate implements ServerMessage.
func (u TrendUpdate) Validate(Audience) error {
	var errs fieldErrors
	errs.within("point", u.Point.validate)
	return errs.join()
}

// ChangeSettings asks for a change of the stand settings (decision 0014).
// The server answers with exactly one Result with the same RequestID:
//
//   - OutcomeDone: applied. A SettingsUpdate with the new version came
//     before the Result.
//   - OutcomeAlreadyDone: the set is the current set at the current
//     version; nothing changed.
//   - OutcomeRejected with CodeInvalidRequest and Result.Fields: values
//     break the bounds or the rules between fields, or the sim part is
//     missing or must be absent.
//   - OutcomeRejected with CodeSettingsConflict: BaseVersion is not the
//     current version. The connection got (or gets before the Result) a
//     SettingsUpdate with the current set.
//   - OutcomeRejected with CodeSettingsTooSoon and Result.RetryAfterMS:
//     the last change was less than opapi.SettingsChangeInterval ago.
//   - OutcomeRejected with CodeNotAllowed: the connection may not change
//     settings (Settings.MayChange is false).
//   - OutcomeRejected with CodeSettingsOff: this stand has no stand
//     settings (the router does not offer them).
//   - OutcomeRejected with CodeUnavailable: the router did not answer.
//     Retry later.
//   - OutcomeRejected with CodeRateLimited or CodeInternal, as for every
//     request.
//
// The gateway checks the right, the bounds and the version itself before it
// calls the router, so a refused change costs no router call.
type ChangeSettings struct {
	// RequestID is chosen by the client and follows event.ValidRoutingID.
	RequestID string `json:"request_id"`
	// BaseVersion is the Settings.Version the set was built from.
	BaseVersion int64 `json:"base_version"`
	// Settings is the whole new set. "Reset to defaults" sends
	// Settings.Defaults.
	Settings opapi.StandSettings `json:"settings"`
}

// ClientType implements ClientMessage.
func (ChangeSettings) ClientType() ClientType { return TypeChangeSettings }

// Validate implements ClientMessage. It checks the shape only. Values
// outside the bounds are not a broken message: the server answers them with
// a Result that lists the fields, and the connection stays open.
func (c ChangeSettings) Validate() error {
	var errs fieldErrors
	checkRequestID(&errs, c.RequestID)
	if c.BaseVersion < 0 {
		errs.invalid("base_version", "%d is negative", c.BaseVersion)
	}
	return errs.join()
}
