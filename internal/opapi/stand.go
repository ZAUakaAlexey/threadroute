package opapi

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

// # Stand settings (decision 0014)
//
// The stand settings are the numbers that a person may change from the
// console while the stand runs. They are state of the stand, not
// configuration: no environment variable sets them. The router stores them
// with a version number and a change log, uses the teams part itself, and
// keeps the sim part for the simulator, which only reads it with
// PatternSettings. Every change goes through PatternPutSettings.
//
// The default of every value is in this file, next to its bound
// (DefaultStandSettings). At a start on a database without a stored set,
// the router stores the defaults as version 1 and logs that change with the
// label ByDefaults. "Reset to defaults" in the console is an ordinary change
// to these values.
//
// # Numbers in JSON
//
//   - Counts and sizes are JSON integers, written without a fraction or an
//     exponent ("20", never "20.0" or "2e1"). A body with another form is
//     not a valid body (CodeInvalidRequest).
//   - Durations are whole microseconds of model time in fields that end in
//     "_us", as everywhere in this package. They are JSON integers too.
//     The largest one (2 hours, 7200000000) is far below 2^53, so a
//     JavaScript number holds every valid value exactly.
//   - A share is a JSON number. It is read into an IEEE 754 double and
//     compared with the bounds as a double, bounds included: "0.05" and
//     the bound 0.05 are the same double on both sides. JSON has no NaN
//     and no infinity, and Validate refuses them in a value built in code.
//   - A field that is absent decodes as 0, which is valid for
//     missed_offer_share and invalid for every other field.

// Bounds of the stand settings, the table of decision 0014. Every bound is
// included. Durations are model time.
const (
	// MinTeamFormLoad and MaxTeamFormLoad bound the load that forms a team.
	MinTeamFormLoad = 2
	MaxTeamFormLoad = 500
	// MinTeamReleaseLoad is the lowest load that releases a team. The
	// highest is the form load minus 1, so the absolute highest is
	// MaxTeamFormLoad - 1.
	MinTeamReleaseLoad = 1
	// MinTeamReleaseAfter and MaxTeamReleaseAfter bound the release time.
	MinTeamReleaseAfter = time.Minute
	MaxTeamReleaseAfter = 2 * time.Hour
	// MinTeamSize and MaxTeamSize bound both team sizes. The min size is
	// never above the max size.
	MinTeamSize = 1
	MaxTeamSize = 50
	// MinTeamMaxShare and MaxTeamMaxShare bound the share of the online
	// operators that teams may hold. 1 turns the cap off (decision 0009).
	MinTeamMaxShare = 0.05
	MaxTeamMaxShare = 1.0

	// MinSimOperators and MaxSimOperators bound the number of simulated
	// operators.
	MinSimOperators = 10
	MaxSimOperators = 500
	// MinSimCapacity and MaxSimCapacity bound the capacity of a simulated
	// operator.
	MinSimCapacity = 1
	MaxSimCapacity = 20
	// MinSimHandlingMedian and MaxSimHandlingMedian bound the median time
	// from the accept to the reply.
	MinSimHandlingMedian = 5 * time.Second
	MaxSimHandlingMedian = 10 * time.Minute
	// MinSimResolveMedian and MaxSimResolveMedian bound the median time from
	// the reply to the resolve.
	MinSimResolveMedian = 10 * time.Second
	MaxSimResolveMedian = 30 * time.Minute
	// MinSimMissedOfferShare and MaxSimMissedOfferShare bound the share of
	// offers that a simulated operator leaves to time out.
	MinSimMissedOfferShare = 0.0
	MaxSimMissedOfferShare = 0.5
)

// Defaults of the stand settings, the table of decision 0014. This is the
// only place that gives them: the router, the simulator and the answers of
// the operator API take them from here. Durations are model time. Every
// default is within the bounds above, which a test checks.
const (
	// DefaultTeamFormLoad is the default load that forms a team.
	DefaultTeamFormLoad = 20
	// DefaultTeamReleaseLoad is the default load that releases a team.
	DefaultTeamReleaseLoad = 8
	// DefaultTeamReleaseAfter is the default release time.
	DefaultTeamReleaseAfter = 15 * time.Minute
	// DefaultTeamMinSize and DefaultTeamMaxSize are the default team size
	// bounds.
	DefaultTeamMinSize = 2
	DefaultTeamMaxSize = 20
	// DefaultTeamMaxShare is the default share of the online operators that
	// all teams together may hold (decision 0009, "A cap on the operators
	// that teams hold").
	DefaultTeamMaxShare = 0.5

	// DefaultSimOperators is the default number of simulated operators. It
	// was sized after the first live run of stage 2, from the number of open
	// conversations.
	DefaultSimOperators = 200
	// DefaultSimCapacity is the default capacity of a simulated operator.
	// It is the router default capacity of decision 0009, which the stand
	// ran with before decision 0014.
	DefaultSimCapacity = 4
	// DefaultSimHandlingMedian is the default median time from the accept
	// to the reply.
	DefaultSimHandlingMedian = 45 * time.Second
	// DefaultSimResolveMedian is the default median time from the reply to
	// the resolve.
	DefaultSimResolveMedian = 3 * time.Minute
	// DefaultSimMissedOfferShare is the default share of offers that a
	// simulated operator does not answer at all.
	DefaultSimMissedOfferShare = 0.03
)

// DefaultTeamSettings returns the defaults of the teams part.
func DefaultTeamSettings() TeamSettings {
	return TeamSettings{
		FormLoad:       DefaultTeamFormLoad,
		ReleaseLoad:    DefaultTeamReleaseLoad,
		ReleaseAfterUS: DefaultTeamReleaseAfter.Microseconds(),
		MinSize:        DefaultTeamMinSize,
		MaxSize:        DefaultTeamMaxSize,
		MaxShare:       DefaultTeamMaxShare,
	}
}

// DefaultSimSettings returns the defaults of the sim part.
func DefaultSimSettings() SimSettings {
	return SimSettings{
		Operators:        DefaultSimOperators,
		Capacity:         DefaultSimCapacity,
		HandlingMedianUS: DefaultSimHandlingMedian.Microseconds(),
		ResolveMedianUS:  DefaultSimResolveMedian.Microseconds(),
		MissedOfferShare: DefaultSimMissedOfferShare,
	}
}

// DefaultStandSettings returns the defaults of the whole set, with both
// parts. Each call returns a new sim part.
func DefaultStandSettings() StandSettings {
	sim := DefaultSimSettings()
	return StandSettings{Teams: DefaultTeamSettings(), Sim: &sim}
}

// Limits of the stand settings calls.
const (
	// SettingsChangeInterval is the shortest time between two changes by
	// PatternPutSettings, for the whole stand (decision 0014, point 6). It
	// is machine time: it is about people, not about routing. It counts
	// from the last change applied by PatternPutSettings. The change that
	// stores the defaults (ByDefaults) is not limited and does not start
	// the time.
	SettingsChangeInterval = 10 * time.Second
	// MaxSettingsChanges is the most entries of StandState.Changes.
	MaxSettingsChanges = 20
	// MaxByLen is the longest "by" label, in bytes.
	MaxByLen = 64
	// SettingsPollInterval is how often the simulator and the gateway read
	// PatternSettings to learn about a new set. It is machine time.
	SettingsPollInterval = 5 * time.Second
)

// Reserved "by" labels of the change log. PatternPutSettings refuses both
// (see ReservedBy).
const (
	// ByDefaults is the label of the change that the router logs when it
	// stores the defaults: at a start on a database without a stored set,
	// or when the stored set has no sim part.
	ByDefaults = "defaults"
	// BySimulator was the label of the change that the router logged when
	// the first report of a simulator became the current sim part. The
	// simulator no longer reports, so no new change carries it, but old
	// rows of the change log may.
	BySimulator = "simulator"
)

// ReservedBy reports whether a "by" label is reserved for the router, so
// that a change request may not carry it.
func ReservedBy(by string) bool { return by == ByDefaults || by == BySimulator }

// Field names of the stand settings: the path of each value inside a
// StandSettings, as JSON names joined with ".". FieldError.Field and
// FieldChange.Field use them, and gwapi uses the same names.
const (
	FieldTeamFormLoad        = "teams.form_load"
	FieldTeamReleaseLoad     = "teams.release_load"
	FieldTeamReleaseAfter    = "teams.release_after_us"
	FieldTeamMinSize         = "teams.min_size"
	FieldTeamMaxSize         = "teams.max_size"
	FieldTeamMaxShare        = "teams.max_share"
	FieldSimOperators        = "sim.operators"
	FieldSimCapacity         = "sim.capacity"
	FieldSimHandlingMedian   = "sim.handling_median_us"
	FieldSimResolveMedian    = "sim.resolve_median_us"
	FieldSimMissedOfferShare = "sim.missed_offer_share"
)

// Field names of a FieldError that are not a single value.
const (
	// FieldSim names the sim part as a whole: it is missing or must be
	// absent.
	FieldSim = "sim"
	// FieldBaseVersion and FieldBy name the fields of
	// SettingsChangeRequest.
	FieldBaseVersion = "base_version"
	FieldBy          = "by"
)

// SettingsFields lists every value of a StandSettings in a fixed order: the
// teams part, then the sim part, each in the order of the struct. Diff
// returns changes in this order.
var SettingsFields = []string{
	FieldTeamFormLoad, FieldTeamReleaseLoad, FieldTeamReleaseAfter, FieldTeamMinSize, FieldTeamMaxSize,
	FieldTeamMaxShare,
	FieldSimOperators, FieldSimCapacity, FieldSimHandlingMedian, FieldSimResolveMedian, FieldSimMissedOfferShare,
}

// StandSettings is one whole set of stand settings: the teams part, which
// the router uses, and the sim part, which the router stores and the
// simulator uses. A change always carries the whole set.
type StandSettings struct {
	Teams TeamSettings `json:"teams"`
	// Sim is the sim part. A router stores it with the defaults at its
	// start, so it is absent only at version 0 (nothing stored) and in the
	// answers of a router from before that rule.
	Sim *SimSettings `json:"sim,omitempty"`
}

// TeamSettings are the team rules of decision 0009 that the console may
// change. No field is omitempty: 0 is sent as 0.
type TeamSettings struct {
	// FormLoad is the active load of a company that forms a team.
	FormLoad int `json:"form_load"`
	// ReleaseLoad releases a team whose active load stayed below it for
	// ReleaseAfterUS. It is below FormLoad.
	ReleaseLoad int `json:"release_load"`
	// ReleaseAfterUS is the release time in model time, in whole
	// microseconds.
	ReleaseAfterUS int64 `json:"release_after_us"`
	// MinSize and MaxSize bound the size of a team. MinSize is not above
	// MaxSize.
	MinSize int `json:"min_size"`
	MaxSize int `json:"max_size"`
	// MaxShare is the share of the online operators that all teams
	// together may hold. 1 turns the cap off.
	MaxShare float64 `json:"max_share"`
}

// SimSettings are the simulator numbers of decision 0010 that the console
// may change. No field is omitempty: 0 is sent as 0.
type SimSettings struct {
	// Operators is the number of simulated operators.
	Operators int `json:"operators"`
	// Capacity is the capacity of each simulated operator. It has no "0
	// takes the router default".
	Capacity int `json:"capacity"`
	// HandlingMedianUS is the median time from the accept to the reply, in
	// model time, in whole microseconds.
	HandlingMedianUS int64 `json:"handling_median_us"`
	// ResolveMedianUS is the median time from the reply to the resolve, in
	// model time, in whole microseconds.
	ResolveMedianUS int64 `json:"resolve_median_us"`
	// MissedOfferShare is the share of offers left to time out.
	MissedOfferShare float64 `json:"missed_offer_share"`
}

// ReleaseAfter returns ReleaseAfterUS as a duration.
func (t TeamSettings) ReleaseAfter() time.Duration {
	return time.Duration(t.ReleaseAfterUS) * time.Microsecond
}

// HandlingMedian returns HandlingMedianUS as a duration.
func (s SimSettings) HandlingMedian() time.Duration {
	return time.Duration(s.HandlingMedianUS) * time.Microsecond
}

// ResolveMedian returns ResolveMedianUS as a duration.
func (s SimSettings) ResolveMedian() time.Duration {
	return time.Duration(s.ResolveMedianUS) * time.Microsecond
}

// Bound is the rule of one value of the stand settings, as it travels to
// the console (gwapi) so that the console never hard-codes a number. Min and
// Max are included. Durations are whole microseconds of model time, like the
// value.
type Bound struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	// Integer reports that the value is a whole number.
	Integer bool `json:"integer"`
	// Below names another field of the same set (a name from
	// SettingsFields) that the value must be below.
	Below string `json:"below,omitempty"`
	// NotAbove names another field of the same set that the value must not
	// be above.
	NotAbove string `json:"not_above,omitempty"`
}

// TeamBounds are the bounds of TeamSettings, field by field.
type TeamBounds struct {
	FormLoad       Bound `json:"form_load"`
	ReleaseLoad    Bound `json:"release_load"`
	ReleaseAfterUS Bound `json:"release_after_us"`
	MinSize        Bound `json:"min_size"`
	MaxSize        Bound `json:"max_size"`
	MaxShare       Bound `json:"max_share"`
}

// SimBounds are the bounds of SimSettings, field by field.
type SimBounds struct {
	Operators        Bound `json:"operators"`
	Capacity         Bound `json:"capacity"`
	HandlingMedianUS Bound `json:"handling_median_us"`
	ResolveMedianUS  Bound `json:"resolve_median_us"`
	MissedOfferShare Bound `json:"missed_offer_share"`
}

// StandBounds are the bounds of StandSettings. Validate checks exactly
// these rules.
type StandBounds struct {
	Teams TeamBounds `json:"teams"`
	Sim   SimBounds  `json:"sim"`
}

// Bounds returns the bounds of the stand settings, built from the
// constants above.
func Bounds() StandBounds {
	whole := func(lo, hi int64) Bound { return Bound{Min: float64(lo), Max: float64(hi), Integer: true} }
	us := func(lo, hi time.Duration) Bound { return whole(lo.Microseconds(), hi.Microseconds()) }
	share := func(lo, hi float64) Bound { return Bound{Min: lo, Max: hi} }

	release := whole(MinTeamReleaseLoad, MaxTeamFormLoad-1)
	release.Below = FieldTeamFormLoad
	minSize := whole(MinTeamSize, MaxTeamSize)
	minSize.NotAbove = FieldTeamMaxSize
	return StandBounds{
		Teams: TeamBounds{
			FormLoad:       whole(MinTeamFormLoad, MaxTeamFormLoad),
			ReleaseLoad:    release,
			ReleaseAfterUS: us(MinTeamReleaseAfter, MaxTeamReleaseAfter),
			MinSize:        minSize,
			MaxSize:        whole(MinTeamSize, MaxTeamSize),
			MaxShare:       share(MinTeamMaxShare, MaxTeamMaxShare),
		},
		Sim: SimBounds{
			Operators:        whole(MinSimOperators, MaxSimOperators),
			Capacity:         whole(MinSimCapacity, MaxSimCapacity),
			HandlingMedianUS: us(MinSimHandlingMedian, MaxSimHandlingMedian),
			ResolveMedianUS:  us(MinSimResolveMedian, MaxSimResolveMedian),
			MissedOfferShare: share(MinSimMissedOfferShare, MaxSimMissedOfferShare),
		},
	}
}

// Validate reports a bound whose numbers cannot be met: not finite, a min
// above the max, or a fraction in an integer bound. It is for a Bound read
// from the wire; Bounds always passes.
func (b Bound) Validate() error {
	switch {
	case math.IsNaN(b.Min) || math.IsInf(b.Min, 0) || math.IsNaN(b.Max) || math.IsInf(b.Max, 0):
		return errors.New("min and max must be finite")
	case b.Min > b.Max:
		return fmt.Errorf("min %v is above max %v", b.Min, b.Max)
	case b.Integer && (b.Min != math.Trunc(b.Min) || b.Max != math.Trunc(b.Max)):
		return errors.New("min and max of an integer bound must be whole numbers")
	}
	return nil
}

// Validate reports every bound that cannot be met, by field name.
func (b StandBounds) Validate() error {
	var errs []error
	for _, f := range b.fields() {
		if err := f.b.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", f.name, err))
		}
	}
	return errors.Join(errs...)
}

type namedBound struct {
	name string
	b    Bound
}

func (b StandBounds) fields() []namedBound {
	t, s := b.Teams, b.Sim
	return []namedBound{
		{FieldTeamFormLoad, t.FormLoad}, {FieldTeamReleaseLoad, t.ReleaseLoad},
		{FieldTeamReleaseAfter, t.ReleaseAfterUS}, {FieldTeamMinSize, t.MinSize},
		{FieldTeamMaxSize, t.MaxSize}, {FieldTeamMaxShare, t.MaxShare},
		{FieldSimOperators, s.Operators}, {FieldSimCapacity, s.Capacity},
		{FieldSimHandlingMedian, s.HandlingMedianUS}, {FieldSimResolveMedian, s.ResolveMedianUS},
		{FieldSimMissedOfferShare, s.MissedOfferShare},
	}
}

// FieldError names one invalid field and says why. Field is a name from
// SettingsFields, or FieldSim, FieldBaseVersion or FieldBy. Reason is for
// people: plain English, without the field name.
type FieldError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

func (e FieldError) Error() string { return e.Field + ": " + e.Reason }

// JoinFieldErrors returns the field errors as one error, or nil when there
// is none.
func JoinFieldErrors(fes []FieldError) error {
	errs := make([]error, len(fes))
	for i, fe := range fes {
		errs[i] = fe
	}
	return errors.Join(errs...)
}

// fieldValue is one value of a set with its name and its bound.
type fieldValue struct {
	name string
	v    float64
	b    Bound
}

func (t TeamSettings) values(b TeamBounds) []fieldValue {
	return []fieldValue{
		{FieldTeamFormLoad, float64(t.FormLoad), b.FormLoad},
		{FieldTeamReleaseLoad, float64(t.ReleaseLoad), b.ReleaseLoad},
		{FieldTeamReleaseAfter, float64(t.ReleaseAfterUS), b.ReleaseAfterUS},
		{FieldTeamMinSize, float64(t.MinSize), b.MinSize},
		{FieldTeamMaxSize, float64(t.MaxSize), b.MaxSize},
		{FieldTeamMaxShare, t.MaxShare, b.MaxShare},
	}
}

func (s SimSettings) values(b SimBounds) []fieldValue {
	return []fieldValue{
		{FieldSimOperators, float64(s.Operators), b.Operators},
		{FieldSimCapacity, float64(s.Capacity), b.Capacity},
		{FieldSimHandlingMedian, float64(s.HandlingMedianUS), b.HandlingMedianUS},
		{FieldSimResolveMedian, float64(s.ResolveMedianUS), b.ResolveMedianUS},
		{FieldSimMissedOfferShare, s.MissedOfferShare, b.MissedOfferShare},
	}
}

func (s StandSettings) values() []fieldValue {
	b := Bounds()
	out := s.Teams.values(b.Teams)
	if s.Sim != nil {
		out = append(out, s.Sim.values(b.Sim)...)
	}
	return out
}

// checkValues checks each value against its bound, then the rules between
// two fields (Below and NotAbove). A rule between two fields is reported on
// the field that carries it, and only when both values are within their own
// bounds, so one wrong value gives one error.
func checkValues(vals []fieldValue) []FieldError {
	var out []FieldError
	ok := make(map[string]float64, len(vals))
	for _, f := range vals {
		// The negated test also catches NaN.
		if !(f.v >= f.b.Min && f.v <= f.b.Max) {
			out = append(out, FieldError{f.name, fmt.Sprintf("%s is not between %s and %s", num(f.v), num(f.b.Min), num(f.b.Max))})
			continue
		}
		ok[f.name] = f.v
	}
	for _, f := range vals {
		v, fine := ok[f.name]
		if !fine {
			continue
		}
		if other, set := ok[f.b.Below]; set && f.b.Below != "" && !(v < other) {
			out = append(out, FieldError{f.name, fmt.Sprintf("%s is not below %s (%s)", num(v), f.b.Below, num(other))})
		}
		if other, set := ok[f.b.NotAbove]; set && f.b.NotAbove != "" && v > other {
			out = append(out, FieldError{f.name, fmt.Sprintf("%s is above %s (%s)", num(v), f.b.NotAbove, num(other))})
		}
	}
	return out
}

// num formats a value for a reason that people read: plain decimals, never
// an exponent ("7200000000", "0.05").
func num(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// FieldErrors reports every invalid field of the teams part, in the order
// of SettingsFields.
func (t TeamSettings) FieldErrors() []FieldError {
	return checkValues(t.values(Bounds().Teams))
}

// Validate reports every invalid field of the teams part.
func (t TeamSettings) Validate() error { return JoinFieldErrors(t.FieldErrors()) }

// FieldErrors reports every invalid field of the sim part, in the order of
// SettingsFields.
func (s SimSettings) FieldErrors() []FieldError {
	return checkValues(s.values(Bounds().Sim))
}

// Validate reports every invalid field of the sim part.
func (s SimSettings) Validate() error { return JoinFieldErrors(s.FieldErrors()) }

// FieldErrors reports every invalid field of the whole set, checked
// together (decision 0014: the same rules as at start, and the bounds). An
// absent sim part is not an error here; whether it may be absent depends
// on the stored set (see PatternPutSettings).
func (s StandSettings) FieldErrors() []FieldError {
	return checkValues(s.values())
}

// Validate reports every invalid field of the whole set.
func (s StandSettings) Validate() error { return JoinFieldErrors(s.FieldErrors()) }

// Equal reports whether two sets hold the same values. Shares are compared
// as doubles.
func (s StandSettings) Equal(o StandSettings) bool {
	if s.Teams != o.Teams || (s.Sim == nil) != (o.Sim == nil) {
		return false
	}
	return s.Sim == nil || *s.Sim == *o.Sim
}

// Diff returns the values that differ from before to after, in the order of
// SettingsFields. When before has no sim part and after has one, every sim
// value is listed with Old absent. after must have a sim part when before
// has one; a sim part that goes away is not listed.
func Diff(before, after StandSettings) []FieldChange {
	old := make(map[string]float64, len(SettingsFields))
	for _, f := range before.values() {
		old[f.name] = f.v
	}
	var out []FieldChange
	for _, f := range after.values() {
		o, had := old[f.name]
		switch {
		case !had:
			out = append(out, FieldChange{Field: f.name, New: f.v})
		case o != f.v:
			out = append(out, FieldChange{Field: f.name, Old: &o, New: f.v})
		}
	}
	return out
}

// FieldChange is one value that a change replaced.
//
// The change log lists changed fields, not the whole set before and after.
// The reasons: a log of 20 whole sets before and after is about 10 KB,
// while most changes touch one or two fields; the console shows "what
// changed" without comparing two sets; and the set after a change is the
// set before the next one, so nothing is lost. Each value is a JSON number:
// integers and microseconds are far below 2^53, so a double holds them
// exactly.
type FieldChange struct {
	// Field is a name from SettingsFields.
	Field string `json:"field"`
	// Old is the value before the change. It is absent when the set had no
	// value for the field: every field of the change that stored the
	// defaults on an empty database, and the sim fields when the defaults
	// of the sim part were added to a stored set without one.
	Old *float64 `json:"old,omitempty"`
	// New is the value after the change.
	New float64 `json:"new"`
}

// SettingsChange is one entry of the change log.
type SettingsChange struct {
	// ChangedAtUS is the machine clock of the router when it applied the
	// change, in unix microseconds.
	ChangedAtUS int64 `json:"changed_at_us"`
	// Version is the version of the set after the change.
	Version int64 `json:"version"`
	// By says who made the change: ByDefaults, the label the gateway sent
	// in SettingsChangeRequest.By, or BySimulator in an old row. It may
	// hold a user ID of the site,
	// so gwapi sends it only to a connection that may change settings;
	// for any other connection it is absent.
	By string `json:"by,omitempty"`
	// Fields are the changed values, in the order of SettingsFields. Never
	// empty: a request that changes nothing is not logged.
	Fields []FieldChange `json:"fields"`
}

// StandState is the stand settings block of Settings.
type StandState struct {
	// Version is the version of the current set. Each change adds 1. A
	// router stores the defaults as version 1 at its first start, so 0
	// (nothing stored, Current holds the team defaults and has no sim part)
	// only comes from a router from before that rule.
	Version int64 `json:"version"`
	// Current is the set in use.
	Current StandSettings `json:"current"`
	// Defaults are the defaults of the code (DefaultStandSettings), with
	// the sim part exactly when Current has one. They are not versioned.
	// "Reset to defaults" is a change to this set like any other.
	Defaults StandSettings `json:"defaults"`
	// Changes are the newest entries of the change log, at most
	// MaxSettingsChanges, newest first. Only the answers that
	// SettingsWithChangesPath and PatternPutSettings name carry them; the
	// plain PatternSettings answer leaves them out so that it stays below
	// MaxBodyBytes.
	Changes []SettingsChange `json:"changes,omitempty"`
}

// Validate reports every invalid field: both sets must pass the bounds,
// the sim part is in both sets or in neither, and the change log must be in
// order and end at Version.
func (s StandState) Validate() error {
	var errs []error
	if s.Version < 0 {
		errs = append(errs, fmt.Errorf("version: %d is negative", s.Version))
	}
	for _, fe := range s.Current.FieldErrors() {
		errs = append(errs, fmt.Errorf("current.%w", fe))
	}
	for _, fe := range s.Defaults.FieldErrors() {
		errs = append(errs, fmt.Errorf("defaults.%w", fe))
	}
	if (s.Current.Sim == nil) != (s.Defaults.Sim == nil) {
		errs = append(errs, errors.New("current.sim and defaults.sim: must be both set or both absent"))
	}
	if s.Version == 0 && s.Current.Sim != nil {
		errs = append(errs, errors.New("current.sim: must be absent at version 0"))
	}
	if err := ValidateChanges(s.Changes, s.Version, true); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// ValidateChanges checks a change log for a current version: at most
// MaxSettingsChanges entries, newest first with versions going down, the
// first one at version, each with known, distinct fields in the order of
// SettingsFields and whole numbers for integer fields. withBy tells whether
// every By must be a valid label (true) or must be empty, because the
// reader may not see it (false, see gwapi).
func ValidateChanges(changes []SettingsChange, version int64, withBy bool) error {
	var errs []error
	if len(changes) > MaxSettingsChanges {
		errs = append(errs, fmt.Errorf("changes: %d are more than %d", len(changes), MaxSettingsChanges))
	}
	order := make(map[string]int, len(SettingsFields))
	for i, f := range SettingsFields {
		order[f] = i
	}
	integer := map[string]bool{}
	for _, f := range Bounds().fields() {
		integer[f.name] = f.b.Integer
	}
	for i, c := range changes {
		at := fmt.Sprintf("changes[%d]", i)
		if c.ChangedAtUS <= 0 {
			errs = append(errs, fmt.Errorf("%s.changed_at_us is not set", at))
		}
		switch {
		case c.Version <= 0:
			errs = append(errs, fmt.Errorf("%s.version: %d is not positive", at, c.Version))
		case i == 0 && c.Version != version:
			errs = append(errs, fmt.Errorf("%s.version: %d is not the current version %d", at, c.Version, version))
		case i > 0 && c.Version >= changes[i-1].Version:
			errs = append(errs, fmt.Errorf("%s.version: %d is not below the entry before", at, c.Version))
		}
		switch {
		case withBy:
			if err := CheckBy(c.By); err != nil {
				errs = append(errs, fmt.Errorf("%s.by: %w", at, err))
			}
		case c.By != "":
			errs = append(errs, fmt.Errorf("%s.by: must be empty for this reader", at))
		}
		if len(c.Fields) == 0 {
			errs = append(errs, fmt.Errorf("%s.fields is not set", at))
		}
		last := -1
		for j, f := range c.Fields {
			fat := fmt.Sprintf("%s.fields[%d]", at, j)
			pos, known := order[f.Field]
			switch {
			case !known:
				errs = append(errs, fmt.Errorf("%s.field: %q is not a known field", fat, f.Field))
				continue
			case pos <= last:
				errs = append(errs, fmt.Errorf("%s.field: %q is listed twice or out of order", fat, f.Field))
			}
			last = pos
			if !finite(f.New) || (f.Old != nil && !finite(*f.Old)) {
				errs = append(errs, fmt.Errorf("%s: old and new must be finite", fat))
				continue
			}
			if integer[f.Field] && (f.New != math.Trunc(f.New) || (f.Old != nil && *f.Old != math.Trunc(*f.Old))) {
				errs = append(errs, fmt.Errorf("%s: old and new must be whole numbers", fat))
			}
		}
	}
	return errors.Join(errs...)
}

func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// CheckBy checks a "by" label: 1 to MaxByLen bytes of printable ASCII
// (from space to "~"), so no control character and nothing that a log or
// a screen could take for markup of another kind.
func CheckBy(by string) error {
	switch {
	case by == "":
		return errors.New("is not set")
	case len(by) > MaxByLen:
		return fmt.Errorf("is longer than %d bytes", MaxByLen)
	}
	for i := range len(by) {
		if c := by[i]; c < 0x20 || c > 0x7e {
			return fmt.Errorf("byte %d is not printable ASCII", i)
		}
	}
	return nil
}

// SettingsChangeRequest is the body of PatternPutSettings.
type SettingsChangeRequest struct {
	// Settings is the whole new set. The sim part may be absent only while
	// the stored set has none, and must be absent then.
	Settings StandSettings `json:"settings"`
	// BaseVersion is the StandState.Version the set was built from. 0
	// while nothing is stored.
	BaseVersion int64 `json:"base_version"`
	// By says who asks, for the change log (see CheckBy). The gateway
	// chooses it. ByDefaults and BySimulator are reserved (ReservedBy).
	By string `json:"by"`
}

// FieldErrors reports every invalid field that the request shows by
// itself: the set against the bounds, the base version and the label. The
// rules that depend on the stored set (the sim part, the version) are the
// router's, see PatternPutSettings.
func (r SettingsChangeRequest) FieldErrors() []FieldError {
	out := r.Settings.FieldErrors()
	if r.BaseVersion < 0 {
		out = append(out, FieldError{FieldBaseVersion, fmt.Sprintf("%d is negative", r.BaseVersion)})
	}
	switch err := CheckBy(r.By); {
	case err != nil:
		out = append(out, FieldError{FieldBy, err.Error()})
	case ReservedBy(r.By):
		out = append(out, FieldError{FieldBy, fmt.Sprintf("%q is reserved", r.By)})
	}
	return out
}

// Validate reports every invalid field that the request shows by itself.
func (r SettingsChangeRequest) Validate() error { return JoinFieldErrors(r.FieldErrors()) }

// SettingsChangeResponse is the answer of PatternPutSettings when the call
// succeeded (HTTP 200).
type SettingsChangeResponse struct {
	// Result is ResultDone when the set changed, and ResultAlreadyDone
	// when the request held the current set at the current version:
	// nothing changed, nothing was logged, and the interval did not start.
	Result Result `json:"result"`
	// Settings are the settings after the call, with StandState.Changes.
	Settings Settings `json:"settings"`
}

// SettingsError is an error answer of PatternPutSettings as a Go error, for
// the caller side, like Error.
// Use errors.As to read the code and the details.
type SettingsError struct {
	// Status is the HTTP status of the answer.
	Status int
	SettingsErrorResponse
}

func (e *SettingsError) Error() string {
	msg := fmt.Sprintf("operator API: %d %s: %s", e.Status, e.Code, e.Message)
	if len(e.Fields) > 0 {
		msg += ": " + JoinFieldErrors(e.Fields).Error()
	}
	return msg
}

// SettingsErrorResponse is the body of every error answer of
// PatternPutSettings: an ErrorResponse with the details of its code. A client that knows only ErrorResponse can read it
// too.
type SettingsErrorResponse struct {
	ErrorResponse
	// Fields lists the invalid fields with CodeInvalidRequest, when the
	// body was a valid JSON object of the right shape. A body that is not
	// one gets CodeInvalidRequest without Fields.
	Fields []FieldError `json:"fields,omitempty"`
	// Settings are the current settings, with StandState.Changes, with
	// CodeSettingsConflict.
	Settings *Settings `json:"settings,omitempty"`
	// RetryAfterMS is how long to wait before the next change, in whole
	// milliseconds of machine time rounded up, from 1 to
	// SettingsChangeInterval, with CodeSettingsTooSoon.
	RetryAfterMS int64 `json:"retry_after_ms,omitempty"`
}

// Validate reports a detail that does not fit the code.
func (e SettingsErrorResponse) Validate() error {
	var errs []error
	if !e.Code.valid() {
		errs = append(errs, fmt.Errorf("code: %q is not a known code", e.Code))
	}
	if len(e.Fields) > 0 && e.Code != CodeInvalidRequest {
		errs = append(errs, fmt.Errorf("fields: only with code %s", CodeInvalidRequest))
	}
	switch {
	case e.Code == CodeSettingsConflict && e.Settings == nil:
		errs = append(errs, errors.New("settings is not set"))
	case e.Code != CodeSettingsConflict && e.Settings != nil:
		errs = append(errs, fmt.Errorf("settings: only with code %s", CodeSettingsConflict))
	case e.Settings != nil:
		if err := errors.Join(e.Settings.Validate(), e.Settings.ValidateStand()); err != nil {
			errs = append(errs, fmt.Errorf("settings: %w", err))
		} else if e.Settings.Stand == nil {
			errs = append(errs, errors.New("settings.stand is not set"))
		}
	}
	maxWait := SettingsChangeInterval.Milliseconds()
	switch {
	case e.Code == CodeSettingsTooSoon && (e.RetryAfterMS < 1 || e.RetryAfterMS > maxWait):
		errs = append(errs, fmt.Errorf("retry_after_ms: %d is not between 1 and %d", e.RetryAfterMS, maxWait))
	case e.Code != CodeSettingsTooSoon && e.RetryAfterMS != 0:
		errs = append(errs, fmt.Errorf("retry_after_ms: only with code %s", CodeSettingsTooSoon))
	}
	return errors.Join(errs...)
}
