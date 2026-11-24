package router

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// Clock gives the machine time. The router reads it only through this
// interface, so tests can use a fake clock. It never uses time.Now or the
// database clock for a routing decision.
type Clock interface {
	Now() time.Time
}

// SystemClock is the clock of the machine.
type SystemClock struct{}

// Now returns time.Now.
func (SystemClock) Now() time.Time { return time.Now() }

// Config holds the routing numbers of decisions 0009 and 0010. Every
// duration is model time: machine time multiplied by SpeedFactor.
type Config struct {
	// SpeedFactor multiplies the machine clock of every timer (decision
	// 0009, "Clock"). 1 for live traffic.
	SpeedFactor float64
	// Capacity is the capacity of an operator created without one.
	Capacity int
	// AcceptTimeout is how long an operator has to accept an offer.
	AcceptTimeout time.Duration
	// AssignmentTarget and FirstResponseTarget are the two SLA targets.
	AssignmentTarget    time.Duration
	FirstResponseTarget time.Duration
	// TeamFormLoad is the active load of a company that forms a team.
	TeamFormLoad int
	// TeamReleaseLoad and TeamReleaseAfter release a team whose active load
	// stayed below TeamReleaseLoad for TeamReleaseAfter.
	TeamReleaseLoad  int
	TeamReleaseAfter time.Duration
	// TeamMinSize and TeamMaxSize bound the size of a team.
	TeamMinSize int
	TeamMaxSize int
}

// DefaultConfig returns the starting values of decisions 0009 and 0010.
func DefaultConfig() Config {
	return Config{
		SpeedFactor:         defaults.RouterSpeedFactor,
		Capacity:            defaults.OperatorCapacity,
		AcceptTimeout:       defaults.AcceptTimeout,
		AssignmentTarget:    defaults.AssignmentTarget,
		FirstResponseTarget: defaults.FirstResponseTarget,
		TeamFormLoad:        defaults.TeamFormLoad,
		TeamReleaseLoad:     defaults.TeamReleaseLoad,
		TeamReleaseAfter:    defaults.TeamReleaseAfter,
		TeamMinSize:         defaults.TeamMinSize,
		TeamMaxSize:         defaults.TeamMaxSize,
	}
}

// maxSpeedFactor bounds the factor, so a machine duration of a timer is
// never shorter than a few microseconds.
const maxSpeedFactor = 10000

// Validate reports every invalid field.
func (c Config) Validate() error {
	var errs []error
	if math.IsNaN(c.SpeedFactor) || c.SpeedFactor <= 0 || c.SpeedFactor > maxSpeedFactor {
		errs = append(errs, fmt.Errorf("speed factor %v is not in (0, %d]", c.SpeedFactor, maxSpeedFactor))
	}
	if c.Capacity < 1 || c.Capacity > opapi.MaxCapacity {
		errs = append(errs, fmt.Errorf("capacity %d is not between 1 and %d", c.Capacity, opapi.MaxCapacity))
	}
	for _, d := range []struct {
		name string
		v    time.Duration
	}{
		{"accept timeout", c.AcceptTimeout},
		{"assignment target", c.AssignmentTarget},
		{"first response target", c.FirstResponseTarget},
		{"team release time", c.TeamReleaseAfter},
	} {
		switch {
		case d.v <= 0:
			errs = append(errs, fmt.Errorf("%s %s is not positive", d.name, d.v))
		case d.v%time.Microsecond != 0:
			errs = append(errs, fmt.Errorf("%s %s is not a whole number of microseconds", d.name, d.v))
		}
	}
	if c.TeamReleaseLoad < 1 {
		errs = append(errs, fmt.Errorf("team release load %d is less than 1", c.TeamReleaseLoad))
	}
	// Decision 0009: the two thresholds differ on purpose.
	if c.TeamFormLoad <= c.TeamReleaseLoad {
		errs = append(errs, fmt.Errorf("team form load %d is not above the release load %d", c.TeamFormLoad, c.TeamReleaseLoad))
	}
	if c.TeamMinSize < 1 {
		errs = append(errs, fmt.Errorf("team min size %d is less than 1", c.TeamMinSize))
	}
	if c.TeamMaxSize < c.TeamMinSize {
		errs = append(errs, fmt.Errorf("team max size %d is less than the min size %d", c.TeamMaxSize, c.TeamMinSize))
	}
	return errors.Join(errs...)
}

// Settings returns the settings of the operator API: what the simulator
// needs, and the two SLA targets for the console (decision 0011).
func (c Config) Settings() opapi.Settings {
	return opapi.Settings{
		SpeedFactor:           c.SpeedFactor,
		AcceptTimeoutUS:       c.AcceptTimeout.Microseconds(),
		CapacityDefault:       c.Capacity,
		AssignmentTargetUS:    c.AssignmentTarget.Microseconds(),
		FirstResponseTargetUS: c.FirstResponseTarget.Microseconds(),
	}
}

// machine turns a model duration into machine time, in whole
// microseconds and at least one microsecond.
func (c Config) machine(model time.Duration) time.Duration {
	d := time.Duration(float64(model) / c.SpeedFactor).Truncate(time.Microsecond)
	return max(d, time.Microsecond)
}

// model turns a machine duration into model time, in whole microseconds.
// A negative duration (clocks of two routers that differ a little) gives
// 0.
func (c Config) model(machine time.Duration) time.Duration {
	if machine <= 0 {
		return 0
	}
	return time.Duration(float64(machine) * c.SpeedFactor).Truncate(time.Microsecond)
}

// teamSize returns the team size for an active load (decision 0009): the
// load divided by the capacity, rounded up, between the min and the max.
func (c Config) teamSize(load int64) int {
	n := int((load + int64(c.Capacity) - 1) / int64(c.Capacity))
	return min(max(n, c.TeamMinSize), c.TeamMaxSize)
}
