package sim

import (
	"math"
	"math/rand/v2"
	"time"
)

// Clock is the machine clock of the simulator. The service uses the system
// clock and the tests a fake one.
type Clock interface {
	Now() time.Time
	// After returns a channel that receives once d has passed.
	After(d time.Duration) <-chan time.Time
}

// SystemClock is the clock of the machine.
type SystemClock struct{}

// Now returns time.Now.
func (SystemClock) Now() time.Time { return time.Now() }

// After returns time.After(d).
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// timeline converts between machine time and model time (decision 0009,
// "Clock"). Model time counts from the start of the simulator and runs
// speed times faster than the machine clock.
type timeline struct {
	start time.Time
	speed float64
}

// model returns the model time at the machine time t.
func (tl timeline) model(t time.Time) time.Duration {
	return time.Duration(float64(t.Sub(tl.start)) * tl.speed)
}

// machine returns the machine time at the model time m. It rounds up, so
// model(machine(m)) is never before m within float precision, and a timer
// armed for machine(m) never fires before m.
func (tl timeline) machine(m time.Duration) time.Time {
	return tl.start.Add(time.Duration(math.Ceil(float64(m) / tl.speed)))
}

// toModel turns a machine duration into a model duration.
func (tl timeline) toModel(d time.Duration) time.Duration {
	return time.Duration(float64(d) * tl.speed)
}

// Distributions of the operator behaviour. Each draw takes the generator,
// so the order of draws decides the result and one seed repeats a run.

// Handling and resolve times are log-normal: median * exp(sigma * Z) with a
// standard normal Z. The median is the configured one. With sigma 1 the mean
// is about 1.65 times the median, and 5 percent of the draws are above 5.2
// times the median: a long tail, as in real handling times. A draw is capped
// at tailCap times the median (less than 0.2 percent of the draws reach
// it), so one draw cannot hold a slot for hours: 15 minutes for the default
// handling median of 45 seconds, 60 minutes for the default resolve median
// of 3 minutes.
const (
	tailSigma = 1.0
	tailCap   = 20
)

// logNormal draws a log-normal duration with the given median, capped at
// tailCap times the median.
func logNormal(rng *rand.Rand, median time.Duration) time.Duration {
	return scaled(median, tailFactor(rng))
}

// tailFactor draws the factor of a log-normal duration: the duration is the
// median times this factor. Drawing the factor apart from the median lets
// the simulator draw it at a fixed point and apply the median that is in
// effect when the step is planned (decision 0014).
func tailFactor(rng *rand.Rand) float64 {
	return min(math.Exp(tailSigma*rng.NormFloat64()), tailCap)
}

// scaled returns the median times a factor of tailFactor.
func scaled(median time.Duration, f float64) time.Duration {
	return time.Duration(float64(median) * f)
}

// uniform draws a duration in [lo, hi].
func uniform(rng *rand.Rand, lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rng.Int64N(int64(hi-lo)+1))
}

// newRand returns the generator for a seed. PCG is small, fast and fully
// defined by the seed.
func newRand(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}
