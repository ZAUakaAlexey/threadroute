// Package defaults holds the values that the decision records fix, so the
// services start from the same numbers. They are defaults: each service's
// configuration may override them.
//
// The windows of normalize and conv are measured in source time, except
// DedupKeyLifetime, which is the lifetime of a Redis key (decisions 0003,
// 0007 and 0008). The durations of the router are model time: machine time
// multiplied by RouterSpeedFactor (decision 0009, "Clock").
package defaults

import "time"

// normalize (decision 0003).
const (
	// DedupKeyLifetime is how long a deduplication key lives in Redis
	// (decision 0003).
	DedupKeyLifetime = 10 * time.Minute
)

// Conversation lifecycle in conv (decision 0007).
const (
	// InactivityTimeout closes a conversation that had no new message for
	// this long, in source time (decision 0007).
	InactivityTimeout = 30 * time.Minute
	// TextKeptAfterClose is how long message text stays in Redis after its
	// conversation closed (decision 0007).
	TextKeptAfterClose = 10 * time.Minute
	// MessagesKept is how many of the newest messages of a conversation
	// are kept in Redis. Older ones are only counted (decision 0007).
	MessagesKept = 50
)

// Company admission in conv (decision 0008).
const (
	// AdmissionThreshold is N: a company is admitted when at least this
	// many different customers replied to it within AdmissionWindow
	// (decision 0008).
	AdmissionThreshold = 10
	// AdmissionWindow is the sliding window in which the different
	// customers are counted, in source time (decision 0008).
	AdmissionWindow = time.Hour
	// AdmissionIdle ends the admission of a company that had no customer
	// message for this long, in source time (decision 0008).
	AdmissionIdle = 2 * time.Hour
	// NoCompanyLimit is the value of the admitted company limit that means
	// "no limit" (decision 0008).
	NoCompanyLimit = 0
	// CompanyLimit is the default limit on the number of admitted
	// companies. Decision 0008 chooses the real default after the one-hour
	// measurement in stage 2. Until then it is NoCompanyLimit.
	CompanyLimit = NoCompanyLimit
)

// Routing in the router (decisions 0009 and 0010). Durations are model time.
const (
	// OperatorCapacity is how many conversations an operator handles at
	// once when it is created without a capacity (decision 0009).
	OperatorCapacity = 4
	// AcceptTimeout is how long an operator has to accept an offer.
	AcceptTimeout = 20 * time.Second
	// AssignmentTarget is the SLA target from entering the queue until an
	// operator accepts.
	AssignmentTarget = 60 * time.Second
	// FirstResponseTarget is the SLA target until the first reply of the
	// operator.
	FirstResponseTarget = 120 * time.Second
	// TeamFormLoad is the active load of a company at which it gets a
	// dedicated team.
	TeamFormLoad = 20
	// TeamReleaseLoad releases a team when the active load stays below it
	// for TeamReleaseAfter.
	TeamReleaseLoad = 8
	// TeamReleaseAfter is how long the load must stay below
	// TeamReleaseLoad.
	TeamReleaseAfter = 15 * time.Minute
	// TeamMinSize and TeamMaxSize bound the size of a team.
	TeamMinSize = 2
	TeamMaxSize = 20
	// RouterSpeedFactor multiplies the machine clock of the router timers.
	// 1 for live traffic (decision 0009, "Clock").
	RouterSpeedFactor = 1.0
)
