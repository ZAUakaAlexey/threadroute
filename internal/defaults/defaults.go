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

// Operator snapshots in the routing topic (decision 0011). This one is
// machine time, not model time: it follows the retention of the topic.
const (
	// OperatorRefresh is how long after its last operator event the router
	// publishes the snapshot of an operator again. The routing topic keeps
	// 24 hours, so a consumer that builds its view from the topic keeps
	// every operator.
	OperatorRefresh = 6 * time.Hour
)

// Simulated operators in sim (decision 0010). Durations are model time, like
// the router ones: sim divides them by the speed factor of the router.
const (
	// SimOperators is the number of simulated operators. It was sized after
	// the first live run of stage 2, from the number of open conversations.
	SimOperators = 200
	// SimLanguages is the language spread of the operators, as
	// "lang:count" pairs. It is a starting value: one measured hour at
	// night (UTC) had 98 percent English posts, and the spread will be
	// tuned after a full-day measurement of the stream. The counts add up
	// to SimOperators. Every operator also serves the general queue (und).
	SimLanguages = "en:176,ja:6,de:4,fr:4,es:4,pt:3,ko:3"
	// SimShiftOnline and SimShiftOffline are the two parts of a shift: an
	// operator is online this long, then offline this long.
	SimShiftOnline  = 4 * time.Hour
	SimShiftOffline = 30 * time.Minute
	// SimAcceptDelayMin and SimAcceptDelayMax bound the random delay before
	// an operator accepts an offer.
	SimAcceptDelayMin = time.Second
	SimAcceptDelayMax = 8 * time.Second
	// SimMissedOfferShare is the share of offers that an operator does not
	// answer at all, so the accept timeout runs out.
	SimMissedOfferShare = 0.03
	// SimHandlingMedian is the median time from the accept to the reply
	// marker.
	SimHandlingMedian = 45 * time.Second
	// SimResolveMedian is the median time from the reply to the resolve.
	SimResolveMedian = 3 * time.Minute
)

// Gateway (decisions 0011 and 0012). Durations are machine time: they are
// about browsers and connections, not about routing.
const (
	// GatewayTick is how often the gateway sends the current state of the
	// keys that changed to a browser (decision 0011, point 4).
	GatewayTick = 250 * time.Millisecond
	// GatewayHelloTimeout closes a connection that has not sent a valid
	// first message within this time (decision 0012, point 5).
	GatewayHelloTimeout = 5 * time.Second
	// GatewayOfflineGrace is how long a human operator stays online after
	// their connection closed, so a page reload keeps their conversations
	// (decision 0012, point 4).
	GatewayOfflineGrace = 15 * time.Second
	// GatewayDemoOperators is the most demo operators (anyone, without
	// sign-in) at the same time (decision 0012, point 3).
	GatewayDemoOperators = 5
	// GatewayHumanCapacity is the capacity of a human operator (decision
	// 0012, point 4).
	GatewayHumanCapacity = 2
)
