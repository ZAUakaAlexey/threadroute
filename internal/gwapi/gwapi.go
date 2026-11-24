// Package gwapi is the contract between the gateway and the browser
// (decisions 0011, 0012 and 0013): the WebSocket messages, their validation
// and JSON encoding, the close codes, the limits, and the JSON of the public
// snapshot. It holds no server and no client logic, only what both sides
// must agree on. docs/gateway-protocol.md describes the same protocol for a
// front-end developer, and the files in testdata/ are examples that both
// the Go tests and the console tests read.
//
// # Messages
//
// Every WebSocket message is one text frame with one JSON object. The field
// "type" names the message. The client sends ClientMessage values (Hello,
// Watch, Unwatch, Action, ChangeSettings) and the server sends
// ServerMessage values (Welcome, Status, State, Messages, MessageRemoved,
// Result, SettingsUpdate, TrendUpdate). Readers ignore unknown fields.
// Times are unix microseconds in fields that end in "_us", as in the event
// package.
//
// # Privacy
//
// A server message is checked against the Audience it is sent to before it
// is encoded: at LevelDemo it may hold no post text, no conversation key,
// no company ID and no customer ID, only pseudonyms (decision 0012). The
// public snapshot holds totals only (decision 0013). The "by" label of a
// stand settings change goes only to a connection that may change settings
// (decision 0014), whatever its level.
package gwapi

import (
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// ProtocolVersion is the version of this protocol. A client sends it in
// Hello. The server closes a connection whose Hello names another version
// with CloseUnsupportedVersion, without a Welcome. It grows by one with
// every change that an older client cannot ignore; adding a field does not
// change it.
const ProtocolVersion = 1

// HTTP paths of the gateway.
const (
	// WebSocketPath is the path of the WebSocket of the console. Nothing
	// secret is put into its URL (decision 0012, point 5).
	WebSocketPath = "/v1/ws"
	// PublicSnapshotPath is the path of the public snapshot: GET, answer
	// PublicSnapshot as JSON (decision 0013, point 1).
	PublicSnapshotPath = "/v1/public/snapshot"
)

// Default listen addresses of the gateway.
const (
	// DefaultMetricsAddr serves /metrics, /healthz and /debug/pprof/, as
	// in every other service.
	DefaultMetricsAddr = ":9471"
	// DefaultHTTPAddr serves WebSocketPath and PublicSnapshotPath.
	DefaultHTTPAddr = ":9472"
)

// Timing.
const (
	// HelloTimeout is how long the server waits for a valid Hello after
	// the connection opened. Then it closes with CloseHelloTimeout.
	HelloTimeout = defaults.GatewayHelloTimeout
	// Tick is the shortest time between two State messages to one client
	// (decision 0011, point 4).
	Tick = defaults.GatewayTick
	// OfflineGrace is how long a human operator stays online after their
	// connection closed (decision 0012, point 4).
	OfflineGrace = defaults.GatewayOfflineGrace
	// PublicSnapshotEvery is how often the gateway rebuilds the public
	// snapshot. The web server may cache it for the same time (decision
	// 0013, point 1).
	PublicSnapshotEvery = time.Second
	// PublicPollEvery is how often the public page asks for the snapshot.
	PublicPollEvery = 2 * time.Second
	// TrendInterval is the machine time between two trend points
	// (decision 0014, point 5).
	TrendInterval = 10 * time.Second
	// TrendHistory is how far back the trend points of a Welcome go.
	TrendHistory = time.Hour
)

// MaxTrendPoints is the most points of Trend.Points: one hour of
// points every 10 seconds.
const MaxTrendPoints = int(TrendHistory / TrendInterval)

// Limits. The server enforces them; a client that knows them can avoid the
// errors.
const (
	// MaxClientMessageBytes is the largest message a client may send. A
	// larger one closes the connection with CloseMessageTooBig.
	MaxClientMessageBytes = 4096
	// MaxTokenBytes is the longest site token in Hello.
	MaxTokenBytes = 2048
	// MaxResumeTokenBytes is the longest resume token.
	MaxResumeTokenBytes = 256
	// MaxRequestIDLen is the longest request ID. A request ID follows
	// event.ValidRoutingID, because the request ID of a "reply" action is
	// the reply ID of the operator API.
	MaxRequestIDLen = event.MaxRoutingIDLen
	// ActionsPerSecond and ActionBurst limit the requests (Action, Watch
	// and Unwatch) of one connection: a token bucket that holds
	// ActionBurst tokens and gets ActionsPerSecond tokens per second. A
	// request over the limit gets a Result with CodeRateLimited.
	ActionsPerSecond = 5
	ActionBurst      = 10
	// MaxWatched is how many conversations one connection may watch at
	// the same time.
	MaxWatched = 4
	// MaxLanguages is the most languages of a human operator.
	MaxLanguages = 32
)

// HumanOperatorPrefix starts the operator ID of every human operator that
// the gateway creates (decision 0012, point 4, "Operator kind"). The
// simulator uses "sim-op-", so the two never collide.
const HumanOperatorPrefix = "web-"

// HumanCapacity is the capacity of a human operator (decision 0012).
const HumanCapacity = defaults.GatewayHumanCapacity

// HumanPriority is the operator priority of a human operator (decisions
// 0009 and 0012). The gateway sends it to the router; the protocol to the
// browser does not carry it.
const HumanPriority = defaults.GatewayHumanPriority

// View is the screen a connection serves. It is chosen in Hello and does
// not change for the life of the connection.
type View string

// Views.
const (
	// ViewOperator: the operator screen. The gateway creates an operator
	// for the connection.
	ViewOperator View = "operator"
	// ViewSupervisor: the supervisor screen.
	ViewSupervisor View = "supervisor"
)

func (v View) valid() bool { return v == ViewOperator || v == ViewSupervisor }

// Level is the access level of a connection (decision 0012). The public
// level has no WebSocket and is not a Level.
type Level string

// Levels.
const (
	// LevelDemo: no post text, no conversation keys, no author IDs and no
	// company IDs, only pseudonyms. Anyone gets it, with or without
	// signing in.
	LevelDemo Level = "demo"
	// LevelFull: post text and IDs. Only users the project owner allowed.
	LevelFull Level = "full"
)

func (l Level) valid() bool { return l == LevelDemo || l == LevelFull }

// Audience is who a server message is for: the level and the view of the
// connection, and its right to change the stand settings. Server messages
// are validated against it. An audience with an unknown level or view is
// refused, and the privacy rules treat every level other than LevelFull
// like LevelDemo.
type Audience struct {
	Level Level
	View  View
	// MayChangeSettings is the right of the connection to change the stand
	// settings (decision 0014, point 6). It does not depend on Level: the
	// gateway has one list of roles for each. The zero value, false, is
	// the safe one: such a connection never gets the "by" label of a
	// change, which may hold a user ID of the site.
	MayChangeSettings bool
}

func (a Audience) validate(errs *fieldErrors) {
	if !a.Level.valid() {
		errs.invalid("audience", "level %q is not %q or %q", a.Level, LevelDemo, LevelFull)
	}
	if !a.View.valid() {
		errs.invalid("audience", "view %q is not %q or %q", a.View, ViewOperator, ViewSupervisor)
	}
}

// Kind tells a human operator from a simulated one.
type Kind string

// Kinds.
const (
	KindHuman     Kind = "human"
	KindSimulated Kind = "simulated"
)

// KindOf returns the kind of an operator by its ID prefix.
func KindOf(operatorID string) Kind {
	if len(operatorID) > len(HumanOperatorPrefix) && operatorID[:len(HumanOperatorPrefix)] == HumanOperatorPrefix {
		return KindHuman
	}
	return KindSimulated
}

// Presence is the presence state of an operator.
type Presence string

// Presence states.
const (
	PresenceOnline  Presence = "online"
	PresenceOffline Presence = "offline"
)

func (p Presence) valid() bool { return p == PresenceOnline || p == PresenceOffline }
