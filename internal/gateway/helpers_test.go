package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/conv"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// Synthetic test data. No real IDs and no real text (decision 0004).
const (
	testCompany  = "did:plc:synthcompany00000000000001"
	testCompany2 = "did:plc:synthcompany00000000000002"
	testCustomer = "did:plc:synthcustomer9999999999999"
	testText     = "synthetic customer question text"
	testMsgID    = "synth-msg-0001"
)

// t0 is a fixed machine time of the tests (2026-01-02).
var t0 = time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)

func customer(i int) string { return fmt.Sprintf("did:plc:synthcustomer%013d", i) }

func convKey(company, cust string) string { return string(event.ConversationKey(company, cust)) }

// convEv builds a conversation event.
func convEv(typ event.ConversationType, company, cust string, session, seq uint64, at time.Time) event.Conversation {
	c := event.Conversation{
		Type: typ, Channel: event.ChannelBluesky, CompanyID: company, CustomerID: cust,
		Session: session, Seq: seq, SourceTime: at, OpenedAt: t0.Add(-time.Minute), Lang: "en",
		CustomerMessages: seq, CompanyMessages: 0,
	}
	switch typ {
	case event.TypeOpened:
		c.MessageID, c.Direction = fmt.Sprintf("synth-msg-%d-%d", session, seq), event.FromCustomer
	case event.TypeMessageAdded:
		c.MessageID, c.Direction = fmt.Sprintf("synth-msg-%d-%d", session, seq), event.FromCustomer
	case event.TypeAnswered:
		c.MessageID, c.Direction, c.ResponseTime = fmt.Sprintf("synth-msg-%d-%d", session, seq), event.FromCompany, time.Second
	case event.TypeClosed:
		c.CloseReason = event.CloseInactivity
	case event.TypeMessageRemoved:
		c.MessageID, c.Direction, c.RemoveReason = testMsgID, event.FromCustomer, event.RemoveDeleted
	}
	if err := c.Validate(); err != nil {
		panic(err)
	}
	return c
}

// rt builds a routing conversation event.
func rt(typ event.RoutingType, company, cust string, session, seq uint64, at time.Time, op, offer string) event.Routing {
	r := event.Routing{Type: typ, CompanyID: company, CustomerID: cust, Session: session, Seq: seq, At: at, Lang: "en"}
	switch typ {
	case event.TypeQueued:
		r.QueuedAt, r.QueueReason = at, event.QueueNew
	case event.TypeOffered:
		r.QueuedAt, r.OperatorID, r.OfferID, r.AcceptBy = at, op, offer, at.Add(20*time.Second)
	case event.TypeAccepted:
		r.QueuedAt, r.OperatorID, r.OfferID, r.Wait = at.Add(-5*time.Second), op, offer, 5*time.Second
	case event.TypeOfferExpired:
		r.QueuedAt, r.OperatorID, r.OfferID, r.OfferEndReason = at, op, offer, event.OfferTimeout
	case event.TypeReplied:
		r.OperatorID, r.OfferID, r.FirstReply, r.ResponseTime = op, offer, true, 3*time.Second
	case event.TypeResolved:
		r.OperatorID, r.OfferID = op, offer
	case event.TypeSLAMissed:
		r.SLATarget, r.Measured, r.Target, r.Priority = event.SLAAssignment, time.Minute, time.Minute, 1
	case event.TypeEnded:
		r.EndReason = event.CloseInactivity
	}
	if err := r.Validate(); err != nil {
		panic(err)
	}
	return r
}

func opEv(typ event.RoutingType, id string, seq uint64, at time.Time, presence event.Presence, langs ...string) event.Routing {
	if len(langs) == 0 {
		langs = []string{"en", "und"}
	}
	r := event.Routing{Type: typ, OperatorID: id, Seq: seq, At: at, Languages: langs, Capacity: 2, Presence: presence}
	if err := r.Validate(); err != nil {
		panic(err)
	}
	return r
}

func teamEv(typ event.RoutingType, company string, seq uint64, at time.Time, members ...string) event.Routing {
	r := event.Routing{Type: typ, CompanyID: company, Seq: seq, At: at, Members: members, ActiveLoad: 21}
	if typ == event.TypeTeamReleased {
		r.ReleaseReason = event.TeamLowLoad
	}
	if err := r.Validate(); err != nil {
		panic(err)
	}
	return r
}

func ci(ev event.Conversation, at time.Time) Item { return Item{Conv: &ev, Time: at} }
func ri(ev event.Routing, at time.Time) Item      { return Item{Routing: &ev, Time: at} }

// fakeClock is a settable clock with timers that fire on Advance.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at      time.Time
	f       func()
	stopped bool
	c       *fakeClock
}

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now.Add(d), f: f, c: c}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock and runs the timers that are due.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*fakeTimer
	for _, t := range c.timers {
		if !t.stopped && !t.at.After(c.now) {
			t.stopped = true
			due = append(due, t)
		}
	}
	c.mu.Unlock()
	for _, t := range due {
		t.f()
	}
}

// fakeAPI is the router API. The stand settings calls go to router when
// it is set, through the real opclient; without it the API is a router
// from before decision 0014.
type fakeAPI struct {
	mu       sync.Mutex
	calls    []string
	err      error
	result   opapi.Result
	settings opapi.Settings
	router   *fakeRouter
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{result: opapi.ResultDone, settings: opapi.Settings{
		SpeedFactor: 1, AcceptTimeoutUS: 20_000_000, CapacityDefault: 4,
		AssignmentTargetUS: 60_000_000, FirstResponseTargetUS: 120_000_000,
	}}
}

func (f *fakeAPI) record(s string) (opapi.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
	return f.result, f.err
}

func (f *fakeAPI) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func (f *fakeAPI) PutOperator(_ context.Context, id string, req opapi.OperatorRequest) (opapi.OperatorResponse, error) {
	res, err := f.record(fmt.Sprintf("put %s %v %d p%d", id, req.Languages, req.Capacity, req.Priority))
	if err != nil {
		return opapi.OperatorResponse{}, err
	}
	return opapi.OperatorResponse{Result: res, Operator: opapi.Operator{
		ID: id, Languages: slices.Sorted(slices.Values(req.Languages)), Capacity: req.Capacity, Priority: req.Priority,
		Presence: opapi.PresenceOffline,
	}}, nil
}
func (f *fakeAPI) Online(_ context.Context, id string) (opapi.Result, error) {
	return f.record("online " + id)
}
func (f *fakeAPI) Offline(_ context.Context, id string) (opapi.Result, error) {
	return f.record("offline " + id)
}
func (f *fakeAPI) Accept(_ context.Context, id, offer string) (opapi.Result, error) {
	return f.record("accept " + id + " " + offer)
}
func (f *fakeAPI) Refuse(_ context.Context, id, offer string) (opapi.Result, error) {
	return f.record("refuse " + id + " " + offer)
}
func (f *fakeAPI) Reply(_ context.Context, id, offer, reply string) (opapi.Result, error) {
	return f.record("reply " + id + " " + offer + " " + reply)
}
func (f *fakeAPI) Resolve(_ context.Context, id, offer string) (opapi.Result, error) {
	return f.record("resolve " + id + " " + offer)
}
func (f *fakeAPI) Settings(context.Context) (opapi.Settings, error) { return f.settings, nil }

func (f *fakeAPI) stand() (*fakeRouter, opapi.Settings) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.router, f.settings
}

func (f *fakeAPI) SettingsWithChanges(ctx context.Context) (opapi.Settings, error) {
	r, s := f.stand()
	if r != nil {
		return r.client.SettingsWithChanges(ctx)
	}
	return s, nil
}

func (f *fakeAPI) PutSettings(ctx context.Context, req opapi.SettingsChangeRequest) (opapi.SettingsChangeResponse, error) {
	if r, _ := f.stand(); r != nil {
		return r.client.PutSettings(ctx, req)
	}
	f.record("put settings")
	return opapi.SettingsChangeResponse{}, &opapi.SettingsError{Status: 405, SettingsErrorResponse: opapi.SettingsErrorResponse{
		ErrorResponse: opapi.ErrorResponse{Code: opapi.CodeSettingsOff},
	}}
}

// Tokens of fakeAuth.
const (
	// fullToken gives the full level, user 7.
	fullToken = "full-token"
	// settingsToken gives the demo level with the right to change
	// settings, user 9.
	settingsToken = "settings-token"
	// fullSettingsToken gives the full level and the right, user 10.
	fullSettingsToken = "full-settings-token"
	// rejectedToken is a token the site rejects.
	rejectedToken = "rejected-token"
)

// fakeAuth gives the identities of the tokens above, and demo to every
// other token.
type fakeAuth struct{}

func (fakeAuth) Check(_ context.Context, token string) (Identity, CheckOutcome) {
	switch token {
	case fullToken:
		return Identity{Level: gwapi.LevelFull, UserID: 7}, CheckFull
	case settingsToken:
		return Identity{Level: gwapi.LevelDemo, UserID: 9, MayChangeSettings: true}, CheckSignedIn
	case fullSettingsToken:
		return Identity{Level: gwapi.LevelFull, UserID: 10, MayChangeSettings: true}, CheckFull
	case rejectedToken:
		return Anonymous, CheckRejected
	case "":
		return Anonymous, CheckNoToken
	}
	return Identity{Level: gwapi.LevelDemo, UserID: 8}, CheckSignedIn
}

// fakeText holds the stored messages by conversation key.
type fakeText struct {
	mu   sync.Mutex
	msgs map[string][]conv.StoredMessage
}

func (f *fakeText) put(company, cust string, m conv.StoredMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.msgs == nil {
		f.msgs = map[string][]conv.StoredMessage{}
	}
	k := convKey(company, cust)
	f.msgs[k] = append(f.msgs[k], m)
}

func (f *fakeText) Messages(_ context.Context, company, cust string) ([]conv.StoredMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.msgs[convKey(company, cust)]), nil
}

func (f *fakeText) Message(_ context.Context, company, cust, id string) (conv.StoredMessage, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.msgs[convKey(company, cust)] {
		if m.ID == id {
			return m, true, nil
		}
	}
	return conv.StoredMessage{}, false, nil
}

// fakeConn is a Conn with channels. Frames written are kept in order.
type fakeConn struct {
	in     chan []byte
	frames chan []byte
	closed chan struct{}
	once   sync.Once
	code   gwapi.CloseCode
	// gate, when set, blocks every Write until it is closed or receives.
	gate chan struct{}
	mu   sync.Mutex
}

func newFakeConn() *fakeConn {
	return &fakeConn{in: make(chan []byte, 16), frames: make(chan []byte, 10000), closed: make(chan struct{})}
}

func (f *fakeConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case b := <-f.in:
		if len(b) > gwapi.MaxClientMessageBytes {
			return nil, ErrTooBig
		}
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.closed:
		return nil, io.EOF
	}
}

func (f *fakeConn) Write(ctx context.Context, b []byte) error {
	f.mu.Lock()
	gate := f.gate
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		case <-f.closed:
			return errors.New("closed")
		}
	}
	select {
	case <-f.closed:
		return errors.New("closed")
	default:
	}
	f.frames <- slices.Clone(b)
	return nil
}

func (f *fakeConn) Ping(context.Context) error { return nil }

func (f *fakeConn) Close(code gwapi.CloseCode) error {
	f.once.Do(func() {
		f.mu.Lock()
		f.code = code
		f.mu.Unlock()
		close(f.closed)
	})
	return nil
}

func (f *fakeConn) closeCode() gwapi.CloseCode {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.code
}

func (f *fakeConn) send(t testing.TB, m gwapi.ClientMessage) {
	t.Helper()
	b, err := gwapi.EncodeClient(m)
	if err != nil {
		t.Fatal(err)
	}
	f.in <- b
}

// next returns the next frame within a second.
func (f *fakeConn) next(t testing.TB) []byte {
	t.Helper()
	select {
	case b := <-f.frames:
		return b
	case <-time.After(2 * time.Second):
		t.Fatal("no frame within 2 seconds")
		return nil
	}
}

// nextOf returns the next frame of the type typ, skipping others.
func (f *fakeConn) nextOf(t testing.TB, typ gwapi.ServerType) []byte {
	t.Helper()
	for {
		b := f.next(t)
		if frameType(t, b) == typ {
			return b
		}
	}
}

func frameType(t testing.TB, b []byte) gwapi.ServerType {
	t.Helper()
	var env struct {
		Type gwapi.ServerType `json:"type"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatal(err)
	}
	return env.Type
}

// env is a Gateway with fakes.
type env struct {
	t     *testing.T
	g     *Gateway
	clock *fakeClock
	api   *fakeAPI
	text  *fakeText
	reg   *prometheus.Registry
	wg    sync.WaitGroup
	ctx   context.Context
	stop  context.CancelFunc
}

func newEnv(t *testing.T, mutate ...func(*Config)) *env {
	t.Helper()
	return newEnvAuth(t, fakeAuth{}, mutate...)
}

// newEnvAuth is newEnv with another token checker.
func newEnvAuth(t *testing.T, auth TokenChecker, mutate ...func(*Config)) *env {
	t.Helper()
	cfg := DefaultConfig()
	cfg.OfflineGrace = 15 * time.Second
	for _, m := range mutate {
		m(&cfg)
	}
	e := &env{t: t, clock: newFakeClock(t0), api: newFakeAPI(), text: &fakeText{}, reg: prometheus.NewRegistry()}
	g, err := New(Deps{
		Config: cfg, API: e.api, Auth: auth, Text: e.text, Clock: e.clock,
		Logger: slog.New(slog.DiscardHandler), Registerer: e.reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	g.SetSettings(e.api.settings)
	g.SetStatus(gwapi.Status{})
	e.g = g
	e.ctx, e.stop = context.WithCancel(context.Background())
	t.Cleanup(func() {
		e.stop()
		e.wg.Wait()
	})
	return e
}

// apply applies items and publishes the changes.
func (e *env) apply(items ...Item) {
	e.g.view.ApplyBatch(items)
	e.g.Publish()
}

// connect opens a connection, sends hello and returns the conn and the
// decoded welcome.
func (e *env) connect(h gwapi.Hello, addr string) (*fakeConn, gwapi.Welcome) {
	e.t.Helper()
	c := newFakeConn()
	e.wg.Go(func() { e.g.Serve(e.ctx, c, addr) })
	if h.Version == 0 {
		h.Version = gwapi.ProtocolVersion
	}
	c.send(e.t, h)
	b := c.next(e.t)
	var w gwapi.Welcome
	if frameType(e.t, b) != gwapi.TypeWelcome {
		e.t.Fatalf("first frame %s", b)
	}
	if err := json.Unmarshal(b, &w); err != nil {
		e.t.Fatal(err)
	}
	return c, w
}

func operatorHello(langs ...string) gwapi.Hello {
	if len(langs) == 0 {
		langs = []string{"en", "und"}
	}
	return gwapi.Hello{Version: gwapi.ProtocolVersion, View: gwapi.ViewOperator, Languages: langs}
}

func decodeState(t testing.TB, b []byte, a gwapi.Audience) gwapi.State {
	t.Helper()
	m, err := gwapi.DecodeServer(b, a)
	if err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	st, ok := m.(gwapi.State)
	if !ok {
		t.Fatalf("frame is %T", m)
	}
	return st
}

// waitFor polls cond for up to 2 seconds.
func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func prometheusRegistry() *prometheus.Registry { return prometheus.NewRegistry() }
