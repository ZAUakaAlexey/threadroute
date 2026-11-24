package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// OperatorAPI is the part of the operator API (internal/router/opclient)
// that the gateway calls.
type OperatorAPI interface {
	PutOperator(ctx context.Context, operatorID string, req opapi.OperatorRequest) (opapi.OperatorResponse, error)
	Online(ctx context.Context, operatorID string) (opapi.Result, error)
	Offline(ctx context.Context, operatorID string) (opapi.Result, error)
	Accept(ctx context.Context, operatorID, offerID string) (opapi.Result, error)
	Refuse(ctx context.Context, operatorID, offerID string) (opapi.Result, error)
	Reply(ctx context.Context, operatorID, offerID, replyID string) (opapi.Result, error)
	Resolve(ctx context.Context, operatorID, offerID string) (opapi.Result, error)
	Settings(ctx context.Context) (opapi.Settings, error)
}

// Clock is the machine clock and its timers. Tests use a fake one.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer can be stopped.
type Timer interface{ Stop() bool }

// SystemClock is the real clock.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }

// AfterFunc implements Clock.
func (SystemClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

// Errors of Attach.
var (
	// ErrNoSeat: every demo operator seat is taken (close code 4503).
	ErrNoSeat = errors.New("no demo operator seat is free")
	// ErrEnded: the operator session ended (grace time over, or stop).
	ErrEnded = errors.New("the operator session ended")
)

// offlineAttempts and offlinePause bound putting an operator offline after
// the grace time. A failure is retried later by the sweep of orphans.
const (
	offlineAttempts = 3
	offlinePause    = 2 * time.Second
)

// opSession is one human operator that this gateway created (decision
// 0012, point 4B).
//
// # Resume token
//
// The resume token is "rt_" and 32 random bytes in base64url, from
// crypto/rand, so it cannot be guessed. The gateway keeps only its
// SHA-256, so a memory dump holds no usable token. A token resumes the operator
// only for the same identity it was given to: the same level and the same
// site user ID, or anonymous for anonymous. A token from another visitor
// or for another identity is ignored, and the hello gets a new operator.
// Every successful hello gets a new token and the old one stops working,
// so a token seen once (for example in an old tab) cannot take the
// operator later. The session and its token end when the operator goes
// offline after the grace time, or when the gateway stops.
type opSession struct {
	id    string
	ident Identity

	// callMu serialises the router calls of this operator, so a late
	// "online" can never follow the "offline" of the grace time.
	callMu sync.Mutex

	// Guarded by registry.mu.
	tokenHash [32]byte
	owner     *client
	grace     Timer
	ended     bool
	row       gwapi.OperatorRow
}

// registry holds the operator sessions of this gateway.
type registry struct {
	api    OperatorAPI
	clock  Clock
	grace  time.Duration
	seats  int
	logger *slog.Logger
	m      *metrics

	mu      sync.Mutex
	byID    map[string]*opSession
	byToken map[[32]byte]*opSession
	stopped bool

	// wg owns the goroutines that put operators offline.
	wg sync.WaitGroup
}

func newRegistry(api OperatorAPI, clock Clock, grace time.Duration, seats int, logger *slog.Logger, m *metrics) *registry {
	return &registry{
		api: api, clock: clock, grace: grace, seats: seats, logger: logger, m: m,
		byID: map[string]*opSession{}, byToken: map[[32]byte]*opSession{},
	}
}

func newResumeToken() (string, [32]byte) {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	tok := "rt_" + base64.RawURLEncoding.EncodeToString(b)
	return tok, sha256.Sum256([]byte(tok))
}

var idEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// newOperatorID returns "web-" and 16 random base32 characters. It is not
// made from the user ID, because operator IDs are shown at every level.
func newOperatorID() string {
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return gwapi.HumanOperatorPrefix + strings.ToLower(idEncoding.EncodeToString(b))
}

// attachResult is what Attach gives a connection.
type attachResult struct {
	sess     *opSession
	token    string
	replaced *client
	resumed  bool
}

// Attach gives owner an operator: the one of resume when the token is
// valid for ident, else a new one. A new operator is created through the
// router API with capacity 2 and priority 1, and is offline.
func (r *registry) Attach(ctx context.Context, ident Identity, langs []string, resume string, owner *client) (attachResult, error) {
	langs = slices.Sorted(slices.Values(langs))
	if s := r.resume(ident, resume, owner); s.sess != nil {
		s.sess.callMu.Lock()
		defer s.sess.callMu.Unlock()
		r.mu.Lock()
		same := slices.Equal(s.sess.row.Languages, langs)
		r.mu.Unlock()
		if !same {
			if resp, err := r.api.PutOperator(ctx, s.sess.id, humanOperator(langs)); err == nil {
				r.setRow(s.sess, resp.Operator)
			} else {
				r.logger.Warn("languages of a resumed operator not changed", "operator_id", s.sess.id, "error", err)
			}
		}
		return s, nil
	}

	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return attachResult{}, ErrEnded
	}
	if ident.Level != gwapi.LevelFull && r.demoCountLocked() >= r.seats {
		r.mu.Unlock()
		return attachResult{}, ErrNoSeat
	}
	tok, h := newResumeToken()
	s := &opSession{
		id: newOperatorID(), ident: ident, tokenHash: h, owner: owner,
		row: gwapi.OperatorRow{Languages: langs, Capacity: gwapi.HumanCapacity, Presence: gwapi.PresenceOffline},
	}
	s.row.ID, s.row.Kind = s.id, gwapi.KindHuman
	// The seat is taken before the call, so two hellos cannot both take
	// the last seat.
	r.byID[s.id] = s
	r.byToken[h] = s
	r.mu.Unlock()

	s.callMu.Lock()
	resp, err := r.api.PutOperator(ctx, s.id, humanOperator(langs))
	s.callMu.Unlock()
	if err != nil {
		r.mu.Lock()
		r.removeLocked(s)
		r.mu.Unlock()
		return attachResult{}, fmt.Errorf("create operator: %w", err)
	}
	r.setRow(s, resp.Operator)
	return attachResult{sess: s, token: tok}, nil
}

// humanOperator is the body of every PutOperator call for a human
// operator: capacity gwapi.HumanCapacity and priority gwapi.HumanPriority.
// A PUT replaces every field, so each call must send both.
func humanOperator(langs []string) opapi.OperatorRequest {
	return opapi.OperatorRequest{Languages: langs, Capacity: gwapi.HumanCapacity, Priority: gwapi.HumanPriority}
}

func (r *registry) setRow(s *opSession, op opapi.Operator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(op.Languages) > 0 {
		s.row.Languages = slices.Sorted(slices.Values(op.Languages))
	}
	if op.Capacity > 0 {
		s.row.Capacity = op.Capacity
	}
	if op.Presence.Valid() {
		s.row.Presence = gwapi.Presence(op.Presence)
	}
}

// resume returns the session of a valid token, attached to owner.
func (r *registry) resume(ident Identity, token string, owner *client) attachResult {
	if token == "" {
		return attachResult{}
	}
	h := sha256.Sum256([]byte(token))
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.byToken[h]
	if s == nil || s.ended || r.stopped ||
		s.ident.UserID != ident.UserID || s.ident.Level != ident.Level {
		return attachResult{}
	}
	if s.grace != nil {
		s.grace.Stop()
		s.grace = nil
	}
	replaced := s.owner
	s.owner = owner
	delete(r.byToken, h)
	tok, nh := newResumeToken()
	s.tokenHash = nh
	r.byToken[nh] = s
	return attachResult{sess: s, token: tok, replaced: replaced, resumed: true}
}

func (r *registry) demoCountLocked() int {
	n := 0
	for _, s := range r.byID {
		if s.ident.Level != gwapi.LevelFull {
			n++
		}
	}
	return n
}

func (r *registry) removeLocked(s *opSession) {
	s.ended = true
	if r.byID[s.id] == s {
		delete(r.byID, s.id)
	}
	if r.byToken[s.tokenHash] == s {
		delete(r.byToken, s.tokenHash)
	}
}

// Owns reports whether owner still holds the operator.
func (r *registry) Owns(s *opSession, owner *client) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !s.ended && s.owner == owner
}

// Row returns the operator row from the last router answer.
func (r *registry) Row(s *opSession) gwapi.OperatorRow {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := s.row
	row.Languages = slices.Clone(row.Languages)
	return row
}

// Has reports whether this gateway holds the operator.
func (r *registry) Has(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byID[id] != nil
}

// Detach starts the grace time when owner still holds the operator. After
// it the operator goes offline (decision 0012, point 4B).
func (r *registry) Detach(s *opSession, owner *client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s.ended || s.owner != owner {
		return
	}
	s.owner = nil
	s.grace = r.clock.AfterFunc(r.grace, func() { r.expire(s) })
}

func (r *registry) expire(s *opSession) {
	r.mu.Lock()
	if s.ended || s.owner != nil || r.stopped {
		r.mu.Unlock()
		return
	}
	r.removeLocked(s)
	r.wg.Add(1)
	r.mu.Unlock()
	go func() {
		defer r.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), offlineAttempts*(offlinePause+5*time.Second))
		defer cancel()
		r.offline(ctx, s, "grace")
	}()
}

// offline puts the operator offline, with a few attempts.
func (r *registry) offline(ctx context.Context, s *opSession, why string) bool {
	s.callMu.Lock()
	defer s.callMu.Unlock()
	return r.offlineID(ctx, s.id, why)
}

func (r *registry) offlineID(ctx context.Context, id, why string) bool {
	for attempt := 1; ; attempt++ {
		_, err := r.api.Offline(ctx, id)
		if err == nil {
			r.m.offlinePuts.WithLabelValues(why, "done").Inc()
			return true
		}
		if attempt >= offlineAttempts || ctx.Err() != nil {
			r.m.offlinePuts.WithLabelValues(why, "failed").Inc()
			r.logger.Warn("operator not put offline", "operator_id", id, "reason", why, "error", err)
			return false
		}
		select {
		case <-ctx.Done():
		case <-time.After(offlinePause):
		}
	}
}

// Call runs fn with the router calls of the operator serialised. It
// fails with ErrEnded when the session ended.
func (r *registry) Call(s *opSession, fn func() error) error {
	s.callMu.Lock()
	defer s.callMu.Unlock()
	r.mu.Lock()
	ended := s.ended
	r.mu.Unlock()
	if ended {
		return ErrEnded
	}
	return fn()
}

// SweepOrphans puts offline every online human operator of the view that
// no session of this gateway holds: operators of an earlier run of the
// gateway, and operators whose offline call after the grace time failed.
// It assumes one gateway process (decision 0011, "One gateway process
// now").
func (r *registry) SweepOrphans(ctx context.Context, online []string) {
	for _, id := range online {
		if r.Has(id) || ctx.Err() != nil {
			continue
		}
		r.offlineID(ctx, id, "orphan")
	}
}

// Shutdown ends every session and puts its operator offline, within ctx.
func (r *registry) Shutdown(ctx context.Context) {
	r.mu.Lock()
	r.stopped = true
	var all []*opSession
	for _, s := range r.byID {
		if s.grace != nil {
			s.grace.Stop()
		}
		all = append(all, s)
		r.removeLocked(s)
	}
	r.mu.Unlock()
	var wg sync.WaitGroup
	for _, s := range all {
		wg.Go(func() {
			s.callMu.Lock()
			defer s.callMu.Unlock()
			r.offlineID(ctx, s.id, "stop")
		})
	}
	wg.Wait()
	r.wg.Wait()
}
