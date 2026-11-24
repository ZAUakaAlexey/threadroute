//go:build integration

package router

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// Size of the concurrency test. Many conversations and few operators, so
// workers fight over the same operator rows and the same slots.
const (
	ccWorkers       = 12
	ccOpsPerWorker  = 400
	ccCompanies     = 3
	ccCustomers     = 40 // per company
	ccOperators     = 8
	ccDefaultSeed   = 20261124
	ccEnvSeed       = "THREADROUTE_TEST_SEED"
	ccCheckInterval = 10 * time.Millisecond
)

// ccKind is one kind of operation in the mix.
type ccKind int

const (
	kAssign ccKind = iota
	kAccept
	kRefuse
	kReply
	kResolve
	kExpire
	kTick
	kOffline
	kOnline
	kCapacity
	kConv
	kSLA
	kTeams
	kRefresh
	ccKinds
)

var ccNames = [ccKinds]string{"assign", "accept", "refuse", "reply", "resolve", "expire", "tick",
	"offline", "online", "capacity", "conv_event", "sla", "teams", "refresh"}

// ccWeights sets how often each kind runs.
var ccWeights = [ccKinds]int{22, 12, 4, 5, 8, 6, 9, 3, 5, 3, 20, 3, 2, 3}

// ccStats counts outcomes. Errors of the database that are not a deadlock
// or a serialization failure are kept as text and fail the test.
type ccStats struct {
	mu            sync.Mutex
	runs          [ccKinds]int
	done          [ccKinds]int
	already       [ccKinds]int
	rejected      map[opapi.ErrorCode]int
	deadlocks     int
	serialization int
	unexpected    []string
	offers        int
	expired       int
	missed        int
	formed        int
	resized       int
	released      int
	refreshed     int
	overCapacity  int
}

func (s *ccStats) record(k ccKind, res opapi.Result, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[k]++
	if err == nil {
		switch res {
		case opapi.ResultDone:
			s.done[k]++
		case opapi.ResultAlreadyDone:
			s.already[k]++
		}
		return
	}
	if ae, ok := errors.AsType[*ActionError](err); ok &&
		(ae.Code.Rejected() || ae.Code == opapi.CodeOfferNotFound || ae.Code == opapi.CodeOperatorNotFound) {
		s.rejected[ae.Code]++
		return
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "40P01":
			s.deadlocks++
			return
		case "40001":
			s.serialization++
			return
		}
	}
	if len(s.unexpected) < 20 {
		s.unexpected = append(s.unexpected, fmt.Sprintf("%s: %v", ccNames[k], err))
	} else {
		s.unexpected = append(s.unexpected[:20], "more errors")
	}
}

// ccRuns counts the runs in this process, so -count=N runs N different
// mixes.
var ccRuns atomic.Uint64

func ccSeed(t *testing.T) uint64 {
	run := ccRuns.Add(1) - 1
	if v := os.Getenv(ccEnvSeed); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("%s=%q: %v", ccEnvSeed, v, err)
		}
		return n + run
	}
	return ccDefaultSeed + run
}

// TestIntegrationConcurrency runs a seeded random mix of every routing
// operation from many workers at once on a real PostgreSQL, checks the
// invariants with SQL while the mix runs and after it, and fails on any
// deadlock, serialization failure or unexpected error.
//
// What runs at once: Assign from every worker, accept, refuse, reply,
// resolve (also late and repeated, on old offers), the accept timeout, the
// SLA check, the team check, operators going offline and online, capacity
// changes that cut below the current load, the refresh of operator
// snapshots, and conversation events (open, customer and company messages,
// "message removed", close, reopen with a preferred operator, and the
// numbering reset of conv). The events of one conversation come from one
// worker, in order, as conv delivers them from one partition.
//
// The invariants cover the three outbox families: sequence numbers per key
// from 1 with no gap and no repeat, op_seq equal to the number of operator
// events, the newest operator event equal to the operator row, and the
// previous pair on every queued event that needs one.
func TestIntegrationConcurrency(t *testing.T) {
	seed := ccSeed(t)
	t.Logf("seed %d (set %s to this value and run once to repeat the mix)", seed, ccEnvSeed)
	pool := startDB(t, ccWorkers+4)
	resetDB(t, pool)

	cfg := DefaultConfig()
	cfg.Capacity = 3
	cfg.TeamFormLoad = 16
	cfg.TeamReleaseLoad = 14
	cfg.TeamReleaseAfter = 10 * time.Second
	cfg.TeamMaxSize = 4
	clock := newFakeClock()
	svc, err := New(pool, cfg, clock, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()

	// Operators: few, with overlapping languages. Nobody has "pt", so that
	// queue never moves and must not block the others.
	opLangs := [][]string{
		{"en", "und"}, {"en", "ja"}, {"ja", "und"}, {"en"},
		{"ja"}, {"en", "und"}, {"en", "ja", "und"}, {"ja", "und"},
	}
	opIDs := make([]string, ccOperators)
	for i := range ccOperators {
		opIDs[i] = operator(i)
		if _, err := svc.PutOperator(ctx, opIDs[i], opapi.OperatorRequest{Languages: opLangs[i]}); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Online(ctx, opIDs[i]); err != nil {
			t.Fatal(err)
		}
	}
	langs := []string{"en", "en", "ja", "und", "pt"}
	var streams []*convStream
	for c := range ccCompanies {
		for u := range ccCustomers {
			streams = append(streams, newStream(company(c), customer(c*ccCustomers+u), langs[(c+u)%len(langs)]))
		}
	}

	stats := &ccStats{rejected: map[opapi.ErrorCode]int{}}
	var inflight, maxInflight atomic.Int64

	// The checker runs every invariant while the workers run. A slot that
	// overfills an operator for a moment is gone once the slot is freed,
	// so a check only at the end can miss it.
	var violations []string
	var checks int
	stopCheck := make(chan struct{})
	checkDone := make(chan struct{})
	go func() {
		defer close(checkDone)
		tick := time.NewTicker(ccCheckInterval)
		defer tick.Stop()
		for {
			select {
			case <-stopCheck:
				return
			case <-tick.C:
			}
			bad, err := checkInvariants(ctx, pool)
			checks++
			if err != nil {
				violations = append(violations, "check failed: "+err.Error())
				return
			}
			if len(violations) < 50 {
				violations = append(violations, bad...)
			}
			var over int
			err = pool.QueryRow(ctx, `SELECT count(*) FROM operators AS o
				WHERE (SELECT count(*) FROM offers AS f WHERE f.operator_id = o.id
				       AND f.state IN ('offered', 'accepted')) > o.capacity`).Scan(&over)
			if err == nil && over > 0 {
				stats.mu.Lock()
				stats.overCapacity++
				stats.mu.Unlock()
			}
		}
	}()

	// Open every connection before the start, so the workers do not wait
	// for connection setup and really overlap.
	warm := make([]*pgxpool.Conn, 0, ccWorkers)
	for range ccWorkers {
		c, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		warm = append(warm, c)
	}
	for _, c := range warm {
		c.Release()
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := range ccWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(w)))
			var own []*convStream
			for i, s := range streams {
				if i%ccWorkers == w {
					own = append(own, s)
				}
			}
			<-start
			for range ccOpsPerWorker {
				k := ccPick(rng)
				n := inflight.Add(1)
				for {
					m := maxInflight.Load()
					if n <= m || maxInflight.CompareAndSwap(m, n) {
						break
					}
				}
				res, err := ccRun(ctx, svc, pool, clock, rng, k, opIDs, opLangs, own, stats)
				inflight.Add(-1)
				stats.record(k, res, err)
			}
		}()
	}
	began := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(began)
	close(stopCheck)
	<-checkDone

	t.Logf("%d workers, %d operations in %s, max %d operations in flight, %d invariant checks during the run",
		ccWorkers, ccWorkers*ccOpsPerWorker, elapsed.Round(time.Millisecond), maxInflight.Load(), checks)
	for k := range ccKinds {
		t.Logf("  %-10s runs %4d done %4d already %4d", ccNames[k], stats.runs[k], stats.done[k], stats.already[k])
	}
	t.Logf("  offers made %d, offers expired %d, SLA misses %d, teams formed %d resized %d released %d",
		stats.offers, stats.expired, stats.missed, stats.formed, stats.resized, stats.released)
	var opEvents, opKeys int
	if err := pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT record_key) FROM outbox WHERE family = 'operator'`).
		Scan(&opEvents, &opKeys); err != nil {
		t.Fatal(err)
	}
	t.Logf("  operator events %d for %d operators, %d of them from the refresh", opEvents, opKeys, stats.refreshed)
	t.Logf("  rejections %v", stats.rejected)
	t.Logf("  checks that saw an operator above its current capacity after a cut: %d", stats.overCapacity)
	t.Logf("  deadlocks %d, serialization failures %d, retries 0 (none are made)", stats.deadlocks, stats.serialization)

	for _, v := range violations {
		t.Errorf("during the run: %s", v)
	}
	for _, u := range stats.unexpected {
		t.Errorf("unexpected error: %s", u)
	}
	if stats.deadlocks > 0 || stats.serialization > 0 {
		t.Errorf("%d deadlocks and %d serialization failures; the lock order should prevent both",
			stats.deadlocks, stats.serialization)
	}
	requireInvariants(t, pool)

	// The mix must have exercised the paths it is meant to test. The floors
	// are about half of the smallest values seen in 10 local runs, so a slower
	// machine that interleaves differently does not fail here.
	if stats.offers < 90 || stats.done[kAccept] < 50 || stats.expired < 4 || stats.done[kOffline] == 0 ||
		stats.done[kRefuse] == 0 || stats.done[kResolve] == 0 || stats.formed == 0 || stats.refreshed == 0 {
		t.Errorf("the mix did too little: offers %d accepts %d expired %d offline %d refuse %d resolve %d teams %d refreshed %d",
			stats.offers, stats.done[kAccept], stats.expired, stats.done[kOffline], stats.done[kRefuse], stats.done[kResolve],
			stats.formed, stats.refreshed)
	}
}

func ccPick(rng *rand.Rand) ccKind {
	total := 0
	for _, w := range ccWeights {
		total += w
	}
	n := rng.IntN(total)
	for k, w := range ccWeights {
		if n < w {
			return ccKind(k)
		}
		n -= w
	}
	return kAssign
}

// ccRun runs one operation. Operations that are not operator actions
// return ResultDone when they changed something.
func ccRun(ctx context.Context, svc *Service, pool *pgxpool.Pool, clock *fakeClock, rng *rand.Rand, k ccKind,
	opIDs []string, opLangs [][]string, own []*convStream, stats *ccStats,
) (opapi.Result, error) {
	done := func(n int, err error) (opapi.Result, error) {
		if err != nil || n == 0 {
			return opapi.ResultAlreadyDone, err
		}
		return opapi.ResultDone, nil
	}
	switch k {
	case kAssign:
		r, err := svc.Assign(ctx, 10)
		stats.mu.Lock()
		stats.offers += r.Offered
		stats.mu.Unlock()
		return done(r.Offered, err)
	case kAccept, kRefuse, kReply, kResolve:
		offerID, opID, err := ccRandomOffer(ctx, pool, rng, k)
		if err != nil || offerID == "" {
			return "", err
		}
		switch k {
		case kAccept:
			return svc.Accept(ctx, opID, offerID)
		case kRefuse:
			return svc.Refuse(ctx, opID, offerID)
		case kReply:
			return svc.Reply(ctx, opID, offerID, fmt.Sprintf("r%d", rng.IntN(3)))
		default:
			return svc.Resolve(ctx, opID, offerID)
		}
	case kExpire:
		n, err := svc.ExpireOffers(ctx, 20)
		stats.mu.Lock()
		stats.expired += n
		stats.mu.Unlock()
		return done(n, err)
	case kTick:
		clock.Advance(time.Duration(rng.IntN(4000)) * time.Millisecond)
		return opapi.ResultDone, nil
	case kOffline:
		return svc.Offline(ctx, opIDs[rng.IntN(len(opIDs))])
	case kOnline:
		return svc.Online(ctx, opIDs[rng.IntN(len(opIDs))])
	case kCapacity:
		i := rng.IntN(len(opIDs))
		r, err := svc.PutOperator(ctx, opIDs[i], opapi.OperatorRequest{Languages: opLangs[i], Capacity: 1 + rng.IntN(4)})
		return r.Result, err
	case kConv:
		return ccConvEvent(ctx, svc, rng, own)
	case kSLA:
		n, err := svc.CheckSLA(ctx, 50)
		stats.mu.Lock()
		stats.missed += n
		stats.mu.Unlock()
		return done(n, err)
	case kTeams:
		r, err := svc.CheckTeams(ctx)
		stats.mu.Lock()
		stats.formed += r.Formed
		stats.resized += r.Resized
		stats.released += r.Released
		stats.mu.Unlock()
		return done(r.Formed+r.Resized+r.Released+r.Moved, err)
	case kRefresh:
		// Every operator whose last event is older than now is due, so the
		// refresh runs against online, offline, capacity and team changes
		// of the same operators.
		n, err := svc.RefreshOperators(ctx, clock.Now(), 1+rng.IntN(len(opIDs)))
		stats.mu.Lock()
		stats.refreshed += n
		stats.mu.Unlock()
		return done(n, err)
	}
	return "", fmt.Errorf("unknown kind %d", k)
}

// ccRandomOffer picks one of the newest offers, live or not, so late and
// repeated calls are part of the mix. Accepts prefer offers that wait for
// an answer, replies and resolves prefer assignments.
func ccRandomOffer(ctx context.Context, pool *pgxpool.Pool, rng *rand.Rand, k ccKind) (string, string, error) {
	want := "offered"
	if k == kReply || k == kResolve {
		want = "accepted"
	}
	q := `SELECT id, operator_id FROM offers ORDER BY num DESC LIMIT 30`
	if rng.IntN(4) != 0 {
		q = `SELECT id, operator_id FROM offers WHERE state = '` + want + `' ORDER BY num DESC LIMIT 30`
	}
	rows, err := pool.Query(ctx, q)
	if err != nil {
		return "", "", err
	}
	type pair struct{ offer, op string }
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (pair, error) {
		var p pair
		return p, r.Scan(&p.offer, &p.op)
	})
	if err != nil || len(list) == 0 {
		return "", "", err
	}
	p := list[rng.IntN(len(list))]
	return p.offer, p.op, nil
}

// ccConvEvent applies the next event of one of the worker's own
// conversations. A closed conversation opens again (a reopen, which
// prefers the previous operator), and now and then conv "forgets" it and
// starts again at session 1, sequence 1.
func ccConvEvent(ctx context.Context, svc *Service, rng *rand.Rand, own []*convStream) (opapi.Result, error) {
	s := own[rng.IntN(len(own))]
	var ev event.Conversation
	switch {
	case !s.open:
		if s.session > 0 && rng.IntN(10) == 0 {
			s.forget()
		}
		ev = s.opened()
	default:
		switch r := rng.IntN(100); {
		case r < 40:
			ev = s.message(event.FromCustomer)
		case r < 50:
			ev = s.message(event.FromCompany)
		case r < 60:
			ev = s.answered(time.Duration(rng.IntN(90)) * time.Second)
		case r < 66:
			ev = s.removed()
		default:
			ev = s.closed()
		}
	}
	r, err := svc.ApplyConversation(ctx, ev, false)
	if err != nil {
		return "", err
	}
	if r.Skipped {
		return opapi.ResultAlreadyDone, nil
	}
	return opapi.ResultDone, nil
}
