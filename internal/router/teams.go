package router

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// langLoad is the active load of a company in one language.
type langLoad struct {
	Lang string
	N    int64
}

// opInfo is an operator as the team planner sees it.
type opInfo struct {
	ID    string
	Langs []string
	Live  int32
}

func (o opInfo) has(lang string) bool { return slices.Contains(o.Langs, lang) }

// langNeeds returns how many more members each language needs: the load of
// the language divided by the capacity, rounded up, less the members who
// already have it.
func langNeeds(loads []langLoad, capacity int, members []opInfo) map[string]int {
	need := make(map[string]int, len(loads))
	for _, l := range loads {
		n := int((l.N + int64(capacity) - 1) / int64(capacity))
		for _, m := range members {
			if m.has(l.Lang) {
				n--
			}
		}
		need[l.Lang] = max(n, 0)
	}
	return need
}

// pickJoin chooses up to k candidates to join a team (decision 0009,
// "Languages in a team"). loads are the languages of the company's active
// conversations, most needed first; candidates are ordered by load, then
// ID. It takes one operator per language in rounds, most needed language
// first, while a language still needs members, and then fills up with the
// remaining candidates in their order. Every candidate has at least one of
// the languages.
func pickJoin(loads []langLoad, capacity, k int, members, candidates []opInfo) []string {
	need := langNeeds(loads, capacity, members)
	used := make(map[string]bool, k)
	var out []string
	take := func(c opInfo) {
		out = append(out, c.ID)
		used[c.ID] = true
		for _, l := range c.Langs {
			if need[l] > 0 {
				need[l]--
			}
		}
	}
	for progress := true; progress && len(out) < k; {
		progress = false
		for _, l := range loads {
			if len(out) == k {
				break
			}
			if need[l.Lang] == 0 {
				continue
			}
			i := slices.IndexFunc(candidates, func(c opInfo) bool { return !used[c.ID] && c.has(l.Lang) })
			if i < 0 {
				need[l.Lang] = 0
				continue
			}
			take(candidates[i])
			progress = true
		}
	}
	for _, c := range candidates {
		if len(out) == k {
			break
		}
		if !used[c.ID] {
			take(c)
		}
	}
	return out
}

// pickLeave chooses k members to leave a team. A member who is the only
// one with a language of the company's active conversations leaves last.
// Among the others, the member with the lowest load leaves first, then by
// ID.
func pickLeave(loads []langLoad, k int, members []opInfo) []string {
	rest := slices.Clone(members)
	var out []string
	for len(out) < k && len(rest) > 0 {
		sole := func(m opInfo) int {
			n := 0
			for _, l := range loads {
				if !m.has(l.Lang) {
					continue
				}
				others := 0
				for _, o := range rest {
					if o.ID != m.ID && o.has(l.Lang) {
						others++
					}
				}
				if others == 0 {
					n++
				}
			}
			return n
		}
		best := slices.MinFunc(rest, func(a, b opInfo) int {
			if d := sole(a) - sole(b); d != 0 {
				return d
			}
			if a.Live != b.Live {
				return int(a.Live - b.Live)
			}
			return compareStrings(a.ID, b.ID)
		})
		out = append(out, best.ID)
		rest = slices.DeleteFunc(rest, func(m opInfo) bool { return m.ID == best.ID })
	}
	return out
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// TeamResult says what CheckTeams did.
type TeamResult struct {
	Formed, Resized, Released int
	// Moved is the number of waiting conversations that moved between the
	// team queue and the shared queue.
	Moved int
	// CapForm counts companies that reached the form threshold but got no
	// team, because the team cap left less room than the minimum team
	// size. CapGrow counts teams, new or existing, that got fewer operators
	// than their size because of the cap. Each is counted once per company
	// and check.
	CapForm, CapGrow int
}

// CheckTeams forms, resizes and releases dedicated teams by the rules of
// decision 0009, and moves waiting conversations to the queue the team
// rules give. Call it at a fixed interval. Each company is one transaction,
// and every team transaction of every router holds one advisory lock, so
// team changes happen one at a time. Companies are checked by active load,
// highest first, so the busiest company gets the room the team cap leaves.
//
// The team cap (decision 0009, "A cap on the operators that teams hold") is
// read inside each company transaction, after the advisory lock, from the
// operators table: the online operators and the online team members. Every
// join to a team happens under that lock, and a statement under READ
// COMMITTED sees every transaction that committed before it started, so
// the count includes every join of an earlier transaction, in this process
// or another. No join can take the team members above the cap computed in
// the same transaction.
//
// Offers and assignments are never touched: an operator who joins or
// leaves a team keeps what it holds, and an offer that is out runs to its
// end. Only conversations that wait without an offer move.
func (s *Service) CheckTeams(ctx context.Context) (TeamResult, error) {
	var res TeamResult
	companies, err := pgdb.New(s.db).TeamCompanies(ctx, int64(s.cfg.TeamFormLoad))
	if err != nil {
		return res, fmt.Errorf("check teams: %w", err)
	}
	for _, row := range companies {
		company := row.CompanyID
		var r TeamResult
		err := s.inTx(ctx, func(q *pgdb.Queries) error {
			r = TeamResult{}
			return s.checkCompany(ctx, q, company, &r)
		})
		if err != nil {
			return res, fmt.Errorf("check team of %s: %w", company, err)
		}
		res.Formed += r.Formed
		res.Resized += r.Resized
		res.Released += r.Released
		res.Moved += r.Moved
		res.CapForm += r.CapForm
		res.CapGrow += r.CapGrow
	}
	return res, nil
}

func (s *Service) checkCompany(ctx context.Context, q *pgdb.Queries, company string, res *TeamResult) error {
	if err := q.TeamLock(ctx, teamLockKey); err != nil {
		return fmt.Errorf("team lock: %w", err)
	}
	if err := q.EnsureCompany(ctx, pgdb.EnsureCompanyParams{CompanyID: company, UpdatedAt: s.now()}); err != nil {
		return fmt.Errorf("ensure company: %w", err)
	}
	co, err := q.LockCompany(ctx, company)
	if err != nil {
		return fmt.Errorf("lock company: %w", err)
	}
	now := s.now(co.LowSince, co.TeamFormedAt)
	loadRows, err := q.CompanyLangLoad(ctx, company)
	if err != nil {
		return fmt.Errorf("company load: %w", err)
	}
	loads := make([]langLoad, len(loadRows))
	var load int64
	for i, r := range loadRows {
		loads[i] = langLoad{Lang: r.Lang, N: r.N}
		load += r.N
	}
	members, err := s.teamMembers(ctx, q, company)
	if err != nil {
		return err
	}

	var ob outbox
	switch {
	case !co.TeamActive && load >= int64(s.cfg.TeamFormLoad):
		if err := s.formTeam(ctx, q, &co, &ob, loads, load, now, res); err != nil {
			return err
		}
	case co.TeamActive && load < int64(s.cfg.TeamReleaseLoad) && co.LowSince != nil &&
		s.cfg.model(now.Sub(*co.LowSince)) >= s.cfg.TeamReleaseAfter:
		if err := s.releaseTeam(ctx, q, &co, &ob, members, load, now, res); err != nil {
			return err
		}
	case co.TeamActive:
		if load < int64(s.cfg.TeamReleaseLoad) {
			if co.LowSince == nil {
				co.LowSince = ptr(now)
			}
		} else {
			co.LowSince = nil
		}
		if err := s.resizeTeam(ctx, q, &co, &ob, loads, load, members, now, res); err != nil {
			return err
		}
	}

	// Move waiting conversations to the queue the team rules give now.
	moved, err := q.QueueMismatches(ctx, pgdb.QueueMismatchesParams{CompanyID: company, TeamActive: co.TeamActive})
	if err != nil {
		return fmt.Errorf("queue mismatches: %w", err)
	}
	for i := range moved {
		c := &moved[i]
		cnow := s.now(c.QueuedAt, &now)
		c.TeamQueue = !c.TeamQueue
		reason := event.QueueTeamReleased
		if c.TeamQueue {
			reason = event.QueueTeamFormed
		}
		ev := convEvent(event.TypeQueued, cnow)
		ev.QueuedAt = deref(c.QueuedAt)
		ev.QueueReason = reason
		if err := ob.addConv(c, ev); err != nil {
			return err
		}
		if err := saveConv(ctx, q, c, cnow); err != nil {
			return err
		}
	}
	res.Moved = len(moved)

	co.UpdatedAt = now
	err = q.SaveCompany(ctx, pgdb.SaveCompanyParams{
		TeamSeq: co.TeamSeq, TeamActive: co.TeamActive, TeamFormedAt: co.TeamFormedAt,
		LowSince: co.LowSince, UpdatedAt: co.UpdatedAt, CompanyID: co.CompanyID,
	})
	if err != nil {
		return fmt.Errorf("save company: %w", err)
	}
	return ob.flush(ctx, q, now)
}

func (s *Service) teamMembers(ctx context.Context, q *pgdb.Queries, company string) ([]opInfo, error) {
	rows, err := q.TeamMembers(ctx, company)
	if err != nil {
		return nil, fmt.Errorf("team members: %w", err)
	}
	out := make([]opInfo, len(rows))
	for i, r := range rows {
		out[i] = opInfo{ID: r.ID, Langs: r.Languages, Live: r.Live}
	}
	return out, nil
}

func (s *Service) poolCandidates(ctx context.Context, q *pgdb.Queries, loads []langLoad) ([]opInfo, error) {
	langs := make([]string, len(loads))
	for i, l := range loads {
		langs[i] = l.Lang
	}
	rows, err := q.PoolCandidates(ctx, langs)
	if err != nil {
		return nil, fmt.Errorf("pool candidates: %w", err)
	}
	out := make([]opInfo, len(rows))
	for i, r := range rows {
		out[i] = opInfo{ID: r.ID, Langs: r.Languages, Live: r.Live}
	}
	return out, nil
}

// teamRoom returns how many more online operators the teams may hold under
// the cap, at least 0. The caller holds the team lock. When pool operators
// went offline, or offline members came back online, the teams can hold
// more than the cap; the room is then 0 and nobody is taken out.
func (s *Service) teamRoom(ctx context.Context, q *pgdb.Queries) (int, error) {
	c, err := q.TeamCapCounts(ctx)
	if err != nil {
		return 0, fmt.Errorf("team cap counts: %w", err)
	}
	return int(max(s.cfg.TeamCap(c.Online)-c.InTeam, 0)), nil
}

// join moves the chosen candidates into the team and returns those that
// joined. Each one gets an operator_updated event, because its dedicated
// company changed.
func (s *Service) join(ctx context.Context, q *pgdb.Queries, ob *outbox, company string, loads []langLoad, k int,
	members []opInfo, now time.Time,
) ([]string, error) {
	cands, err := s.poolCandidates(ctx, q, loads)
	if err != nil {
		return nil, err
	}
	ids := pickJoin(loads, s.cfg.Capacity, k, members, cands)
	if len(ids) == 0 {
		return nil, nil
	}
	joined, err := q.JoinTeam(ctx, pgdb.JoinTeamParams{CompanyID: company, UpdatedAt: now, Ids: ids})
	if err != nil {
		return nil, fmt.Errorf("join team: %w", err)
	}
	if err := addOperatorEvents(ctx, q, ob, joined, event.TypeOperatorUpdated, now); err != nil {
		return nil, err
	}
	return joined, nil
}

// leave moves members of the team of a company back to the shared pool
// and returns those that left. Each one gets an operator_updated event,
// because its dedicated company changed.
func leave(ctx context.Context, q *pgdb.Queries, ob *outbox, company string, ids []string, now time.Time) ([]string, error) {
	left, err := q.LeaveTeam(ctx, pgdb.LeaveTeamParams{UpdatedAt: now, Ids: ids, CompanyID: company})
	if err != nil {
		return nil, fmt.Errorf("leave team: %w", err)
	}
	if err := addOperatorEvents(ctx, q, ob, left, event.TypeOperatorUpdated, now); err != nil {
		return nil, err
	}
	return left, nil
}

func (s *Service) formTeam(ctx context.Context, q *pgdb.Queries, co *pgdb.Company, ob *outbox,
	loads []langLoad, load int64, now time.Time, res *TeamResult,
) error {
	size := s.cfg.teamSize(load)
	if !s.cfg.capOff() {
		room, err := s.teamRoom(ctx, q)
		if err != nil {
			return err
		}
		if room < s.cfg.TeamMinSize {
			// The company stays in the shared queues. The next check tries
			// again.
			res.CapForm++
			s.logger.DebugContext(ctx, "team cap stops a new team", "company", co.CompanyID, "room", room, "active_load", load)
			return nil
		}
		if room < size {
			res.CapGrow++
			size = room
		}
	}
	joined, err := s.join(ctx, q, ob, co.CompanyID, loads, size, nil, now)
	if err != nil || len(joined) == 0 {
		// No online operator of the shared pool has a language of the
		// company. The next check tries again.
		return err
	}
	slices.Sort(joined)
	co.TeamActive = true
	co.TeamFormedAt = ptr(now)
	co.LowSince = nil
	res.Formed++
	s.logger.InfoContext(ctx, "team formed", "company", co.CompanyID, "members", len(joined), "active_load", load)
	return ob.addTeam(co, event.Routing{Type: event.TypeTeamFormed, At: now, Members: joined, ActiveLoad: uint64(load)})
}

func (s *Service) resizeTeam(ctx context.Context, q *pgdb.Queries, co *pgdb.Company, ob *outbox,
	loads []langLoad, load int64, members []opInfo, now time.Time, res *TeamResult,
) error {
	target := s.cfg.teamSize(load)
	changed := false
	switch {
	case target > len(members):
		k := target - len(members)
		if !s.cfg.capOff() {
			room, err := s.teamRoom(ctx, q)
			if err != nil {
				return err
			}
			if room < k {
				res.CapGrow++
				k = room
			}
		}
		if k == 0 {
			break
		}
		joined, err := s.join(ctx, q, ob, co.CompanyID, loads, k, members, now)
		if err != nil {
			return err
		}
		changed = len(joined) > 0
	case target < len(members):
		ids := pickLeave(loads, len(members)-target, members)
		left, err := leave(ctx, q, ob, co.CompanyID, ids, now)
		if err != nil {
			return err
		}
		changed = len(left) > 0
	}
	if !changed {
		return nil
	}
	after, err := s.teamMembers(ctx, q, co.CompanyID)
	if err != nil {
		return err
	}
	ids := make([]string, len(after))
	for i, m := range after {
		ids[i] = m.ID
	}
	res.Resized++
	return ob.addTeam(co, event.Routing{Type: event.TypeTeamResized, At: now, Members: ids, ActiveLoad: uint64(load)})
}

func (s *Service) releaseTeam(ctx context.Context, q *pgdb.Queries, co *pgdb.Company, ob *outbox,
	members []opInfo, load int64, now time.Time, res *TeamResult,
) error {
	ids := make([]string, len(members))
	for i, m := range members {
		ids[i] = m.ID
	}
	left, err := leave(ctx, q, ob, co.CompanyID, ids, now)
	if err != nil {
		return fmt.Errorf("release team: %w", err)
	}
	co.TeamActive = false
	co.LowSince = nil
	res.Released++
	s.logger.InfoContext(ctx, "team released", "company", co.CompanyID, "members", len(left), "active_load", load)
	if len(left) == 0 {
		// A team event needs members. A team without members has nothing
		// to tell the readers.
		return nil
	}
	slices.Sort(left)
	return ob.addTeam(co, event.Routing{
		Type: event.TypeTeamReleased, At: now, Members: left, ActiveLoad: uint64(load),
		ReleaseReason: event.TeamLowLoad,
	})
}
