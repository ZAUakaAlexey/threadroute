package router

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// # The trim after a lowered share (decision 0009, "A trim after the cap
// was lowered")
//
// A change of the stand settings that lowers teams.max_share stores a trim
// request on the settings row (PutSettings). The next team check runs the
// trim before the normal check of the companies: one transaction that
// takes the team lock, then locks the settings row, reads the request
// again on the locked row, trims the teams to the cap of the stored set and
// clears the request. A second router that waited for the team lock finds
// no request and does nothing.
//
// The trim takes online members out of teams and releases teams. It never
// adds a member and never touches a conversation: the members that leave
// keep their offers and assignments, and the waiting conversations of a
// released team move to the shared queues in the normal check that
// follows in the same CheckTeams call.

// trimTeam is a team as the trim plan sees it.
type trimTeam struct {
	Company string
	// Load is the active load of the company, and Loads the same load by
	// language, most needed first, for pickLeave.
	Load  int64
	Loads []langLoad
	// Online are the online members. Offline are the IDs of the offline
	// members: they never leave a team that stays.
	Online  []opInfo
	Offline []string
}

// trimAction is what the trim does to one team.
type trimAction int

const (
	// trimKeep keeps the team as it is.
	trimKeep trimAction = iota
	// trimShrink takes the online members in Leave out of the team.
	trimShrink
	// trimRelease releases the team, with every member, offline ones too.
	trimRelease
)

func (a trimAction) String() string {
	switch a {
	case trimKeep:
		return "keep"
	case trimShrink:
		return "shrink"
	case trimRelease:
		return "release"
	}
	return fmt.Sprintf("trimAction(%d)", int(a))
}

// trimStep is the plan for one team.
type trimStep struct {
	Company string
	Action  trimAction
	// Leave lists the online members that leave a team that gets smaller.
	Leave []string
}

// planTrim plans the trim of the teams to teamCap online members
// (decision 0009, "A trim after the cap was lowered"). It goes through the
// teams by active load, highest first, then by company ID. A team keeps
// all its online members while the room under the cap allows. The first
// team that does not fit keeps as many online members as the room allows
// when that is at least minSize, and the others leave by the rule of
// pickLeave among its online members; with less room it is released.
// Every team after that one is released, even one that would fit in what
// is left. When all online members fit, every team is kept. The result
// has one step per team, in that order.
func planTrim(teamCap int64, minSize int, teams []trimTeam) []trimStep {
	order := slices.Clone(teams)
	slices.SortStableFunc(order, func(a, b trimTeam) int {
		if c := cmp.Compare(b.Load, a.Load); c != 0 {
			return c
		}
		return cmp.Compare(a.Company, b.Company)
	})
	var online int64
	for _, t := range order {
		online += int64(len(t.Online))
	}
	steps := make([]trimStep, len(order))
	for i, t := range order {
		steps[i] = trimStep{Company: t.Company, Action: trimKeep}
	}
	room := max(teamCap, 0)
	if online <= room {
		return steps
	}
	missed := false
	for i, t := range order {
		n := int64(len(t.Online))
		switch {
		case missed:
			// An earlier team did not fit.
			steps[i].Action = trimRelease
		case n <= room:
			room -= n
		default:
			missed = true
			if room >= int64(minSize) {
				steps[i].Action = trimShrink
				steps[i].Leave = pickLeave(t.Loads, int(n-room), t.Online)
			} else {
				steps[i].Action = trimRelease
			}
			room = 0
		}
	}
	return steps
}

// trimInfo is what one trim did, for the log line.
type trimInfo struct {
	version         int64
	teamCap         int64
	before, after   int64
	shrunk, release int
}

// trimTeams runs a pending trim. The caller saw a request on the stored
// row without a lock; the transaction reads it again on the locked row.
// After the commit the Service applies the stored set the trim used, so the
// normal check that follows uses the same numbers. A stored row that is not
// valid is logged and skipped: the request stays.
func (s *Service) trimTeams(ctx context.Context) (TeamResult, error) {
	var (
		res  TeamResult
		st   standRow
		info trimInfo
		ran  bool
	)
	err := s.inTx(ctx, func(q *pgdb.Queries) error {
		res, info, ran = TeamResult{}, trimInfo{}, false
		// Lock order: the team lock, then the settings row, then companies,
		// then operators (internal/pg, 00006_team_trim.sql).
		if err := q.TeamLock(ctx, teamLockKey); err != nil {
			return fmt.Errorf("team lock: %w", err)
		}
		cur, err := s.lockStored(ctx, q)
		if err != nil {
			return err
		}
		st = cur
		if cur.trimVersion == nil {
			// Another router ran the trim since the read without a lock.
			return nil
		}
		ran = true
		cfg := s.config().withTeams(cur.current.Teams)
		if info, err = s.trim(ctx, q, cfg, &res); err != nil {
			return err
		}
		info.version = *cur.trimVersion
		if err := q.ClearTeamTrim(ctx, s.now()); err != nil {
			return fmt.Errorf("clear trim request: %w", err)
		}
		return nil
	})
	if errors.Is(err, errBadStored) {
		s.logger.WarnContext(ctx, "team trim skipped: the stored stand settings are not valid", "error", err)
		return TeamResult{}, nil
	}
	if err != nil {
		return TeamResult{}, fmt.Errorf("trim teams: %w", err)
	}
	s.apply(ctx, st)
	if ran {
		s.logger.InfoContext(ctx, "team trim", "requested_by_version", info.version, "team_cap", info.teamCap,
			"online_in_teams_before", info.before, "online_in_teams_after", info.after,
			"operators_out", res.TrimOperators, "teams_shrunk", info.shrunk, "teams_released", info.release)
	}
	return res, nil
}

// trim plans and applies the trim with the team numbers of cfg. The caller
// holds the team lock and the settings row.
func (s *Service) trim(ctx context.Context, q *pgdb.Queries, cfg Config, res *TeamResult) (trimInfo, error) {
	counts, err := q.TeamCapCounts(ctx)
	if err != nil {
		return trimInfo{}, fmt.Errorf("team cap counts: %w", err)
	}
	info := trimInfo{teamCap: cfg.TeamCap(counts.Online), before: counts.InTeam, after: counts.InTeam}
	if counts.InTeam <= info.teamCap {
		return info, nil
	}
	rows, err := q.ActiveTeamCompanies(ctx)
	if err != nil {
		return info, fmt.Errorf("active team companies: %w", err)
	}
	cos := make(map[string]*pgdb.Company, len(rows))
	teams := make([]trimTeam, 0, len(rows))
	members := make(map[string][]opInfo, len(rows))
	for _, row := range rows {
		t, co, all, err := s.lockTrimTeam(ctx, q, row.CompanyID)
		if err != nil {
			return info, err
		}
		cos[t.Company], members[t.Company] = co, all
		teams = append(teams, t)
	}
	byCompany := make(map[string]trimTeam, len(teams))
	for _, t := range teams {
		byCompany[t.Company] = t
	}
	for _, step := range planTrim(info.teamCap, cfg.TeamMinSize, teams) {
		if step.Action == trimKeep {
			continue
		}
		t, co := byCompany[step.Company], cos[step.Company]
		now := s.now(co.LowSince, co.TeamFormedAt)
		var ob outbox
		if step.Action == trimShrink {
			left, err := leave(ctx, q, &ob, co.CompanyID, step.Leave, now)
			if err != nil {
				return info, err
			}
			res.TrimOperators += len(left)
			info.shrunk++
			if len(left) > 0 {
				if err := s.addResized(ctx, q, co, &ob, t.Load, now, res); err != nil {
					return info, err
				}
			}
		} else {
			if err := s.releaseTeam(ctx, q, co, &ob, members[t.Company], t.Load, now, event.TeamCapLowered, res); err != nil {
				return info, err
			}
			res.TrimOperators += len(t.Online)
			res.TrimReleased++
			info.release++
		}
		if err := saveCompany(ctx, q, co, now); err != nil {
			return info, err
		}
		if err := ob.flush(ctx, q, now); err != nil {
			return info, err
		}
	}
	if res.TrimOperators > 0 {
		res.Trims = 1
	}
	info.after = counts.InTeam - int64(res.TrimOperators)
	return info, nil
}

// lockTrimTeam locks the company row of a team and reads what the plan
// needs, and every member for a release.
func (s *Service) lockTrimTeam(ctx context.Context, q *pgdb.Queries, company string) (trimTeam, *pgdb.Company, []opInfo, error) {
	co, err := q.LockCompany(ctx, company)
	if err != nil {
		return trimTeam{}, nil, nil, fmt.Errorf("lock company: %w", err)
	}
	loads, load, err := companyLoad(ctx, q, company)
	if err != nil {
		return trimTeam{}, nil, nil, err
	}
	all, err := s.teamMembers(ctx, q, company)
	if err != nil {
		return trimTeam{}, nil, nil, err
	}
	t := trimTeam{Company: company, Load: load, Loads: loads}
	for _, m := range all {
		if m.Online {
			t.Online = append(t.Online, m)
		} else {
			t.Offline = append(t.Offline, m.ID)
		}
	}
	return t, &co, all, nil
}
