package router

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// trimOp returns an online member with English and a load.
func trimOp(id string, live int32, langs ...string) opInfo {
	if len(langs) == 0 {
		langs = []string{"en"}
	}
	return opInfo{ID: id, Langs: langs, Live: live, Online: true}
}

// trimTeamOf returns a team of a company with n online English members
// with load 0, named after the company.
func trimTeamOf(company string, load int64, n int, offline ...string) trimTeam {
	t := trimTeam{Company: company, Load: load, Loads: []langLoad{{"en", load}}, Offline: offline}
	for i := range n {
		t.Online = append(t.Online, trimOp(fmt.Sprintf("%s-op-%02d", company, i), 0))
	}
	return t
}

// stepsString writes a plan as "company:action[leave,...]" items, for
// short failure messages and easy wants.
func stepsString(steps []trimStep) string {
	parts := make([]string, len(steps))
	for i, s := range steps {
		parts[i] = s.Company + ":" + s.Action.String()
		if len(s.Leave) > 0 {
			parts[i] += "[" + strings.Join(s.Leave, ",") + "]"
		}
	}
	return strings.Join(parts, " ")
}

func TestPlanTrim(t *testing.T) {
	tests := []struct {
		name    string
		cap     int64
		minSize int
		teams   []trimTeam
		want    string
	}{
		{
			name: "everything fits",
			cap:  10, minSize: 2,
			teams: []trimTeam{trimTeamOf("co-a", 30, 6), trimTeamOf("co-b", 20, 4)},
			want:  "co-a:keep co-b:keep",
		},
		{
			name: "no team",
			cap:  0, minSize: 2,
			want: "",
		},
		{
			name: "one team trimmed to the room",
			cap:  8, minSize: 2,
			teams: []trimTeam{
				trimTeamOf("co-a", 30, 5),
				{Company: "co-b", Load: 20, Loads: []langLoad{{"en", 20}}, Online: []opInfo{
					trimOp("b1", 3), trimOp("b2", 0), trimOp("b3", 1), trimOp("b4", 2),
				}},
			},
			// Room 3 for co-b: the member with the lowest load leaves.
			want: "co-a:keep co-b:shrink[b2]",
		},
		{
			name: "room exactly the min size",
			cap:  7, minSize: 2,
			teams: []trimTeam{trimTeamOf("co-a", 30, 5), trimTeamOf("co-b", 20, 4)},
			want:  "co-a:keep co-b:shrink[co-b-op-00,co-b-op-01]",
		},
		{
			name: "room below the min size releases that team",
			cap:  6, minSize: 2,
			teams: []trimTeam{trimTeamOf("co-a", 30, 5), trimTeamOf("co-b", 20, 4)},
			want:  "co-a:keep co-b:release",
		},
		{
			name: "the first team does not fit",
			cap:  3, minSize: 2,
			teams: []trimTeam{trimTeamOf("co-a", 30, 5), trimTeamOf("co-b", 20, 2)},
			want:  "co-a:shrink[co-a-op-00,co-a-op-01] co-b:release",
		},
		{
			name: "every later team is released even if it would fit",
			cap:  9, minSize: 3,
			teams: []trimTeam{
				trimTeamOf("co-a", 40, 5), trimTeamOf("co-b", 30, 5), trimTeamOf("co-c", 20, 1), trimTeamOf("co-d", 10, 2),
			},
			// co-b gets room 4 and is trimmed; co-c and co-d would fit in
			// nothing, and a team of 1 would fit in a room of 1.
			want: "co-a:keep co-b:shrink[co-b-op-00] co-c:release co-d:release",
		},
		{
			name: "every later team is released after a release",
			cap:  6, minSize: 2,
			teams: []trimTeam{trimTeamOf("co-a", 40, 5), trimTeamOf("co-b", 30, 4), trimTeamOf("co-c", 20, 1)},
			// Room 1 for co-b is below the min size; co-c would fit in it.
			want: "co-a:keep co-b:release co-c:release",
		},
		{
			name: "a team with only offline members fits before the first miss",
			cap:  4, minSize: 2,
			teams: []trimTeam{
				trimTeamOf("co-a", 40, 0, "a-off-1", "a-off-2"), trimTeamOf("co-b", 30, 6), trimTeamOf("co-c", 20, 0, "c-off-1"),
			},
			// co-a takes no room. co-c comes after the first miss and is
			// released with its offline member.
			want: "co-a:keep co-b:shrink[co-b-op-00,co-b-op-01] co-c:release",
		},
		{
			name: "offline members are never chosen to leave",
			cap:  2, minSize: 2,
			teams: []trimTeam{{
				Company: "co-a", Load: 30, Loads: []langLoad{{"en", 30}},
				// The offline members would have the lowest load. They are
				// not in Online, so only online members leave.
				Online:  []opInfo{trimOp("a1", 2), trimOp("a2", 1), trimOp("a3", 4)},
				Offline: []string{"a0", "a00"},
			}},
			want: "co-a:shrink[a2]",
		},
		{
			name: "the only speaker of a needed language leaves last",
			cap:  2, minSize: 2,
			teams: []trimTeam{{
				Company: "co-a", Load: 30, Loads: []langLoad{{"en", 20}, {"ja", 10}},
				Online: []opInfo{trimOp("a1", 3, "en"), trimOp("a2", 0, "ja"), trimOp("a3", 2, "en")},
			}},
			// a2 has the lowest load, but it is the only member with ja.
			// Of the two English members, a3 has the lower load.
			want: "co-a:shrink[a3]",
		},
		{
			name: "ties in load are ordered by company ID",
			cap:  5, minSize: 2,
			teams: []trimTeam{trimTeamOf("co-b", 20, 4), trimTeamOf("co-a", 20, 4)},
			want:  "co-a:keep co-b:release",
		},
		{
			name: "higher load first, whatever the input order",
			cap:  5, minSize: 2,
			teams: []trimTeam{trimTeamOf("co-a", 20, 4), trimTeamOf("co-b", 25, 4)},
			want:  "co-b:keep co-a:release",
		},
		{
			name: "cap 0 releases every team with online members from the first miss",
			cap:  0, minSize: 2,
			teams: []trimTeam{trimTeamOf("co-a", 40, 0, "a-off-1"), trimTeamOf("co-b", 30, 2), trimTeamOf("co-c", 20, 0, "c-off-1")},
			want:  "co-a:keep co-b:release co-c:release",
		},
		{
			name: "cap 0 with no online member keeps every team",
			cap:  0, minSize: 2,
			teams: []trimTeam{trimTeamOf("co-a", 40, 0, "a-off-1")},
			want:  "co-a:keep",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := slices.Clone(tt.teams)
			got := planTrim(tt.cap, tt.minSize, tt.teams)
			if s := stepsString(got); s != tt.want {
				t.Errorf("planTrim() = %q, want %q", s, tt.want)
			}
			if !slices.EqualFunc(in, tt.teams, func(a, b trimTeam) bool { return a.Company == b.Company }) {
				t.Error("planTrim() changed the order of its input")
			}
			// The trim never adds a member, and the online members left
			// are within the cap.
			var left int64
			for i, s := range got {
				team := teamOf(tt.teams, s.Company)
				switch s.Action {
				case trimKeep:
					left += int64(len(team.Online))
				case trimShrink:
					if len(s.Leave) == 0 || len(s.Leave) >= len(team.Online) {
						t.Errorf("step %d: %d leave of %d online", i, len(s.Leave), len(team.Online))
					}
					left += int64(len(team.Online) - len(s.Leave))
				}
			}
			if total := onlineOf(tt.teams); total > tt.cap && left > tt.cap {
				t.Errorf("online members after the trim = %d, above the cap %d", left, tt.cap)
			}
		})
	}
}

func teamOf(teams []trimTeam, company string) trimTeam {
	i := slices.IndexFunc(teams, func(t trimTeam) bool { return t.Company == company })
	return teams[i]
}

func onlineOf(teams []trimTeam) int64 {
	var n int64
	for _, t := range teams {
		n += int64(len(t.Online))
	}
	return n
}
