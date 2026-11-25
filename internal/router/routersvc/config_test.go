package routersvc

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg"
)

func lookupFrom(env map[string]string) LookupFunc {
	return func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(lookupFrom(nil))
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"brokers", strings.Join(cfg.KafkaBrokers, ","), DefaultKafkaBrokers},
		{"group", cfg.GroupID, DefaultGroupID},
		{"metrics", cfg.MetricsAddr, ":9468"},
		{"api", cfg.APIAddr, "127.0.0.1:9470"},
		{"dsn", cfg.Postgres.DSN, pg.DefaultDSN},
		{"max conns", cfg.Postgres.MaxConns, int32(pg.DefaultMaxConns)},
		{"speed", cfg.Router.SpeedFactor, 1.0},
		{"capacity", cfg.Router.Capacity, 4},
		{"accept", cfg.Router.AcceptTimeout, 20 * time.Second},
		{"assignment target", cfg.Router.AssignmentTarget, time.Minute},
		{"response target", cfg.Router.FirstResponseTarget, 2 * time.Minute},
		{"form", cfg.Router.TeamFormLoad, 20},
		{"release", cfg.Router.TeamReleaseLoad, 8},
		{"release after", cfg.Router.TeamReleaseAfter, 15 * time.Minute},
		{"team size", [2]int{cfg.Router.TeamMinSize, cfg.Router.TeamMaxSize}, [2]int{2, 20}},
		{"team max share", cfg.Router.TeamMaxShare, 0.5},
		{"workers", cfg.AssignWorkers, 4},
		{"outbox keep", cfg.OutboxKeep, time.Hour},
		{"outage", cfg.OutageLimit, 2 * time.Minute},
		{"operator refresh", cfg.OperatorRefresh, 6 * time.Hour},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if !isLoopback(cfg.APIAddr) {
		t.Errorf("default API address %q is not loopback", cfg.APIAddr)
	}
}

func TestLoadValues(t *testing.T) {
	cfg, err := Load(lookupFrom(map[string]string{
		EnvKafkaBrokers:        "k1:9092, k2:9092",
		EnvAPIAddr:             "0.0.0.0:9500",
		EnvSpeedFactor:         "50",
		EnvOperatorCapacity:    "6",
		EnvAcceptTimeout:       "5s",
		EnvAssignWorkers:       "8",
		EnvPublishBatch:        "200",
		EnvOutboxKeep:          "30m",
		EnvLogLevel:            "debug",
		pg.EnvDSN:              "postgres://u:secret@db:5432/x?sslmode=disable",
		pg.EnvMaxConns:         "16",
		pg.EnvConnectWait:      "10s",
		EnvExpireInterval:      "500ms",
		EnvFirstResponseTarget: " 90s ",
		EnvOperatorRefresh:     "2h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.APIAddr != "0.0.0.0:9500" || cfg.Router.SpeedFactor != 50 ||
		cfg.Router.Capacity != 6 || cfg.Router.AcceptTimeout != 5*time.Second || cfg.AssignWorkers != 8 || cfg.PublishBatch != 200 ||
		cfg.OutboxKeep != 30*time.Minute || cfg.Postgres.MaxConns != 16 || cfg.Postgres.ConnectWait != 10*time.Second ||
		cfg.ExpireInterval != 500*time.Millisecond || cfg.Router.FirstResponseTarget != 90*time.Second ||
		cfg.OperatorRefresh != 2*time.Hour {
		t.Errorf("config %+v", cfg)
	}
	if isLoopback(cfg.APIAddr) {
		t.Error("0.0.0.0 counts as loopback")
	}
}

// TestLoadIgnoresRemovedVars: the team numbers are stand settings
// (decision 0014). Their old variables are not read, so even a value that
// does not parse keeps the defaults of the code and is no error.
func TestLoadIgnoresRemovedVars(t *testing.T) {
	for _, v := range []string{"10", "abc", "-1", "0.01"} {
		env := map[string]string{}
		for _, key := range RemovedVars {
			env[key] = v
		}
		cfg, err := Load(lookupFrom(env))
		if err != nil {
			t.Fatalf("Load() with removed variables set to %q: %v", v, err)
		}
		got := opapi.TeamSettings{
			FormLoad: cfg.Router.TeamFormLoad, ReleaseLoad: cfg.Router.TeamReleaseLoad,
			ReleaseAfterUS: cfg.Router.TeamReleaseAfter.Microseconds(), MinSize: cfg.Router.TeamMinSize,
			MaxSize: cfg.Router.TeamMaxSize, MaxShare: cfg.Router.TeamMaxShare,
		}
		if got != opapi.DefaultTeamSettings() {
			t.Errorf("team numbers with removed variables set to %q = %+v, want the defaults", v, got)
		}
	}
	// THREADROUTE_ROUTER_TEAM_INTERVAL is not a stand setting and stays.
	cfg, err := Load(lookupFrom(map[string]string{EnvTeamInterval: "30s"}))
	if err != nil || cfg.TeamInterval != 30*time.Second {
		t.Errorf("team interval = %s, %v; want 30s", cfg.TeamInterval, err)
	}
}

func TestWarnRemoved(t *testing.T) {
	want := []string{
		"THREADROUTE_ROUTER_TEAM_FORM_LOAD", "THREADROUTE_ROUTER_TEAM_RELEASE_LOAD", "THREADROUTE_ROUTER_TEAM_RELEASE_AFTER",
		"THREADROUTE_ROUTER_TEAM_MIN_SIZE", "THREADROUTE_ROUTER_TEAM_MAX_SIZE", "THREADROUTE_ROUTER_TEAM_MAX_SHARE",
	}
	if strings.Join(RemovedVars, ",") != strings.Join(want, ",") {
		t.Fatalf("RemovedVars = %v, want %v", RemovedVars, want)
	}
	tests := []struct {
		name string
		env  map[string]string
		want []string
	}{
		{"none set", nil, nil},
		{"empty and blank values", map[string]string{want[0]: "", want[1]: "  "}, nil},
		{"one set", map[string]string{want[0]: "30"}, []string{want[0]}},
		{"two set", map[string]string{want[2]: "10m", want[5]: "abc"}, []string{want[2], want[5]}},
		{"other variables", map[string]string{EnvTeamInterval: "10s", EnvSpeedFactor: "2"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			WarnRemoved(lookupFrom(tt.env), slog.New(slog.NewTextHandler(&buf, nil)))
			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			if buf.Len() == 0 {
				lines = nil
			}
			if len(lines) != len(tt.want) {
				t.Fatalf("%d log lines, want %d:\n%s", len(lines), len(tt.want), buf.String())
			}
			for i, line := range lines {
				if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "variable="+tt.want[i]) ||
					!strings.Contains(line, "stand settings") || !strings.Contains(line, "PUT /v1/settings") {
					t.Errorf("line %q does not warn about %s", line, tt.want[i])
				}
				if strings.Contains(line, "abc") || strings.Contains(line, "10m") {
					t.Errorf("line %q holds the value", line)
				}
			}
		})
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []string
	}{
		{"bad broker", map[string]string{EnvKafkaBrokers: "nohost"}, []string{EnvKafkaBrokers}},
		{"group with space", map[string]string{EnvGroupID: "a b"}, []string{EnvGroupID}},
		{"bad api addr", map[string]string{EnvAPIAddr: "localhost"}, []string{EnvAPIAddr}},
		{"port 0", map[string]string{EnvMetricsAddr: ":0"}, []string{EnvMetricsAddr}},
		{"speed zero", map[string]string{EnvSpeedFactor: "0"}, []string{EnvSpeedFactor}},
		{"speed NaN", map[string]string{EnvSpeedFactor: "NaN"}, []string{EnvSpeedFactor}},
		{"speed too large", map[string]string{EnvSpeedFactor: "100000"}, []string{EnvSpeedFactor}},
		{"capacity", map[string]string{EnvOperatorCapacity: "101"}, []string{EnvOperatorCapacity}},
		{"duration word", map[string]string{EnvAcceptTimeout: "soon"}, []string{EnvAcceptTimeout}},
		{"duration negative", map[string]string{EnvSLAInterval: "-1s"}, []string{EnvSLAInterval}},
		{"duration nanoseconds", map[string]string{EnvAssignmentTarget: "1500ns"}, []string{EnvAssignmentTarget}},
		{"workers", map[string]string{EnvAssignWorkers: "0"}, []string{EnvAssignWorkers}},
		{"batch", map[string]string{EnvPublishBatch: "x"}, []string{EnvPublishBatch}},
		{"log level", map[string]string{EnvLogLevel: "loud"}, []string{EnvLogLevel}},
		{"max conns", map[string]string{pg.EnvMaxConns: "0"}, []string{pg.EnvMaxConns}},
		{"refresh not positive", map[string]string{EnvOperatorRefresh: "-1h"}, []string{EnvOperatorRefresh}},
		{"several", map[string]string{EnvOutageLimit: "0s", EnvPruneInterval: "x"}, []string{EnvOutageLimit, EnvPruneInterval}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(lookupFrom(tt.env))
			if err == nil {
				t.Fatal("Load() = nil error")
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not name %s", err, w)
				}
			}
		})
	}
}

// TestLoadHidesDSN checks that a DSN that does not parse never shows up in
// the error.
func TestLoadHidesDSN(t *testing.T) {
	_, err := Load(lookupFrom(map[string]string{pg.EnvDSN: "postgres://u:topsecret@db:notaport/x"}))
	if err == nil {
		t.Fatal("Load() = nil error")
	}
	if strings.Contains(err.Error(), "topsecret") {
		t.Errorf("error %q contains the password", err)
	}
	if !strings.Contains(err.Error(), pg.EnvDSN) {
		t.Errorf("error %q does not name %s", err, pg.EnvDSN)
	}
}

func TestValidateZero(t *testing.T) {
	err := Config{}.Validate()
	if err == nil {
		t.Fatal("Validate() of a zero config = nil")
	}
	for _, w := range []string{"brokers", "group", "postgres", "router", "workers", "publish batch", "outage limit"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("error %q does not mention %q", err, w)
		}
	}
}

func TestIsLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:9470": true, "localhost:9470": true, "[::1]:9470": true,
		":9470": false, "0.0.0.0:9470": false, "10.0.0.5:9470": false, "bad": false,
	} {
		if got := isLoopback(addr); got != want {
			t.Errorf("isLoopback(%q) = %v, want %v", addr, got, want)
		}
	}
}
