package routersvc

import (
	"strings"
	"testing"
	"time"

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
		EnvTeamFormLoad:        "10",
		EnvTeamReleaseLoad:     "3",
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
		cfg.Router.Capacity != 6 || cfg.Router.AcceptTimeout != 5*time.Second || cfg.Router.TeamFormLoad != 10 ||
		cfg.Router.TeamReleaseLoad != 3 || cfg.AssignWorkers != 8 || cfg.PublishBatch != 200 ||
		cfg.OutboxKeep != 30*time.Minute || cfg.Postgres.MaxConns != 16 || cfg.Postgres.ConnectWait != 10*time.Second ||
		cfg.ExpireInterval != 500*time.Millisecond || cfg.Router.FirstResponseTarget != 90*time.Second ||
		cfg.OperatorRefresh != 2*time.Hour {
		t.Errorf("config %+v", cfg)
	}
	if isLoopback(cfg.APIAddr) {
		t.Error("0.0.0.0 counts as loopback")
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
		{"thresholds", map[string]string{EnvTeamFormLoad: "8", EnvTeamReleaseLoad: "8"}, []string{EnvTeamFormLoad}},
		{"sizes", map[string]string{EnvTeamMinSize: "5", EnvTeamMaxSize: "4"}, []string{EnvTeamMaxSize}},
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
