package sim

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

func lookup(env map[string]string) LookupFunc {
	return func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	b := cfg.Behaviour
	if b.Operators != 200 || b.Capacity != 4 || b.ShiftOnline != 4*time.Hour || b.ShiftOffline != 30*time.Minute ||
		b.AcceptDelayMin != time.Second || b.AcceptDelayMax != 8*time.Second || b.MissedOfferShare != 0.03 ||
		b.HandlingMedian != 45*time.Second || b.ResolveMedian != 3*time.Minute || b.Seed != 1 {
		t.Errorf("behaviour %+v", b)
	}
	if b.Languages.String() != defaults.SimLanguages || b.Languages.total() != opapi.DefaultSimOperators {
		t.Errorf("languages %s, total %d", b.Languages, b.Languages.total())
	}
	if got := simOf(b, 0); got != opapi.DefaultSimSettings() {
		t.Errorf("sim values %+v, want the defaults of the code %+v", got, opapi.DefaultSimSettings())
	}
	if cfg.RouterURL != "http://127.0.0.1:9470" || cfg.MetricsAddr != ":9469" || cfg.GroupID != "threadroute-sim" ||
		cfg.OutageLimit != 2*time.Minute || cfg.StartWait != 2*time.Minute || cfg.LogLevel != slog.LevelInfo ||
		len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Errorf("config %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(lookup(map[string]string{
		EnvKafkaBrokers:   "kafka:9092, kafka2:9092",
		EnvRouterURL:      "http://router:9470",
		EnvLanguages:      "en:8, ja:2",
		EnvShiftOnline:    "1h",
		EnvShiftOffline:   "5m",
		EnvAcceptDelayMin: "0s",
		EnvAcceptDelayMax: "2s",
		EnvSeed:           "18446744073709551615",
		EnvGroupID:        "sim-test",
		EnvMetricsAddr:    "127.0.0.1:19469",
		EnvOutageLimit:    "30s",
		EnvStartWait:      "10s",
		EnvLogLevel:       "debug",
	}))
	if err != nil {
		t.Fatal(err)
	}
	b := cfg.Behaviour
	if b.Languages.String() != "en:8,ja:2" || b.ShiftOnline != time.Hour ||
		b.ShiftOffline != 5*time.Minute || b.AcceptDelayMin != 0 || b.AcceptDelayMax != 2*time.Second ||
		b.Seed != 18446744073709551615 {
		t.Errorf("behaviour %+v", b)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.RouterURL != "http://router:9470" || cfg.GroupID != "sim-test" ||
		cfg.MetricsAddr != "127.0.0.1:19469" || cfg.OutageLimit != 30*time.Second || cfg.StartWait != 10*time.Second ||
		cfg.LogLevel != slog.LevelDebug {
		t.Errorf("config %+v", cfg)
	}
}

func TestLoadInvalid(t *testing.T) {
	tests := []struct {
		env  map[string]string
		want []string
	}{
		{map[string]string{EnvLanguages: "en"}, []string{EnvLanguages, "lang:count"}},
		{map[string]string{EnvLanguages: "und:3"}, []string{EnvLanguages, "general queue"}},
		{map[string]string{EnvShiftOnline: "0s"}, []string{EnvShiftOnline}},
		{map[string]string{EnvAcceptDelayMin: "9s"}, []string{EnvAcceptDelayMin}},
		{map[string]string{EnvSeed: "-1"}, []string{EnvSeed}},
		{map[string]string{EnvRouterURL: "ftp://router"}, []string{EnvRouterURL}},
		{map[string]string{EnvMetricsAddr: "9469"}, []string{EnvMetricsAddr}},
		{map[string]string{EnvGroupID: "a b"}, []string{EnvGroupID}},
		{map[string]string{EnvOutageLimit: "-1s"}, []string{EnvOutageLimit}},
		{map[string]string{EnvLogLevel: "loud"}, []string{EnvLogLevel}},
		// Every invalid variable is named.
		{map[string]string{EnvShiftOffline: "x", EnvSeed: "y"}, []string{EnvShiftOffline, EnvSeed}},
	}
	for _, tt := range tests {
		_, err := Load(lookup(tt.env))
		if err == nil {
			t.Errorf("%v: no error", tt.env)
			continue
		}
		for _, w := range tt.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%v: error %q does not name %q", tt.env, err, w)
			}
		}
	}
}

// TestLoadIgnoresRemovedVars: the sim numbers are stand settings (decision
// 0014). Their old variables are not read, so even a value that does not
// parse keeps the defaults of the code and is no error.
func TestLoadIgnoresRemovedVars(t *testing.T) {
	for _, v := range []string{"10", "abc", "-1", "1.5"} {
		env := map[string]string{}
		for _, key := range RemovedVars {
			env[key] = v
		}
		cfg, err := Load(lookup(env))
		if err != nil {
			t.Fatalf("Load() with removed variables set to %q: %v", v, err)
		}
		if got := simOf(cfg.Behaviour, 0); got != opapi.DefaultSimSettings() {
			t.Errorf("sim values with removed variables set to %q = %+v, want the defaults", v, got)
		}
	}
}

func TestWarnRemoved(t *testing.T) {
	want := []string{
		"THREADROUTE_SIM_OPERATORS", "THREADROUTE_SIM_CAPACITY", "THREADROUTE_SIM_HANDLING_MEDIAN",
		"THREADROUTE_SIM_RESOLVE_MEDIAN", "THREADROUTE_SIM_MISSED_OFFER_SHARE",
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
		{"empty and blank values", map[string]string{want[0]: "", want[1]: " "}, nil},
		{"one set", map[string]string{want[1]: "6"}, []string{want[1]}},
		{"all set", map[string]string{want[0]: "1", want[1]: "2", want[2]: "3s", want[3]: "4m", want[4]: "abc"}, want},
		{"other variables", map[string]string{EnvLanguages: "en:1", EnvSeed: "2"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			WarnRemoved(lookup(tt.env), slog.New(slog.NewTextHandler(&buf, nil)))
			var lines []string
			if buf.Len() > 0 {
				lines = strings.Split(strings.TrimSpace(buf.String()), "\n")
			}
			if len(lines) != len(tt.want) {
				t.Fatalf("%d log lines, want %d:\n%s", len(lines), len(tt.want), buf.String())
			}
			for i, line := range lines {
				if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "variable="+tt.want[i]) ||
					!strings.Contains(line, "stand settings") || !strings.Contains(line, "PUT /v1/settings") {
					t.Errorf("line %q does not warn about %s", line, tt.want[i])
				}
				if strings.Contains(line, "abc") {
					t.Errorf("line %q holds the value", line)
				}
			}
		})
	}
}

func TestParseSpread(t *testing.T) {
	tests := []struct {
		in   string
		want string
		err  string
	}{
		{"en:176,ja:6,de:4,fr:4,es:4,pt:3,ko:3", "en:176,ja:6,de:4,fr:4,es:4,pt:3,ko:3", ""},
		{" en : 3 , ja:1 ,", "en:3,ja:1", ""},
		{"fil:2", "fil:2", ""},
		{"", "", "no languages"},
		{"en:0", "", "positive"},
		{"en:x", "", "positive"},
		{"EN:1", "", "lower case"},
		{"en:1,en:2", "", "twice"},
		{"und:1", "", "general queue"},
		{"en", "", "lang:count"},
	}
	for _, tt := range tests {
		s, err := ParseSpread(tt.in)
		if tt.err != "" {
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Errorf("%q: error %v, want %q", tt.in, err, tt.err)
			}
			continue
		}
		if err != nil || s.String() != tt.want {
			t.Errorf("%q: %s, %v, want %s", tt.in, s, err, tt.want)
		}
	}
}
