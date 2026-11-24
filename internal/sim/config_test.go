package sim

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
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
	if b.Operators != 200 || b.Capacity != 0 || b.ShiftOnline != 4*time.Hour || b.ShiftOffline != 30*time.Minute ||
		b.AcceptDelayMin != time.Second || b.AcceptDelayMax != 8*time.Second || b.MissedOfferShare != 0.03 ||
		b.HandlingMedian != 45*time.Second || b.ResolveMedian != 3*time.Minute || b.Seed != 1 {
		t.Errorf("behaviour %+v", b)
	}
	if b.Languages.String() != defaults.SimLanguages || b.Languages.total() != defaults.SimOperators {
		t.Errorf("languages %s, total %d", b.Languages, b.Languages.total())
	}
	if cfg.RouterURL != "http://127.0.0.1:9470" || cfg.MetricsAddr != ":9469" || cfg.GroupID != "threadroute-sim" ||
		cfg.OutageLimit != 2*time.Minute || cfg.StartWait != 2*time.Minute || cfg.LogLevel != slog.LevelInfo ||
		len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Errorf("config %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(lookup(map[string]string{
		EnvKafkaBrokers:     "kafka:9092, kafka2:9092",
		EnvRouterURL:        "http://router:9470",
		EnvOperators:        "10",
		EnvLanguages:        "en:8, ja:2",
		EnvCapacity:         "3",
		EnvShiftOnline:      "1h",
		EnvShiftOffline:     "5m",
		EnvAcceptDelayMin:   "0s",
		EnvAcceptDelayMax:   "2s",
		EnvMissedOfferShare: "0",
		EnvHandlingMedian:   "10s",
		EnvResolveMedian:    "1m",
		EnvSeed:             "18446744073709551615",
		EnvGroupID:          "sim-test",
		EnvMetricsAddr:      "127.0.0.1:19469",
		EnvOutageLimit:      "30s",
		EnvStartWait:        "10s",
		EnvLogLevel:         "debug",
	}))
	if err != nil {
		t.Fatal(err)
	}
	b := cfg.Behaviour
	if b.Operators != 10 || b.Languages.String() != "en:8,ja:2" || b.Capacity != 3 || b.ShiftOnline != time.Hour ||
		b.ShiftOffline != 5*time.Minute || b.AcceptDelayMin != 0 || b.AcceptDelayMax != 2*time.Second ||
		b.MissedOfferShare != 0 || b.HandlingMedian != 10*time.Second || b.ResolveMedian != time.Minute ||
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
		{map[string]string{EnvOperators: "0"}, []string{EnvOperators}},
		{map[string]string{EnvOperators: "10000"}, []string{EnvOperators}},
		{map[string]string{EnvOperators: "2", EnvLanguages: "en:1,ja:1,de:1"}, []string{EnvLanguages, "at least 3 operators"}},
		{map[string]string{EnvLanguages: "en"}, []string{EnvLanguages, "lang:count"}},
		{map[string]string{EnvLanguages: "und:3"}, []string{EnvLanguages, "general queue"}},
		{map[string]string{EnvCapacity: "101"}, []string{EnvCapacity}},
		{map[string]string{EnvShiftOnline: "0s"}, []string{EnvShiftOnline}},
		{map[string]string{EnvAcceptDelayMin: "9s"}, []string{EnvAcceptDelayMin}},
		{map[string]string{EnvMissedOfferShare: "1.5"}, []string{EnvMissedOfferShare}},
		{map[string]string{EnvHandlingMedian: "soon"}, []string{EnvHandlingMedian}},
		{map[string]string{EnvSeed: "-1"}, []string{EnvSeed}},
		{map[string]string{EnvRouterURL: "ftp://router"}, []string{EnvRouterURL}},
		{map[string]string{EnvMetricsAddr: "9469"}, []string{EnvMetricsAddr}},
		{map[string]string{EnvGroupID: "a b"}, []string{EnvGroupID}},
		{map[string]string{EnvOutageLimit: "-1s"}, []string{EnvOutageLimit}},
		{map[string]string{EnvLogLevel: "loud"}, []string{EnvLogLevel}},
		// Every invalid variable is named.
		{map[string]string{EnvOperators: "x", EnvSeed: "y"}, []string{EnvOperators, EnvSeed}},
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
