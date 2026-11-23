package config

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
)

func lookupFrom(env map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	got, err := Load(lookupFrom(nil))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Config{
		KafkaBrokers:               []string{"localhost:9092"},
		SourcePrimaryHost:          "jetstream.us-east.bsky.network",
		SourceFailoverHost:         "jetstream.us-west.bsky.network",
		SourceCollection:           "app.bsky.feed.post",
		MetricsAddr:                ":9464",
		LogLevel:                   slog.LevelInfo,
		ProducerMaxBufferedRecords: 10_000,
		ProducerMaxBufferedBytes:   32 << 20,
		ProducerDeliveryTimeout:    2 * time.Minute,
		RawLimits:                  bus.RawLimits{Retention: 24 * time.Hour, MaxBytes: 8_000_000_000},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		check func(t *testing.T, c Config)
	}{
		{
			name: "broker list with spaces",
			env:  map[string]string{EnvKafkaBrokers: " kafka-1.example.test:9092 , kafka-2.example.test:9093,"},
			check: func(t *testing.T, c Config) {
				want := []string{"kafka-1.example.test:9092", "kafka-2.example.test:9093"}
				if !reflect.DeepEqual(c.KafkaBrokers, want) {
					t.Errorf("KafkaBrokers = %v, want %v", c.KafkaBrokers, want)
				}
			},
		},
		{
			name: "hosts",
			env: map[string]string{
				EnvSourcePrimaryHost:  "jetstream-a.example.test",
				EnvSourceFailoverHost: "wss://jetstream-b.example.test",
			},
			check: func(t *testing.T, c Config) {
				if c.SourcePrimaryHost != "jetstream-a.example.test" || c.SourceFailoverHost != "wss://jetstream-b.example.test" {
					t.Errorf("hosts = %q, %q", c.SourcePrimaryHost, c.SourceFailoverHost)
				}
			},
		},
		{
			name: "collection",
			env:  map[string]string{EnvSourceCollection: "app.bsky.feed.like"},
			check: func(t *testing.T, c Config) {
				if c.SourceCollection != "app.bsky.feed.like" {
					t.Errorf("SourceCollection = %q", c.SourceCollection)
				}
			},
		},
		{
			name: "metrics addr and log level",
			env:  map[string]string{EnvMetricsAddr: "127.0.0.1:9000", EnvLogLevel: "DEBUG"},
			check: func(t *testing.T, c Config) {
				if c.MetricsAddr != "127.0.0.1:9000" || c.LogLevel != slog.LevelDebug {
					t.Errorf("MetricsAddr = %q, LogLevel = %v", c.MetricsAddr, c.LogLevel)
				}
			},
		},
		{
			name: "producer limits",
			env: map[string]string{
				EnvProducerMaxBufferedRecords: "500",
				EnvProducerMaxBufferedBytes:   "1048576",
				EnvProducerDeliveryTimeout:    "45s",
			},
			check: func(t *testing.T, c Config) {
				if c.ProducerMaxBufferedRecords != 500 || c.ProducerMaxBufferedBytes != 1<<20 || c.ProducerDeliveryTimeout != 45*time.Second {
					t.Errorf("producer = %d, %d, %s", c.ProducerMaxBufferedRecords, c.ProducerMaxBufferedBytes, c.ProducerDeliveryTimeout)
				}
			},
		},
		{
			name: "raw limits",
			env:  map[string]string{EnvRawRetention: "6h", EnvRawMaxBytes: "600000000"},
			check: func(t *testing.T, c Config) {
				want := bus.RawLimits{Retention: 6 * time.Hour, MaxBytes: 600_000_000}
				if c.RawLimits != want {
					t.Errorf("RawLimits = %+v, want %+v", c.RawLimits, want)
				}
			},
		},
		{
			name: "empty value takes the default",
			env:  map[string]string{EnvLogLevel: "", EnvKafkaBrokers: "  "},
			check: func(t *testing.T, c Config) {
				if c.LogLevel != DefaultLogLevel || len(c.KafkaBrokers) != 1 || c.KafkaBrokers[0] != DefaultKafkaBrokers {
					t.Errorf("LogLevel = %v, KafkaBrokers = %v", c.LogLevel, c.KafkaBrokers)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Load(lookupFrom(tt.env))
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			tt.check(t, c)
		})
	}
}

func TestLoadInvalid(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantBad []string
	}{
		{name: "broker without port", env: map[string]string{EnvKafkaBrokers: "kafka.example.test"}, wantBad: []string{EnvKafkaBrokers}},
		{name: "broker bad port", env: map[string]string{EnvKafkaBrokers: "kafka.example.test:99999"}, wantBad: []string{EnvKafkaBrokers}},
		{name: "only commas", env: map[string]string{EnvKafkaBrokers: ",,"}, wantBad: []string{EnvKafkaBrokers}},
		{name: "host with space", env: map[string]string{EnvSourcePrimaryHost: "a b"}, wantBad: []string{EnvSourcePrimaryHost}},
		{name: "host list", env: map[string]string{EnvSourceFailoverHost: "a.example.test,b.example.test"}, wantBad: []string{EnvSourceFailoverHost}},
		{
			name:    "same primary and failover",
			env:     map[string]string{EnvSourceFailoverHost: DefaultSourcePrimaryHost},
			wantBad: []string{EnvSourceFailoverHost},
		},
		{name: "collection wildcard", env: map[string]string{EnvSourceCollection: "app.bsky.*"}, wantBad: []string{EnvSourceCollection}},
		{name: "collection too short", env: map[string]string{EnvSourceCollection: "post"}, wantBad: []string{EnvSourceCollection}},
		{name: "metrics addr without port", env: map[string]string{EnvMetricsAddr: "localhost"}, wantBad: []string{EnvMetricsAddr}},
		{name: "log level", env: map[string]string{EnvLogLevel: "loud"}, wantBad: []string{EnvLogLevel}},
		{name: "records not a number", env: map[string]string{EnvProducerMaxBufferedRecords: "many"}, wantBad: []string{EnvProducerMaxBufferedRecords}},
		{name: "records zero", env: map[string]string{EnvProducerMaxBufferedRecords: "0"}, wantBad: []string{EnvProducerMaxBufferedRecords}},
		{name: "bytes negative", env: map[string]string{EnvProducerMaxBufferedBytes: "-5"}, wantBad: []string{EnvProducerMaxBufferedBytes}},
		{name: "bytes with unit", env: map[string]string{EnvProducerMaxBufferedBytes: "32MB"}, wantBad: []string{EnvProducerMaxBufferedBytes}},
		{name: "timeout not a duration", env: map[string]string{EnvProducerDeliveryTimeout: "120"}, wantBad: []string{EnvProducerDeliveryTimeout}},
		{name: "timeout negative", env: map[string]string{EnvProducerDeliveryTimeout: "-1s"}, wantBad: []string{EnvProducerDeliveryTimeout}},
		{name: "retention shorter than segment", env: map[string]string{EnvRawRetention: "30m"}, wantBad: []string{EnvRawRetention}},
		{name: "retention not whole ms", env: map[string]string{EnvRawRetention: "2h1us"}, wantBad: []string{EnvRawRetention}},
		{name: "raw bytes below partitions", env: map[string]string{EnvRawMaxBytes: "5"}, wantBad: []string{EnvRawMaxBytes}},
		{name: "raw bytes too small for two segments", env: map[string]string{EnvRawMaxBytes: "600000"}, wantBad: []string{EnvRawMaxBytes}},
		{
			name: "every bad variable is named",
			env: map[string]string{
				EnvKafkaBrokers:            "nope",
				EnvLogLevel:                "loud",
				EnvProducerDeliveryTimeout: "soon",
				EnvRawMaxBytes:             "0",
			},
			wantBad: []string{EnvKafkaBrokers, EnvLogLevel, EnvProducerDeliveryTimeout, EnvRawMaxBytes},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(lookupFrom(tt.env))
			if err == nil {
				t.Fatal("Load() error = nil, want error")
			}
			msg := err.Error()
			for _, key := range tt.wantBad {
				if !strings.Contains(msg, key+":") {
					t.Errorf("error does not name %s: %v", key, err)
				}
			}
		})
	}
}
