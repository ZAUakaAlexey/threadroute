package conv

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"
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
		KafkaBrokers:   []string{"localhost:9092"},
		GroupID:        "threadroute-conv",
		RedisAddr:      "localhost:6379",
		RedisPassword:  "",
		RedisDB:        0,
		SweepInterval:  time.Second,
		InputTopicWait: 2 * time.Minute,
		MetricsAddr:    ":9467",
		LogLevel:       slog.LevelInfo,
		Rules: Rules{
			AdmissionThreshold: 10,
			AdmissionWindow:    time.Hour,
			AdmissionIdle:      2 * time.Hour,
			CompanyLimit:       0,
			InactivityTimeout:  30 * time.Minute,
			TextKeptAfterClose: 10 * time.Minute,
			MessagesKept:       50,
			SessionRecordKept:  24 * time.Hour,
			EarlyDeletionKept:  10 * time.Minute,
			AppliedLifetime:    7 * time.Hour,
			KeyMargin:          7 * time.Hour,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	got, err := Load(lookupFrom(map[string]string{
		"THREADROUTE_KAFKA_BROKERS":              "kafka-a:9092, kafka-b:9093",
		"THREADROUTE_CONV_GROUP_ID":              "conv-test",
		"THREADROUTE_REDIS_ADDR":                 "redis:6380",
		"THREADROUTE_REDIS_PASSWORD":             "test-password",
		"THREADROUTE_REDIS_DB":                   "3",
		"THREADROUTE_CONV_SWEEP_INTERVAL":        "250ms",
		"THREADROUTE_CONV_INPUT_TOPIC_WAIT":      "30s",
		"THREADROUTE_CONV_METRICS_ADDR":          "127.0.0.1:19467",
		"THREADROUTE_LOG_LEVEL":                  "debug",
		"THREADROUTE_CONV_ADMISSION_THRESHOLD":   "5",
		"THREADROUTE_CONV_ADMISSION_WINDOW":      "30m",
		"THREADROUTE_CONV_ADMISSION_IDLE":        "24h",
		"THREADROUTE_CONV_COMPANY_LIMIT":         "200",
		"THREADROUTE_CONV_INACTIVITY_TIMEOUT":    "1h",
		"THREADROUTE_CONV_TEXT_KEPT_AFTER_CLOSE": "5m",
		"THREADROUTE_CONV_MESSAGES_KEPT":         "20",
		"THREADROUTE_CONV_SESSION_RECORD_KEPT":   "12h",
		"THREADROUTE_CONV_EARLY_DELETION_KEPT":   "15m",
		"THREADROUTE_CONV_APPLIED_LIFETIME":      "2h",
		"THREADROUTE_CONV_KEY_MARGIN":            "3h",
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Config{
		KafkaBrokers:   []string{"kafka-a:9092", "kafka-b:9093"},
		GroupID:        "conv-test",
		RedisAddr:      "redis:6380",
		RedisPassword:  "test-password",
		RedisDB:        3,
		SweepInterval:  250 * time.Millisecond,
		InputTopicWait: 30 * time.Second,
		MetricsAddr:    "127.0.0.1:19467",
		LogLevel:       slog.LevelDebug,
		Rules: Rules{
			AdmissionThreshold: 5,
			AdmissionWindow:    30 * time.Minute,
			AdmissionIdle:      24 * time.Hour,
			CompanyLimit:       200,
			InactivityTimeout:  time.Hour,
			TextKeptAfterClose: 5 * time.Minute,
			MessagesKept:       20,
			SessionRecordKept:  12 * time.Hour,
			EarlyDeletionKept:  15 * time.Minute,
			AppliedLifetime:    2 * time.Hour,
			KeyMargin:          3 * time.Hour,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadInvalid(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []string // every one must be named in the error
	}{
		{"broker without port", map[string]string{EnvKafkaBrokers: "kafka"}, []string{EnvKafkaBrokers}},
		{"no brokers", map[string]string{EnvKafkaBrokers: " , "}, []string{EnvKafkaBrokers}},
		{"group with a space", map[string]string{EnvGroupID: "a b"}, []string{EnvGroupID}},
		{"redis address", map[string]string{EnvRedisAddr: "redis:0"}, []string{EnvRedisAddr}},
		{"negative redis db", map[string]string{EnvRedisDB: "-1"}, []string{EnvRedisDB}},
		{"threshold not a number", map[string]string{EnvAdmissionThreshold: "ten"}, []string{EnvAdmissionThreshold}},
		{"threshold zero", map[string]string{EnvAdmissionThreshold: "0"}, []string{EnvAdmissionThreshold}},
		{"negative limit", map[string]string{EnvCompanyLimit: "-1"}, []string{EnvCompanyLimit}},
		{"messages kept zero", map[string]string{EnvMessagesKept: "0"}, []string{EnvMessagesKept}},
		{"window not a duration", map[string]string{EnvAdmissionWindow: "1 hour"}, []string{EnvAdmissionWindow}},
		{"negative timeout", map[string]string{EnvInactivityTimeout: "-30m"}, []string{EnvInactivityTimeout}},
		{"sub-microsecond", map[string]string{EnvTextKeptAfterClose: "10m1ns"}, []string{EnvTextKeptAfterClose}},
		{"idle not above timeout", map[string]string{EnvAdmissionIdle: "30m"}, []string{EnvAdmissionIdle}},
		{"log level", map[string]string{EnvLogLevel: "loud"}, []string{EnvLogLevel}},
		{"metrics address", map[string]string{EnvMetricsAddr: "nowhere"}, []string{EnvMetricsAddr}},
		{"zero sweep interval", map[string]string{EnvSweepInterval: "0s"}, []string{EnvSweepInterval}},
		{
			"every invalid variable is named",
			map[string]string{EnvAdmissionThreshold: "x", EnvSweepInterval: "y", EnvAppliedLifetime: "-1h", EnvKeyMargin: "z"},
			[]string{EnvAdmissionThreshold, EnvSweepInterval, EnvAppliedLifetime, EnvKeyMargin},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(lookupFrom(tt.env))
			if err == nil {
				t.Fatal("Load() error = nil")
			}
			for _, name := range tt.want {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("error %q does not name %s", err, name)
				}
			}
		})
	}
}

func TestLoadDoesNotTrimPassword(t *testing.T) {
	cfg, err := Load(lookupFrom(map[string]string{EnvRedisPassword: " spaced "}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RedisPassword != " spaced " {
		t.Errorf("password = %q", cfg.RedisPassword)
	}
}

func TestRulesValidate(t *testing.T) {
	r := DefaultRules()
	r.AppliedLifetime = time.Microsecond
	r.AdmissionThreshold = 0
	err := r.Validate()
	if err == nil || !strings.Contains(err.Error(), "applied lifetime") || !strings.Contains(err.Error(), "threshold") {
		t.Errorf("Validate() = %v", err)
	}
}
