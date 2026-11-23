package normalize

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
		KafkaBrokers:    []string{"localhost:9092"},
		GroupID:         "threadroute-normalize",
		TransactionalID: "threadroute-normalize",
		Start:           StartBeginning,
		MaxBatchRecords: 1000,
		RawTopicWait:    2 * time.Minute,
		Producer: bus.ProducerLimits{
			MaxBufferedRecords: 10_000,
			MaxBufferedBytes:   32 << 20,
			DeliveryTimeout:    2 * time.Minute,
		},
		RedisAddr:        "localhost:6379",
		RedisPassword:    "",
		RedisDB:          0,
		DedupKeyLifetime: 10 * time.Minute,
		LogLevel:         slog.LevelInfo,
		MetricsAddr:      ":9466",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	got, err := Load(lookupFrom(map[string]string{
		"THREADROUTE_KAFKA_BROKERS":                           "kafka-a:9092, kafka-b:9093",
		"THREADROUTE_LOG_LEVEL":                               "debug",
		"THREADROUTE_REDIS_ADDR":                              "redis.example.test:6380",
		"THREADROUTE_REDIS_PASSWORD":                          " synthetic secret ",
		"THREADROUTE_REDIS_DB":                                "3",
		"THREADROUTE_NORMALIZE_GROUP_ID":                      "normalize-a",
		"THREADROUTE_NORMALIZE_TRANSACTIONAL_ID":              "normalize-a-0",
		"THREADROUTE_NORMALIZE_START":                         "END",
		"THREADROUTE_NORMALIZE_MAX_BATCH_RECORDS":             "200",
		"THREADROUTE_NORMALIZE_DEDUP_KEY_LIFETIME":            "1h",
		"THREADROUTE_NORMALIZE_RAW_TOPIC_WAIT":                "30s",
		"THREADROUTE_NORMALIZE_METRICS_ADDR":                  "127.0.0.1:19466",
		"THREADROUTE_NORMALIZE_PRODUCER_MAX_BUFFERED_RECORDS": "500",
		"THREADROUTE_NORMALIZE_PRODUCER_MAX_BUFFERED_BYTES":   "1048576",
		"THREADROUTE_NORMALIZE_PRODUCER_DELIVERY_TIMEOUT":     "45s",
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Config{
		KafkaBrokers:    []string{"kafka-a:9092", "kafka-b:9093"},
		GroupID:         "normalize-a",
		TransactionalID: "normalize-a-0",
		Start:           StartEnd,
		MaxBatchRecords: 200,
		RawTopicWait:    30 * time.Second,
		Producer: bus.ProducerLimits{
			MaxBufferedRecords: 500,
			MaxBufferedBytes:   1 << 20,
			DeliveryTimeout:    45 * time.Second,
		},
		RedisAddr:        "redis.example.test:6380",
		RedisPassword:    " synthetic secret ",
		RedisDB:          3,
		DedupKeyLifetime: time.Hour,
		LogLevel:         slog.LevelDebug,
		MetricsAddr:      "127.0.0.1:19466",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadEmptyMeansDefault(t *testing.T) {
	got, err := Load(lookupFrom(map[string]string{
		"THREADROUTE_KAFKA_BROKERS":                "  ",
		"THREADROUTE_NORMALIZE_METRICS_ADDR":       "",
		"THREADROUTE_NORMALIZE_GROUP_ID":           "",
		"THREADROUTE_NORMALIZE_DEDUP_KEY_LIFETIME": "",
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.MetricsAddr != DefaultMetricsAddr || got.GroupID != DefaultGroupID || len(got.KafkaBrokers) != 1 || got.DedupKeyLifetime != 10*time.Minute {
		t.Errorf("Load() = %+v, want defaults", got)
	}
}

func TestLoadInvalid(t *testing.T) {
	tests := []struct {
		key   string
		value string
	}{
		{"THREADROUTE_KAFKA_BROKERS", "kafka-a"},
		{"THREADROUTE_KAFKA_BROKERS", "kafka-a:0"},
		{"THREADROUTE_KAFKA_BROKERS", ",,"},
		{"THREADROUTE_LOG_LEVEL", "loud"},
		{"THREADROUTE_REDIS_ADDR", "redis"},
		{"THREADROUTE_REDIS_ADDR", "redis:99999"},
		{"THREADROUTE_REDIS_DB", "one"},
		{"THREADROUTE_REDIS_DB", "-1"},
		{"THREADROUTE_NORMALIZE_GROUP_ID", "group one"},
		{"THREADROUTE_NORMALIZE_TRANSACTIONAL_ID", "a,b"},
		{"THREADROUTE_NORMALIZE_START", "middle"},
		{"THREADROUTE_NORMALIZE_MAX_BATCH_RECORDS", "0"},
		{"THREADROUTE_NORMALIZE_MAX_BATCH_RECORDS", "many"},
		{"THREADROUTE_NORMALIZE_MAX_BATCH_RECORDS", "20000"},
		{"THREADROUTE_NORMALIZE_MAX_BATCH_RECORDS", "99999999999"},
		{"THREADROUTE_NORMALIZE_DEDUP_KEY_LIFETIME", "10"},
		{"THREADROUTE_NORMALIZE_DEDUP_KEY_LIFETIME", "-1m"},
		{"THREADROUTE_NORMALIZE_DEDUP_KEY_LIFETIME", "500ms"},
		{"THREADROUTE_NORMALIZE_RAW_TOPIC_WAIT", "0s"},
		{"THREADROUTE_NORMALIZE_METRICS_ADDR", "9466"},
		{"THREADROUTE_NORMALIZE_PRODUCER_MAX_BUFFERED_RECORDS", "-5"},
		{"THREADROUTE_NORMALIZE_PRODUCER_MAX_BUFFERED_BYTES", "1.5"},
		{"THREADROUTE_NORMALIZE_PRODUCER_DELIVERY_TIMEOUT", "100ms"},
		{"THREADROUTE_NORMALIZE_PRODUCER_DELIVERY_TIMEOUT", "soon"},
	}
	for _, tt := range tests {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			_, err := Load(lookupFrom(map[string]string{tt.key: tt.value}))
			if err == nil {
				t.Fatal("Load() error = nil")
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Errorf("error %q does not name %s", err, tt.key)
			}
		})
	}
}

func TestLoadNamesEveryInvalidVariable(t *testing.T) {
	_, err := Load(lookupFrom(map[string]string{
		"THREADROUTE_KAFKA_BROKERS":          "nope",
		"THREADROUTE_REDIS_DB":               "x",
		"THREADROUTE_NORMALIZE_START":        "x",
		"THREADROUTE_NORMALIZE_METRICS_ADDR": "x",
	}))
	if err == nil {
		t.Fatal("Load() error = nil")
	}
	for _, key := range []string{EnvKafkaBrokers, EnvRedisDB, EnvStart, EnvMetricsAddr} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not name %s: %v", key, err)
		}
	}
}

func TestLoadDoesNotLeakPassword(t *testing.T) {
	_, err := Load(lookupFrom(map[string]string{
		"THREADROUTE_REDIS_PASSWORD": "synthetic-password-value",
		"THREADROUTE_REDIS_DB":       "x",
	}))
	if err == nil || strings.Contains(err.Error(), "synthetic-password-value") {
		t.Errorf("Load() error = %v", err)
	}
}
