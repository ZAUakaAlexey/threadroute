package record

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
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

const testHome = "/home/testalice"

func TestLoadDefaults(t *testing.T) {
	got, err := Load(lookupFrom(map[string]string{"HOME": testHome}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Config{
		KafkaBrokers: []string{"localhost:9092"},
		GroupID:      "threadroute-record",
		Start:        StartEnd,
		Writer: WriterOptions{
			Dir:             "/home/testalice/threadroute-recordings",
			SegmentMaxBytes: 256 << 20,
			SegmentMaxAge:   time.Hour,
			Retention:       14 * 24 * time.Hour,
			MaxTotalBytes:   0,
		},
		LogLevel:    slog.LevelInfo,
		MetricsAddr: ":9465",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	got, err := Load(lookupFrom(map[string]string{
		"HOME":                                 testHome,
		"THREADROUTE_KAFKA_BROKERS":            "kafka-a:9092, kafka-b:9093",
		"THREADROUTE_RECORD_GROUP_ID":          "record-test",
		"THREADROUTE_RECORD_START":             "Beginning",
		"THREADROUTE_RECORD_DIR":               "/srv/recordings/",
		"THREADROUTE_RECORD_SEGMENT_MAX_BYTES": "1048576",
		"THREADROUTE_RECORD_SEGMENT_MAX_AGE":   "15m",
		"THREADROUTE_RECORD_RETENTION":         "72h",
		"THREADROUTE_RECORD_MAX_TOTAL_BYTES":   "10000000000",
		"THREADROUTE_LOG_LEVEL":                "debug",
		"THREADROUTE_RECORD_METRICS_ADDR":      "127.0.0.1:19465",
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Config{
		KafkaBrokers: []string{"kafka-a:9092", "kafka-b:9093"},
		GroupID:      "record-test",
		Start:        StartBeginning,
		Writer: WriterOptions{
			Dir:             "/srv/recordings",
			SegmentMaxBytes: 1 << 20,
			SegmentMaxAge:   15 * time.Minute,
			Retention:       72 * time.Hour,
			MaxTotalBytes:   10_000_000_000,
		},
		LogLevel:    slog.LevelDebug,
		MetricsAddr: "127.0.0.1:19465",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadInvalid(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantVar []string
	}{
		{"broker without port", map[string]string{"THREADROUTE_KAFKA_BROKERS": "kafka"}, []string{EnvKafkaBrokers}},
		{"broker bad port", map[string]string{"THREADROUTE_KAFKA_BROKERS": "kafka:0"}, []string{EnvKafkaBrokers}},
		{"no brokers", map[string]string{"THREADROUTE_KAFKA_BROKERS": " , "}, []string{EnvKafkaBrokers}},
		{"group with space", map[string]string{"THREADROUTE_RECORD_GROUP_ID": "a b"}, []string{EnvRecordGroupID}},
		{"unknown start", map[string]string{"THREADROUTE_RECORD_START": "middle"}, []string{EnvRecordStart}},
		{"relative dir", map[string]string{"THREADROUTE_RECORD_DIR": "recordings"}, []string{EnvRecordDir}},
		{"no dir and no home", map[string]string{"HOME": ""}, []string{EnvRecordDir}},
		{"size not a number", map[string]string{"THREADROUTE_RECORD_SEGMENT_MAX_BYTES": "256MiB"}, []string{EnvRecordSegmentBytes}},
		{"size zero", map[string]string{"THREADROUTE_RECORD_SEGMENT_MAX_BYTES": "0"}, []string{EnvRecordSegmentBytes}},
		{"age not a duration", map[string]string{"THREADROUTE_RECORD_SEGMENT_MAX_AGE": "hourly"}, []string{EnvRecordSegmentAge}},
		{"age negative", map[string]string{"THREADROUTE_RECORD_SEGMENT_MAX_AGE": "-1h"}, []string{EnvRecordSegmentAge}},
		{"retention above 14 days", map[string]string{"THREADROUTE_RECORD_RETENTION": "337h"}, []string{EnvRecordRetention}},
		{"total size negative", map[string]string{"THREADROUTE_RECORD_MAX_TOTAL_BYTES": "-5"}, []string{EnvRecordMaxTotalBytes}},
		{"log level", map[string]string{"THREADROUTE_LOG_LEVEL": "loud"}, []string{EnvLogLevel}},
		{"metrics addr without port", map[string]string{"THREADROUTE_RECORD_METRICS_ADDR": "localhost"}, []string{EnvRecordMetricsAddr}},
		{
			"several at once",
			map[string]string{
				"THREADROUTE_RECORD_START":           "middle",
				"THREADROUTE_RECORD_RETENTION":       "30d",
				"THREADROUTE_RECORD_MAX_TOTAL_BYTES": "lots",
			},
			[]string{EnvRecordStart, EnvRecordRetention, EnvRecordMaxTotalBytes},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{"HOME": testHome}
			for k, v := range tt.env {
				env[k] = v
			}
			_, err := Load(lookupFrom(env))
			if err == nil {
				t.Fatal("Load() error = nil, want an error")
			}
			for _, v := range tt.wantVar {
				if !strings.Contains(err.Error(), v) {
					t.Errorf("error %q does not name %s", err, v)
				}
			}
		})
	}
}

func TestCheckNotInGitTree(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, root string) string
		wantErr bool
	}{
		{
			name:  "outside any repository",
			setup: func(t *testing.T, root string) string { return filepath.Join(root, "recordings") },
		},
		{
			name: "existing directory in a repository",
			setup: func(t *testing.T, root string) string {
				mkdir(t, filepath.Join(root, "repo", ".git"))
				d := filepath.Join(root, "repo", "data")
				mkdir(t, d)
				return d
			},
			wantErr: true,
		},
		{
			name: "missing directory deep in a repository",
			setup: func(t *testing.T, root string) string {
				mkdir(t, filepath.Join(root, "repo", ".git"))
				return filepath.Join(root, "repo", "a", "b", "recordings")
			},
			wantErr: true,
		},
		{
			name: "worktree with a .git file",
			setup: func(t *testing.T, root string) string {
				mkdir(t, filepath.Join(root, "wt"))
				if err := os.WriteFile(filepath.Join(root, "wt", ".git"), []byte("gitdir: /nowhere\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				return filepath.Join(root, "wt", "recordings")
			},
			wantErr: true,
		},
		{
			name: "symbolic link into a repository",
			setup: func(t *testing.T, root string) string {
				mkdir(t, filepath.Join(root, "repo", ".git"))
				mkdir(t, filepath.Join(root, "repo", "data"))
				link := filepath.Join(root, "link")
				if err := os.Symlink(filepath.Join(root, "repo", "data"), link); err != nil {
					t.Skipf("symlink not supported: %v", err)
				}
				return filepath.Join(link, "recordings")
			},
			wantErr: true,
		},
		{
			name: "sibling of a repository",
			setup: func(t *testing.T, root string) string {
				mkdir(t, filepath.Join(root, "repo", ".git"))
				return filepath.Join(root, "recordings")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := tt.setup(t, t.TempDir())
			err := CheckNotInGitTree(dir)
			if tt.wantErr {
				if !errors.Is(err, ErrInsideGitTree) {
					t.Fatalf("CheckNotInGitTree() error = %v, want ErrInsideGitTree", err)
				}
				if !strings.Contains(err.Error(), "real post text") {
					t.Errorf("error does not say why: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("CheckNotInGitTree() error = %v", err)
			}
		})
	}
}

func TestOpenWriterRefusesGitTree(t *testing.T) {
	root := t.TempDir()
	mkdir(t, filepath.Join(root, ".git"))
	dir := filepath.Join(root, "recordings")
	_, err := OpenWriter(WriterOptions{Dir: dir}, discardLogger(), nil)
	if !errors.Is(err, ErrInsideGitTree) {
		t.Fatalf("OpenWriter() error = %v, want ErrInsideGitTree", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("directory was created inside the repository: %v", err)
	}
}

func mkdir(t *testing.T, d string) {
	t.Helper()
	if err := os.MkdirAll(d, 0o700); err != nil {
		t.Fatal(err)
	}
}
