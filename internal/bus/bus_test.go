package bus

import (
	"math"
	"strconv"
	"testing"
	"time"
)

func TestRawLimitsPerPartitionBytes(t *testing.T) {
	const mib = 1 << 20
	tests := []struct {
		name        string
		maxBytes    int64
		wantSegment int64
		wantBytes   int64
	}{
		{name: "default", maxBytes: DefaultRawMaxBytes, wantSegment: 64 * mib, wantBytes: DefaultRawMaxBytes/6 - 64*mib},
		{name: "segment at the cap", maxBytes: 6 * 256 * mib, wantSegment: 64 * mib, wantBytes: 192 * mib},
		{name: "segment below the cap", maxBytes: 6 * 40 * mib, wantSegment: 10 * mib, wantBytes: 30 * mib},
		{name: "share rounds down", maxBytes: 6*40*mib + 5, wantSegment: 10 * mib, wantBytes: 30 * mib},
		{name: "segment at the floor", maxBytes: 6 * 3 * mib, wantSegment: mib, wantBytes: 2 * mib},
		{name: "smallest valid limit", maxBytes: RawMinMaxBytes, wantSegment: mib, wantBytes: mib},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := RawLimits{Retention: DefaultRawRetention, MaxBytes: tt.maxBytes}
			if err := l.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			seg := l.SegmentBytes()
			got := l.PerPartitionBytes()
			if seg != tt.wantSegment {
				t.Errorf("SegmentBytes() = %d, want %d", seg, tt.wantSegment)
			}
			if got != tt.wantBytes {
				t.Errorf("PerPartitionBytes() = %d, want %d", got, tt.wantBytes)
			}
			if (got+seg)*int64(PartitionsRaw) > tt.maxBytes {
				t.Errorf("(%d + %d) bytes x %d partitions exceeds the limit %d", got, seg, PartitionsRaw, tt.maxBytes)
			}
			if got < seg {
				t.Errorf("retention.bytes %d is smaller than one segment %d", got, seg)
			}
			if seg < RawSegmentBytesMin || seg > RawSegmentBytesMax || seg > math.MaxInt32 {
				t.Errorf("SegmentBytes() = %d is out of range", seg)
			}
		})
	}
}

func TestRawLimitsValidate(t *testing.T) {
	tests := []struct {
		name    string
		limits  RawLimits
		wantErr bool
	}{
		{name: "default", limits: DefaultRawLimits()},
		{name: "retention equals segment", limits: RawLimits{Retention: RawSegmentInterval, MaxBytes: 1 << 30}},
		{name: "retention too short", limits: RawLimits{Retention: 30 * time.Minute, MaxBytes: 1 << 30}, wantErr: true},
		{name: "retention not whole ms", limits: RawLimits{Retention: 2*time.Hour + time.Microsecond, MaxBytes: 1 << 30}, wantErr: true},
		{name: "zero bytes", limits: RawLimits{Retention: DefaultRawRetention}, wantErr: true},
		{name: "less than one byte per partition", limits: RawLimits{Retention: DefaultRawRetention, MaxBytes: 5}, wantErr: true},
		{name: "smallest valid bytes", limits: RawLimits{Retention: DefaultRawRetention, MaxBytes: RawMinMaxBytes}},
		{name: "one byte below the smallest", limits: RawLimits{Retention: DefaultRawRetention, MaxBytes: RawMinMaxBytes - 1}, wantErr: true},
		{name: "too small for two segments", limits: RawLimits{Retention: DefaultRawRetention, MaxBytes: 600_000}, wantErr: true},
		{name: "negative bytes", limits: RawLimits{Retention: DefaultRawRetention, MaxBytes: -1}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.limits.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRawTopicSpec(t *testing.T) {
	spec, err := RawTopicSpec(DefaultRawLimits())
	if err != nil {
		t.Fatalf("RawTopicSpec() error = %v", err)
	}
	if spec.Name != TopicRaw || spec.Partitions != 6 || spec.ReplicationFactor != 1 {
		t.Errorf("spec = %+v", spec)
	}
	want := map[string]string{
		ConfigCleanupPolicy:        "delete",
		ConfigRetentionMs:          "86400000",
		ConfigRetentionBytes:       strconv.FormatInt(8_000_000_000/6-64<<20, 10),
		ConfigSegmentMs:            "3600000",
		ConfigSegmentBytes:         strconv.FormatInt(64<<20, 10),
		ConfigMessageTimestampType: "LogAppendTime",
	}
	assertConfigs(t, spec.Configs, want)

	if _, err := RawTopicSpec(RawLimits{}); err == nil {
		t.Error("RawTopicSpec(zero limits) error = nil, want error")
	}
}

func TestOtherTopicSpecs(t *testing.T) {
	tests := []struct {
		name       string
		spec       TopicSpec
		topic      string
		partitions int32
		configs    map[string]string
	}{
		{name: "events", spec: EventsTopicSpec(), topic: "events", partitions: 16, configs: map[string]string{
			ConfigCleanupPolicy:        "delete",
			ConfigRetentionMs:          "21600000",
			ConfigSegmentMs:            "3600000",
			ConfigMessageTimestampType: "LogAppendTime",
		}},
		{name: "control", spec: ControlTopicSpec(), topic: "events.control", partitions: 6, configs: map[string]string{
			ConfigCleanupPolicy:        "delete",
			ConfigRetentionMs:          "21600000",
			ConfigSegmentMs:            "3600000",
			ConfigMessageTimestampType: "LogAppendTime",
		}},
		{name: "conversations", spec: ConversationsTopicSpec(), topic: "conversations", partitions: 16, configs: map[string]string{
			ConfigCleanupPolicy:        "delete",
			ConfigRetentionMs:          "86400000",
			ConfigSegmentMs:            "3600000",
			ConfigMessageTimestampType: "LogAppendTime",
		}},
		{name: "cursor", spec: CursorTopicSpec(), topic: "ingest.cursor", partitions: 1, configs: map[string]string{
			ConfigCleanupPolicy:          "compact",
			ConfigSegmentMs:              "600000",
			ConfigSegmentBytes:           "1048576",
			ConfigMinCleanableDirtyRatio: "0.01",
			ConfigMinCompactionLagMs:     "0",
			ConfigMaxCompactionLagMs:     "900000",
			ConfigDeleteRetentionMs:      "3600000",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.spec.Name != tt.topic || tt.spec.Partitions != tt.partitions || tt.spec.ReplicationFactor != 1 {
				t.Errorf("spec = %+v", tt.spec)
			}
			assertConfigs(t, tt.spec.Configs, tt.configs)
		})
	}
}

func assertConfigs(t *testing.T, got map[string]*string, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("got %d configs, want %d", len(got), len(want))
	}
	for k, v := range want {
		p, ok := got[k]
		if !ok || p == nil {
			t.Errorf("config %s missing", k)
			continue
		}
		if *p != v {
			t.Errorf("config %s = %q, want %q", k, *p, v)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	in := CursorRecord{
		Host:      "jetstream-east.example.test",
		Witnessed: time.UnixMicro(1_700_000_000_123_456).UTC(),
		Seq:       987654,
		WrittenAt: time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC),
	}
	b, err := MarshalCursor(in)
	if err != nil {
		t.Fatalf("MarshalCursor() error = %v", err)
	}
	out, err := UnmarshalCursor(b)
	if err != nil {
		t.Fatalf("UnmarshalCursor() error = %v", err)
	}
	if out.Host != in.Host || out.Seq != in.Seq || !out.Witnessed.Equal(in.Witnessed) || !out.WrittenAt.Equal(in.WrittenAt) {
		t.Errorf("round trip: got %+v, want %+v", out, in)
	}
}

func TestMarshalCursorFormat(t *testing.T) {
	b, err := MarshalCursor(CursorRecord{
		Host:      "jetstream-east.example.test",
		Witnessed: time.UnixMicro(1_700_000_000_000_001),
		Seq:       5,
		WrittenAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("MarshalCursor() error = %v", err)
	}
	want := `{"host":"jetstream-east.example.test","witnessed_us":1700000000000001,"seq":5,"written_at":"2026-01-02T03:04:05Z"}`
	if string(b) != want {
		t.Errorf("MarshalCursor() = %s, want %s", b, want)
	}
}

func TestMarshalCursorInvalid(t *testing.T) {
	valid := CursorRecord{
		Host:      "jetstream-east.example.test",
		Witnessed: time.UnixMicro(1_700_000_000_000_000),
		WrittenAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	tests := []struct {
		name   string
		change func(r *CursorRecord)
	}{
		{name: "no witnessed time", change: func(r *CursorRecord) { r.Witnessed = time.Time{} }},
		{name: "witnessed at unix zero", change: func(r *CursorRecord) { r.Witnessed = time.UnixMicro(0) }},
		{name: "no write time", change: func(r *CursorRecord) { r.WrittenAt = time.Time{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := valid
			tt.change(&r)
			if _, err := MarshalCursor(r); err == nil {
				t.Error("MarshalCursor() error = nil, want error")
			}
		})
	}
}

func TestUnmarshalCursorMalformed(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty tombstone", value: ""},
		{name: "not json", value: "not json"},
		{name: "truncated", value: `{"host":"jetstream-east.example.test","witnessed_us":17`},
		{name: "wrong type", value: `{"host":"h","witnessed_us":"soon","seq":1,"written_at":"2026-01-02T03:04:05Z"}`},
		{name: "negative seq", value: `{"host":"h","witnessed_us":1700000000000000,"seq":-1,"written_at":"2026-01-02T03:04:05Z"}`},
		{name: "missing witnessed", value: `{"host":"h","seq":1,"written_at":"2026-01-02T03:04:05Z"}`},
		{name: "missing written_at", value: `{"host":"h","witnessed_us":1700000000000000,"seq":1}`},
		{name: "json null", value: `null`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := UnmarshalCursor([]byte(tt.value)); err == nil {
				t.Error("UnmarshalCursor() error = nil, want error")
			}
		})
	}
}

func TestCursorKey(t *testing.T) {
	if got := string(CursorKey(SourceBluesky)); got != "bluesky" {
		t.Errorf("CursorKey() = %q, want %q", got, "bluesky")
	}
}
