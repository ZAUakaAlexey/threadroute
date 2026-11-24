package gwapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// fixture is one entry of testdata/index.json or of
// testdata/settings/index.json. The console tests read the same index, so
// both sides check the same examples.
//
// testdata/settings holds the examples of decision 0014 (stand settings and
// trends) in its own index, so that the tests of the gateway and of the
// console that read testdata/index.json keep passing until they handle the
// new messages. Its entries have MayChangeSettings, the right of the
// audience.
type fixture struct {
	File              string `json:"file"`
	Direction         string `json:"direction"`
	Type              string `json:"type"`
	Level             Level  `json:"level"`
	View              View   `json:"view"`
	MayChangeSettings bool   `json:"may_change_settings"`
	Purpose           string `json:"purpose"`

	// dir is the directory of the index; it is not in the JSON.
	dir string
}

// Fixture directories.
const (
	fixtureDir         = "testdata"
	settingsFixtureDir = "testdata/settings"
)

func (f fixture) audience() Audience {
	return Audience{Level: f.Level, View: f.View, MayChangeSettings: f.MayChangeSettings}
}

func (f fixture) read(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.dir, f.File))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// loadIndex returns the entries of testdata/index.json.
func loadIndex(t *testing.T) []fixture { return loadIndexIn(t, fixtureDir) }

func loadIndexIn(t *testing.T, dir string) []fixture {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var idx []fixture
	if err := json.Unmarshal(b, &idx); err != nil {
		t.Fatalf("%s/index.json: %v", dir, err)
	}
	for i := range idx {
		idx[i].dir = dir
	}
	return idx
}

// allFixtures returns the entries of both indexes.
func allFixtures(t *testing.T) []fixture {
	t.Helper()
	return append(loadIndex(t), loadIndexIn(t, settingsFixtureDir)...)
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// sameJSON reports whether a and b hold the same JSON value.
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		t.Fatalf("unmarshal %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	return reflect.DeepEqual(va, vb)
}

func TestFixturesAreIndexed(t *testing.T) {
	var listed []string
	for _, dir := range []string{fixtureDir, settingsFixtureDir} {
		files, err := filepath.Glob(filepath.Join(dir, "*.json"))
		if err != nil {
			t.Fatal(err)
		}
		var onDisk, inDir []string
		for _, f := range files {
			if base := filepath.Base(f); base != "index.json" {
				onDisk = append(onDisk, base)
			}
		}
		for _, f := range loadIndexIn(t, dir) {
			inDir = append(inDir, f.File)
			if f.Purpose == "" || !strings.HasPrefix(f.File, f.Direction+"_") {
				t.Errorf("%s: purpose is empty or the name does not start with %q", f.File, f.Direction+"_")
			}
			if f.MayChangeSettings && dir == fixtureDir {
				t.Errorf("%s: may_change_settings belongs in %s", f.File, settingsFixtureDir)
			}
		}
		slices.Sort(inDir)
		if !slices.Equal(onDisk, inDir) {
			t.Errorf("files on disk %v, listed in %s/index.json %v", onDisk, dir, inDir)
		}
		listed = append(listed, inDir...)
	}
	idx := allFixtures(t)

	// Every message type has at least one example.
	have := map[string]bool{}
	for _, f := range idx {
		have[f.Direction+":"+f.Type] = true
	}
	var want []string
	for _, c := range []ClientType{TypeHello, TypeWatch, TypeUnwatch, TypeAction, TypeChangeSettings} {
		want = append(want, "client:"+string(c))
	}
	for _, s := range []ServerType{TypeWelcome, TypeStatus, TypeState, TypeMessages, TypeMessageRemoved, TypeResult,
		TypeSettings, TypeTrend} {
		want = append(want, "server:"+string(s))
	}
	want = append(want, "public:public_snapshot")
	for _, w := range want {
		if !have[w] {
			t.Errorf("no fixture for %s", w)
		}
	}
	for _, a := range Actions {
		if !slices.Contains(listed, "client_action_"+string(a)+".json") {
			t.Errorf("no fixture for action %s", a)
		}
	}
}

// TestFixturesRoundTrip decodes every fixture, encodes it again, and
// compares the JSON values.
func TestFixturesRoundTrip(t *testing.T) {
	for _, f := range allFixtures(t) {
		t.Run(f.File, func(t *testing.T) {
			raw := f.read(t)
			var out []byte
			switch f.Direction {
			case "client":
				m, err := DecodeClient(compact(t, raw))
				if err != nil {
					t.Fatalf("DecodeClient() error = %v", err)
				}
				if string(m.ClientType()) != f.Type {
					t.Fatalf("type = %s, want %s", m.ClientType(), f.Type)
				}
				if out, err = EncodeClient(m); err != nil {
					t.Fatalf("EncodeClient() error = %v", err)
				}
			case "server":
				a := f.audience()
				m, err := DecodeServer(raw, a)
				if err != nil {
					t.Fatalf("DecodeServer() error = %v", err)
				}
				if string(m.ServerType()) != f.Type {
					t.Fatalf("type = %s, want %s", m.ServerType(), f.Type)
				}
				if out, err = EncodeServer(m, a); err != nil {
					t.Fatalf("EncodeServer() error = %v", err)
				}
				if !bytes.HasPrefix(out, []byte(`{"type":"`+f.Type+`"`)) {
					t.Errorf("encoded message does not start with its type: %s", out)
				}
			case "public":
				p, err := DecodePublic(raw)
				if err != nil {
					t.Fatalf("DecodePublic() error = %v", err)
				}
				if out, err = EncodePublic(p); err != nil {
					t.Fatalf("EncodePublic() error = %v", err)
				}
			default:
				t.Fatalf("unknown direction %q", f.Direction)
			}
			if !sameJSON(t, raw, out) {
				t.Errorf("round trip:\n got %s\nwant %s", out, compact(t, raw))
			}
		})
	}
}

// TestFixturesDemoLevel checks the privacy rules on the examples: a demo
// fixture holds no text and no ID of a person or a company, and a full
// fixture that holds them is refused for a demo audience.
func TestFixturesDemoLevel(t *testing.T) {
	for _, f := range allFixtures(t) {
		if f.Direction == "client" {
			continue
		}
		raw := f.read(t)
		if f.Level != LevelFull {
			for _, s := range []string{"did:", `"text"`, "synthetic customer", "synthetic company"} {
				if bytes.Contains(raw, []byte(s)) {
					t.Errorf("%s holds %q", f.File, s)
				}
			}
		}
		if f.Level == LevelFull && f.Direction == "server" && f.Type != string(TypeWelcome) {
			demo := Audience{Level: LevelDemo, View: f.View, MayChangeSettings: f.MayChangeSettings}
			if _, err := DecodeServer(raw, demo); err == nil {
				t.Errorf("%s: DecodeServer() for a demo audience error = nil, want an error", f.File)
			}
		}
	}
}

func compact(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestDecodeClientErrors(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		target  error
		fields  []string
		noToken bool
	}{
		{name: "not JSON", in: `hello`},
		{name: "no type", in: `{"version":1}`, fields: []string{"type"}},
		{name: "unknown type", in: `{"type":"subscribe"}`, target: ErrUnknownType},
		{name: "server type", in: `{"type":"welcome","version":1}`, target: ErrUnknownType},
		{name: "older version", in: `{"type":"hello","version":0,"view":"operator"}`, target: ErrUnsupportedVersion},
		{name: "newer version with unknown fields", in: `{"type":"hello","version":2,"screen":"x"}`, target: ErrUnsupportedVersion},
		{name: "hello without view", in: `{"type":"hello","version":1}`, fields: []string{"view"}},
		{name: "hello with unknown view", in: `{"type":"hello","version":1,"view":"admin"}`, fields: []string{"view"}},
		{name: "operator without languages", in: `{"type":"hello","version":1,"view":"operator"}`, fields: []string{"languages"}},
		{name: "operator with bad languages", in: `{"type":"hello","version":1,"view":"operator","languages":["en","EN","en"]}`,
			fields: []string{`"EN"`, "listed twice"}},
		{name: "supervisor with languages", in: `{"type":"hello","version":1,"view":"supervisor","languages":["en"]}`, fields: []string{"languages"}},
		{name: "supervisor with resume token", in: `{"type":"hello","version":1,"view":"supervisor","resume_token":"abc"}`, fields: []string{"resume_token"}},
		{name: "bad resume token", in: `{"type":"hello","version":1,"view":"operator","languages":["en"],"resume_token":"a b"}`, fields: []string{"resume_token"}},
		{name: "token too long", in: `{"type":"hello","version":1,"view":"supervisor","token":"` + strings.Repeat("t", MaxTokenBytes+1) + `"}`,
			fields: []string{"token"}, noToken: true},
		{name: "watch without IDs", in: `{"type":"watch"}`, fields: []string{"request_id", "conversation_id"}},
		{name: "unwatch with bad request ID", in: `{"type":"unwatch","request_id":"a/b","conversation_id":"x"}`, fields: []string{"request_id"}},
		{name: "action without name", in: `{"type":"action","request_id":"r1"}`, fields: []string{"action"}},
		{name: "unknown action", in: `{"type":"action","request_id":"r1","action":"transfer"}`, fields: []string{"action"}},
		{name: "accept without offer", in: `{"type":"action","request_id":"r1","action":"accept"}`, fields: []string{"offer_id"}},
		{name: "resolve with bad offer", in: `{"type":"action","request_id":"r1","action":"resolve","offer_id":"of 1"}`, fields: []string{"offer_id"}},
		{name: "online with offer", in: `{"type":"action","request_id":"r1","action":"online","offer_id":"of-1"}`, fields: []string{"offer_id"}},
		{name: "set languages without languages", in: `{"type":"action","request_id":"r1","action":"set_languages"}`, fields: []string{"languages"}},
		{name: "accept with languages", in: `{"type":"action","request_id":"r1","action":"accept","offer_id":"of-1","languages":["en"]}`,
			fields: []string{"languages"}},
		{name: "too big", in: `{"type":"action","request_id":"r1","action":"online","pad":"` + strings.Repeat("x", MaxClientMessageBytes) + `"}`,
			fields: []string{"bytes"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeClient([]byte(tt.in))
			if err == nil {
				t.Fatal("DecodeClient() error = nil, want an error")
			}
			if tt.target != nil && !errors.Is(err, tt.target) {
				t.Errorf("DecodeClient() error = %v, want %v", err, tt.target)
			}
			for _, f := range tt.fields {
				if !strings.Contains(err.Error(), f) {
					t.Errorf("error %q does not name %s", err, f)
				}
			}
			if tt.noToken && strings.Contains(err.Error(), "ttttt") {
				t.Errorf("error %q contains the token", err)
			}
		})
	}
}

func TestDecodeClientKeepsUnknownFieldsOut(t *testing.T) {
	m, err := DecodeClient([]byte(`{"type":"action","request_id":"r1","action":"online","future":true}`))
	if err != nil {
		t.Fatalf("DecodeClient() error = %v", err)
	}
	if a, ok := m.(Action); !ok || a.Action != ActionOnline || a.RequestID != "r1" {
		t.Errorf("DecodeClient() = %#v", m)
	}
}

func TestDecodeServerUnknownType(t *testing.T) {
	_, err := DecodeServer([]byte(`{"type":"typing"}`), Audience{Level: LevelDemo, View: ViewOperator})
	if !errors.Is(err, ErrUnknownType) {
		t.Errorf("DecodeServer() error = %v, want ErrUnknownType", err)
	}
}
