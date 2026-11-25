package opapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Synthetic IDs.
const (
	testOperator = "sim-op-0001"
	testOffer    = "offer-000001"
)

// TestPatternsRegister registers every pattern on one ServeMux. ServeMux
// panics on an invalid pattern or on two patterns that conflict.
func TestPatternsRegister(t *testing.T) {
	mux := http.NewServeMux()
	for _, p := range allPatterns() {
		mux.HandleFunc(p, func(http.ResponseWriter, *http.Request) {})
	}
	if len(Patterns) != 8 || len(SettingsPatterns) != 1 {
		t.Errorf("len(Patterns) = %d, len(SettingsPatterns) = %d, want 8 and 1", len(Patterns), len(SettingsPatterns))
	}
}

func allPatterns() []string {
	return append(append([]string{}, Patterns...), SettingsPatterns...)
}

// TestPathsMatchPatterns checks that each path builder gives a path that
// the mux routes to its own pattern, with the wildcards set to the IDs.
func TestPathsMatchPatterns(t *testing.T) {
	mux := http.NewServeMux()
	for _, p := range allPatterns() {
		mux.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "%s|%s|%s", r.Pattern, r.PathValue(ParamOperatorID), r.PathValue(ParamOfferID))
		})
	}

	tests := []struct {
		method   string
		path     string
		pattern  string
		operator string
		offer    string
	}{
		{http.MethodPut, OperatorPath(testOperator), PatternPutOperator, testOperator, ""},
		{http.MethodPost, OnlinePath(testOperator), PatternOnline, testOperator, ""},
		{http.MethodPost, OfflinePath(testOperator), PatternOffline, testOperator, ""},
		{http.MethodPost, AcceptPath(testOperator, testOffer), PatternAccept, testOperator, testOffer},
		{http.MethodPost, RefusePath(testOperator, testOffer), PatternRefuse, testOperator, testOffer},
		{http.MethodPost, ReplyPath(testOperator, testOffer), PatternReply, testOperator, testOffer},
		{http.MethodPost, ResolvePath(testOperator, testOffer), PatternResolve, testOperator, testOffer},
		{http.MethodGet, SettingsPath(), PatternSettings, "", ""},
		{http.MethodGet, SettingsWithChangesPath(), PatternSettings, "", ""},
		{http.MethodPut, SettingsPath(), PatternPutSettings, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))
			want := tt.pattern + "|" + tt.operator + "|" + tt.offer
			if rec.Code != http.StatusOK || rec.Body.String() != want {
				t.Errorf("%s %s: status %d body %q, want %q", tt.method, tt.path, rec.Code, rec.Body.String(), want)
			}
		})
	}

	t.Run("wrong method", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, AcceptPath(testOperator, testOffer), nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("GET accept: status %d, want 405", rec.Code)
		}
	})
}

func TestValidID(t *testing.T) {
	for _, s := range []string{testOperator, testOffer, "42"} {
		if !ValidID(s) {
			t.Errorf("ValidID(%q) = false", s)
		}
	}
	for _, s := range []string{"", "a/b", "a b", "did:plc:x", strings.Repeat("a", 65)} {
		if ValidID(s) {
			t.Errorf("ValidID(%q) = true", s)
		}
	}
}

func TestOperatorRequestValidate(t *testing.T) {
	tests := []struct {
		name string
		in   OperatorRequest
		errs []string
	}{
		{name: "valid", in: OperatorRequest{Languages: []string{"en", "und"}, Capacity: 4}},
		{name: "default capacity", in: OperatorRequest{Languages: []string{"ja"}}},
		{name: "max capacity", in: OperatorRequest{Languages: []string{"ja"}, Capacity: MaxCapacity}},
		{name: "empty", in: OperatorRequest{}, errs: []string{"languages is not set"}},
		{name: "bad and repeated languages", in: OperatorRequest{Languages: []string{"en", "EN", "en-US", "en"}},
			errs: []string{`"EN"`, `"en-US"`, "listed twice"}},
		{name: "too many languages", in: OperatorRequest{Languages: manyLangs(MaxLanguages + 1)}, errs: []string{"more than"}},
		{name: "negative capacity", in: OperatorRequest{Languages: []string{"en"}, Capacity: -1}, errs: []string{"capacity"}},
		{name: "capacity too large", in: OperatorRequest{Languages: []string{"en"}, Capacity: MaxCapacity + 1}, errs: []string{"capacity"}},
		{name: "priority 0", in: OperatorRequest{Languages: []string{"en"}, Priority: 0}},
		{name: "priority 9", in: OperatorRequest{Languages: []string{"en"}, Priority: MaxPriority}},
		{name: "priority 10", in: OperatorRequest{Languages: []string{"en"}, Priority: MaxPriority + 1},
			errs: []string{"priority: 10 is not between 0 and 9"}},
		{name: "negative priority", in: OperatorRequest{Languages: []string{"en"}, Priority: -1}, errs: []string{"priority"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertErrors(t, tt.in.Validate(), tt.errs...)
		})
	}
}

// TestOperatorRequestPriorityJSON decodes request bodies as a caller sends
// them: a body without the field (the simulator, and every caller from
// before the field) has priority 0, like an explicit 0.
func TestOperatorRequestPriorityJSON(t *testing.T) {
	tests := []struct {
		body    string
		want    int
		invalid bool
	}{
		{body: `{"languages":["en"],"capacity":4}`, want: 0},
		{body: `{"languages":["en"],"capacity":4,"priority":0}`, want: 0},
		{body: `{"languages":["en"],"priority":9}`, want: 9},
		{body: `{"languages":["en"],"priority":10}`, want: 10, invalid: true},
	}
	for _, tt := range tests {
		var r OperatorRequest
		if err := json.Unmarshal([]byte(tt.body), &r); err != nil {
			t.Fatalf("%s: %v", tt.body, err)
		}
		if r.Priority != tt.want {
			t.Errorf("%s: priority = %d, want %d", tt.body, r.Priority, tt.want)
		}
		if err := r.Validate(); (err != nil) != tt.invalid {
			t.Errorf("%s: Validate() = %v, want invalid %v", tt.body, err, tt.invalid)
		}
	}
}

func manyLangs(n int) []string {
	out := make([]string, 0, n)
	for i := range n {
		out = append(out, string([]byte{'a' + byte(i/26%26), 'a' + byte(i%26)}))
	}
	return out
}

func TestReplyRequestValidate(t *testing.T) {
	tests := []struct {
		name string
		in   ReplyRequest
		errs []string
	}{
		{name: "valid", in: ReplyRequest{ReplyID: "r-1"}},
		{name: "empty", in: ReplyRequest{}, errs: []string{"reply_id is not set"}},
		{name: "bad characters", in: ReplyRequest{ReplyID: "r/1"}, errs: []string{"reply_id"}},
		{name: "too long", in: ReplyRequest{ReplyID: strings.Repeat("r", MaxReplyIDLen+1)}, errs: []string{"reply_id"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertErrors(t, tt.in.Validate(), tt.errs...)
		})
	}
}

func TestSettings(t *testing.T) {
	valid := Settings{SpeedFactor: 50, AcceptTimeoutUS: 20_000_000, CapacityDefault: 4}
	if err := valid.Validate(); err != nil {
		t.Errorf("Validate() error = %v", err)
	}
	if got := valid.AcceptTimeout(); got != 20*time.Second {
		t.Errorf("AcceptTimeout() = %s, want 20s", got)
	}
	b, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"speed_factor":50,"accept_timeout_us":20000000,"capacity_default":4}`; string(b) != want {
		t.Errorf("json = %s, want %s", b, want)
	}

	withTargets := valid
	withTargets.AssignmentTargetUS, withTargets.FirstResponseTargetUS = 60_000_000, 120_000_000
	if err := withTargets.Validate(); err != nil {
		t.Errorf("Validate() with targets error = %v", err)
	}
	b, err = json.Marshal(withTargets)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"speed_factor":50,"accept_timeout_us":20000000,"capacity_default":4,` +
		`"assignment_target_us":60000000,"first_response_target_us":120000000}`; string(b) != want {
		t.Errorf("json = %s, want %s", b, want)
	}

	tests := []struct {
		name string
		in   Settings
		errs []string
	}{
		{name: "zero", in: Settings{}, errs: []string{"speed_factor", "accept_timeout_us", "capacity_default"}},
		{name: "negative speed", in: Settings{SpeedFactor: -1, AcceptTimeoutUS: 1, CapacityDefault: 1}, errs: []string{"speed_factor"}},
		{name: "capacity too large", in: Settings{SpeedFactor: 1, AcceptTimeoutUS: 1, CapacityDefault: MaxCapacity + 1}, errs: []string{"capacity_default"}},
		{name: "negative targets", in: Settings{SpeedFactor: 1, AcceptTimeoutUS: 1, CapacityDefault: 1,
			AssignmentTargetUS: -1, FirstResponseTargetUS: -1}, errs: []string{"assignment_target_us", "first_response_target_us"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertErrors(t, tt.in.Validate(), tt.errs...)
		})
	}
}

func TestErrorCodes(t *testing.T) {
	want := map[ErrorCode]int{
		CodeInvalidRequest:   400,
		CodeOperatorNotFound: 404,
		CodeOfferNotFound:    404,
		CodeOfferExpired:     409,
		CodeOfferRefused:     409,
		CodeOfferAccepted:    409,
		CodeOfferEnded:       409,
		CodeOfferNotAccepted: 409,
		CodeAssignmentEnded:  409,
		CodeUnavailable:      503,
		CodeInternal:         500,
		CodeSettingsConflict: 409,
		CodeSettingsTooSoon:  429,
		CodeSettingsOff:      501,
	}
	if len(Codes) != len(want) {
		t.Errorf("len(Codes) = %d, want %d", len(Codes), len(want))
	}
	seen := map[ErrorCode]bool{}
	for _, c := range Codes {
		if seen[c] {
			t.Errorf("code %q listed twice", c)
		}
		seen[c] = true
		if got := c.Status(); got != want[c] {
			t.Errorf("%s.Status() = %d, want %d", c, got, want[c])
		}
		if got := c.Rejected(); got != (want[c] == 409) {
			t.Errorf("%s.Rejected() = %v", c, got)
		}
	}
	if got := ErrorCode("new_code").Status(); got != 500 {
		t.Errorf("unknown code status = %d, want 500", got)
	}
}

func TestErrorFormat(t *testing.T) {
	resp := ErrorResponse{Code: CodeOfferExpired, Message: "the offer expired"}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"code":"offer_expired","message":"the offer expired"}`; string(b) != want {
		t.Errorf("json = %s, want %s", b, want)
	}

	err2 := fmt.Errorf("accept: %w", &Error{Status: 409, ErrorResponse: resp})
	var apiErr *Error
	if !errors.As(err2, &apiErr) || !apiErr.Code.Rejected() {
		t.Fatalf("errors.As(%v) failed", err2)
	}
	if got := err2.Error(); got != "accept: operator API: 409 offer_expired: the offer expired" {
		t.Errorf("Error() = %q", got)
	}
}

func TestBodiesFormat(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{name: "operator request", in: OperatorRequest{Languages: []string{"en", "und"}, Capacity: 4},
			want: `{"languages":["en","und"],"capacity":4}`},
		{name: "operator request with priority", in: OperatorRequest{Languages: []string{"en", "und"}, Capacity: 2, Priority: 1},
			want: `{"languages":["en","und"],"capacity":2,"priority":1}`},
		{name: "operator response", in: OperatorResponse{Result: ResultDone, Operator: Operator{
			ID: testOperator, Languages: []string{"en", "und"}, Capacity: 4, Presence: PresenceOffline,
		}}, want: `{"result":"done","operator":{"id":"sim-op-0001","languages":["en","und"],"capacity":4,"priority":0,"presence":"offline"}}`},
		{name: "operator response with priority", in: OperatorResponse{Result: ResultDone, Operator: Operator{
			ID: testOperator, Languages: []string{"en"}, Capacity: 2, Priority: 1, Presence: PresenceOnline,
		}}, want: `{"result":"done","operator":{"id":"sim-op-0001","languages":["en"],"capacity":2,"priority":1,"presence":"online"}}`},
		{name: "action", in: ActionResponse{Result: ResultAlreadyDone}, want: `{"result":"already_done"}`},
		{name: "reply", in: ReplyRequest{ReplyID: "r-1"}, want: `{"reply_id":"r-1"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != tt.want {
				t.Errorf("json = %s, want %s", b, tt.want)
			}
			if len(b) > MaxBodyBytes {
				t.Errorf("body of %d bytes is larger than MaxBodyBytes", len(b))
			}
		})
	}
	if !PresenceOnline.Valid() || !PresenceOffline.Valid() || Presence("away").Valid() {
		t.Error("Presence.Valid() is wrong")
	}
}

// assertErrors checks that err is nil when want is empty, and otherwise
// that it contains every string in want.
func assertErrors(t *testing.T, err error, want ...string) {
	t.Helper()
	if len(want) == 0 {
		if err != nil {
			t.Errorf("error = %v, want nil", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("error = nil, want %v", want)
	}
	for _, w := range want {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("error %q does not contain %q", err, w)
		}
	}
}
