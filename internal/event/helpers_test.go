package event

import (
	"strings"
	"testing"
	"time"
)

// Synthetic, clearly fake test data (decision 0004).
const (
	testCompany  = "did:plc:testalice0000000000000000"
	testCustomer = "did:plc:testbob000000000000000000"
	testEventID  = "c:did:plc:testbob000000000000000000:app.bsky.feed.post:aaa111:3testrev00001:create"
	testMsgID    = "at://did:plc:testbob000000000000000000/app.bsky.feed.post/aaa111"
	testParentID = "at://did:plc:testalice0000000000000000/app.bsky.feed.post/bbb222"
	testText     = "synthetic reply one"
)

// testTime is a fixed source time with microseconds.
var testTime = time.UnixMicro(1_767_323_045_123_456).UTC()

// assertFieldErrors checks that err is not nil, that it names every field
// in fields, and that it never contains the post text.
func assertFieldErrors(t *testing.T, err error, fields ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want errors for %v", fields)
	}
	msg := err.Error()
	for _, f := range fields {
		if !strings.Contains(msg, f) {
			t.Errorf("error %q does not name %s", msg, f)
		}
	}
	if strings.Contains(msg, testText) {
		t.Errorf("error %q contains the post text", msg)
	}
}
