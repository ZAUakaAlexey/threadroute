package conv

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

func TestDeletionAfterMessage(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute),
	)
	r := env.control(t, deletion("test:a1", testAlice, 3*time.Minute))[0]
	if r.Outcome != OutcomeRemoved {
		t.Fatalf("outcome = %q, want removed", r.Outcome)
	}
	if env.hasText(t, "test:a1") {
		t.Error("text still stored")
	}
	if ids := env.messageIDs(t, testCompany, testAlice); len(ids) != 1 || ids[0] != "test:a2" {
		t.Errorf("messages = %v, want only test:a2", ids)
	}
	members, _ := env.rdb.SMembers(testCtx(t), DefaultKeyPrefix+"au:"+testAlice).Result()
	if len(members) != 1 || members[0] != "test:a2" {
		t.Errorf("author index = %v, want only test:a2", members)
	}
	if c := env.conv(t, testCompany, testAlice); c["nc"] != "2" {
		t.Errorf("counter = %s, want 2: a deletion does not lower counters", c["nc"])
	}
}

func TestDeletionBeforeMessage(t *testing.T) {
	tests := []struct {
		name     string
		moveWM   time.Duration // a later message of another conversation moves the watermark here first
		wantText bool
	}{
		{name: "message within 10 minutes", moveWM: 5 * time.Minute, wantText: false},
		{name: "message at exactly 10 minutes", moveWM: 13 * time.Minute, wantText: false},
		{name: "message after 10 minutes", moveWM: 13*time.Minute + time.Second, wantText: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(t, testRules())
			env.admit(t, testCompany, 0)
			if r := env.control(t, deletion("test:a1", testAlice, 3*time.Minute))[0]; r.Outcome != OutcomePending {
				t.Fatalf("outcome = %q, want pending", r.Outcome)
			}
			env.apply(t, customerMsg(testCompany, testBob, "test:b1", tt.moveWM))
			r := env.applyOne(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
			if r.Outcome != OutcomeStored || r.Opened != 1 {
				t.Fatalf("message: %+v, want stored and opened", r)
			}
			if got := env.hasText(t, "test:a1"); got != tt.wantText {
				t.Errorf("text stored = %v, want %v", got, tt.wantText)
			}
			if c := env.conv(t, testCompany, testAlice); c["nc"] != "1" {
				t.Errorf("counter = %s, want 1", c["nc"])
			}
		})
	}
}

// TestControlOrderSameState runs each control event before and after its
// message and compares the final Redis state, outbox included. The one
// allowed difference is the message_removed event (decision 0011): with the
// message first, its text was stored and the control event removes it with
// an event; with the control event first, no text is ever stored and no
// event is due. So the removed events and the sequence numbers are left out
// of the comparison, and the removed events are checked on their own.
func TestControlOrderSameState(t *testing.T) {
	tests := []struct {
		name    string
		msgs    func() []event.Message
		control func() event.Control
		removed int // message_removed events when the message comes first
	}{
		{
			name: "deletion",
			msgs: func() []event.Message {
				return []event.Message{customerMsg(testCompany, testAlice, "test:a1", time.Minute)}
			},
			control: func() event.Control { return deletion("test:a1", testAlice, 2*time.Minute) },
			removed: 1,
		},
		{
			// One message only. A company message that follows the customer
			// message of a removed customer is not covered: with the
			// removal first, the session is already closed when the company
			// message arrives, so it is dropped as no_open_conversation.
			name: "customer removal",
			msgs: func() []event.Message {
				return []event.Message{customerMsg(testCompany, testAlice, "test:a1", time.Minute)}
			},
			control: func() event.Control { return removal(testAlice, 2*time.Minute) },
			removed: 1,
		},
		{
			name: "company removal",
			msgs: func() []event.Message {
				return []event.Message{customerMsg(testCompany, testAlice, "test:a1", time.Minute)}
			},
			control: func() event.Control { return removal(testCompany, 2*time.Minute) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := func(controlFirst bool) (string, int) {
				env := newTestEnv(t, testRules())
				// The admitting conversation has its own keys; it is the
				// same in both orders.
				env.admit(t, testCompany, 0)
				if controlFirst {
					env.control(t, tt.control())
					env.apply(t, tt.msgs()...)
				} else {
					env.apply(t, tt.msgs()...)
					env.control(t, tt.control())
				}
				snap := env.snapshot(t)
				if tt.removed > 0 {
					snap = withoutRemoved(snap)
				}
				return snap, len(env.removedOf(t, testCompany, testAlice))
			}
			after, removedAfter := run(false)
			before, removedBefore := run(true)
			if after != before {
				t.Errorf("state differs.\nmessage first:\n%s\ncontrol first:\n%s", after, before)
			}
			if removedAfter != tt.removed || removedBefore != 0 {
				t.Errorf("removed events: %d with the message first, %d with the control first, want %d and 0",
					removedAfter, removedBefore, tt.removed)
			}
		})
	}
}

var (
	removedEntry = regexp.MustCompile(` ?\[[^\[\]]*ty=message_removed[^\[\]]*\]`)
	seqField     = regexp.MustCompile(`\bq=[0-9]+`)
)

// withoutRemoved drops the message_removed outbox entries and every
// sequence number from a snapshot.
func withoutRemoved(snapshot string) string {
	s := removedEntry.ReplaceAllString(snapshot, "")
	s = strings.ReplaceAll(s, "[ [", "[[")
	return seqField.ReplaceAllString(s, "q=")
}

func TestCustomerRemoval(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.admit(t, testCompany2, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		companyMsg(testCompany, testAlice, "test:c1", 2*time.Minute),
		customerMsg(testCompany2, testAlice, "test:a2", 3*time.Minute),
		customerMsg(testCompany, testBob, "test:b1", 3*time.Minute),
	)
	r := env.control(t, removal(testAlice, 4*time.Minute))[0]
	if r.Closed[event.CloseCustomerRemoved] != 2 {
		t.Fatalf("closed = %v, want 2 customer_removed", r.Closed)
	}
	if env.hasText(t, "test:a1") || env.hasText(t, "test:a2") {
		t.Error("text of the removed author is still stored")
	}
	// The company's text stays until close + 10 minutes. Bob is not
	// touched.
	if !env.hasText(t, "test:c1") || !env.hasText(t, "test:b1") {
		t.Error("text of other authors was removed")
	}
	if c := env.conv(t, testCompany, testBob); c["st"] != "o" {
		t.Error("conversation of another customer was closed")
	}
	evs := env.eventsOf(t, testCompany, testAlice)
	last := evs[len(evs)-1]
	if last.Type != event.TypeClosed || last.CloseReason != event.CloseCustomerRemoved || !last.SourceTime.Equal(t0.Add(4*time.Minute)) {
		t.Errorf("last event = %+v", last)
	}
}

func TestCompanyRemoval(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		companyMsg(testCompany, testAlice, "test:c1", 2*time.Minute),
		customerMsg(testCompany, testBob, "test:b1", 3*time.Minute),
	)
	r := env.control(t, removal(testCompany, 4*time.Minute))[0]
	// Alice, Bob and the third admitting customer.
	if r.Closed[event.CloseCompanyRemoved] != 3 || r.Ended != 1 {
		t.Fatalf("result = %+v, want 3 company_removed closes and an ended admission", r)
	}
	if env.hasText(t, "test:c1") {
		t.Error("company text still stored")
	}
	if !env.hasText(t, "test:a1") {
		t.Error("customer text removed at once, want it kept until close + 10 minutes")
	}
	// The admission ended: a new customer message must earn it again.
	if res := env.applyOne(t, customerMsg(testCompany, testCarol, "test:x1", 5*time.Minute)); res.Outcome != OutcomeNotAdmitted {
		t.Errorf("after removal: outcome = %q, want not_admitted", res.Outcome)
	}
}

func TestRemovalPurgesMessagesNewerThanTheRemoval(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	// The message is written after the removal but processed before it,
	// because the two topics are consumed independently.
	env.apply(t, customerMsg(testCompany, testAlice, "test:a1", 2*time.Minute+time.Second))
	env.control(t, removal(testCompany, 2*time.Minute))
	if !env.hasText(t, "test:a1") {
		t.Fatal("customer text removed at once, want it kept until close + 10 minutes")
	}
	env.apply(t, customerMsg(testCompany2, testBob, "test:b1", 13*time.Minute))
	env.sweep(t, 0)
	if env.hasText(t, "test:a1") {
		t.Error("text still stored after the removal close + 10 minutes")
	}
}
