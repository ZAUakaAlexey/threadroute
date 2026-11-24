package conv

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// removedOf returns the message_removed events of a conversation.
func (e *testEnv) removedOf(t testing.TB, company, customer string) []event.Conversation {
	t.Helper()
	var out []event.Conversation
	for _, ev := range e.eventsOf(t, company, customer) {
		if ev.Type == event.TypeMessageRemoved {
			out = append(out, ev)
		}
	}
	return out
}

// assertSequence checks that the outbox events of a conversation have the
// sequence numbers 1 to n with no gap, and that n is the number the state
// assigned.
func (e *testEnv) assertSequence(t testing.TB, company, customer string) {
	t.Helper()
	evs := e.eventsOf(t, company, customer)
	for i, ev := range evs {
		if ev.Seq != uint64(i+1) {
			t.Errorf("%s/%s: event %d (%s) has seq %d", company, customer, i, ev.Type, ev.Seq)
		}
	}
	if q := e.conv(t, company, customer)["q"]; q != strconv.Itoa(len(evs)) {
		t.Errorf("%s/%s: state seq %s, %d events in the outbox", company, customer, q, len(evs))
	}
}

func TestRemovedOnDeletion(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute),
	)
	r := env.control(t, deletion("test:a1", testAlice, 3*time.Minute))[0]
	if r.Outcome != OutcomeRemoved || r.Removed[event.RemoveDeleted] != 1 || r.Events != 1 {
		t.Fatalf("result = %+v, want removed with one deleted event", r)
	}
	if len(r.Outboxes) != 1 || r.Outboxes[0] != 0 {
		t.Errorf("outboxes = %v, want [0]", r.Outboxes)
	}
	evs := env.eventsOf(t, testCompany, testAlice)
	want := []event.ConversationType{event.TypeOpened, event.TypeMessageAdded, event.TypeMessageRemoved}
	if got := types(evs); !reflect.DeepEqual(got, want) {
		t.Fatalf("types = %v, want %v", got, want)
	}
	rm := evs[2]
	if rm.MessageID != "test:a1" || rm.Direction != event.FromCustomer || rm.RemoveReason != event.RemoveDeleted ||
		rm.Session != 1 || rm.Seq != 3 || !rm.SourceTime.Equal(t0.Add(3*time.Minute)) ||
		rm.CustomerMessages != 2 || !rm.OpenedAt.Equal(t0.Add(time.Minute)) || rm.Lang != "en" {
		t.Errorf("removed event = %+v", rm)
	}
	env.assertSequence(t, testCompany, testAlice)
}

func TestRemovedOnDeletionOfCompanyMessage(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		companyMsg(testCompany, testAlice, "test:c1", 2*time.Minute),
	)
	env.control(t, deletion("test:c1", testCompany, 3*time.Minute))
	rm := env.removedOf(t, testCompany, testAlice)
	if len(rm) != 1 || rm[0].Direction != event.FromCompany || rm[0].MessageID != "test:c1" {
		t.Fatalf("removed events = %+v, want one for the company message", rm)
	}
	env.assertSequence(t, testCompany, testAlice)
}

func TestNoRemovedForDeletionBeforeMessage(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	if r := env.control(t, deletion("test:a1", testAlice, 3*time.Minute))[0]; r.Outcome != OutcomePending || r.Events != 0 {
		t.Fatalf("result = %+v, want pending and no event", r)
	}
	env.applyOne(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	if rm := env.removedOf(t, testCompany, testAlice); len(rm) != 0 {
		t.Errorf("removed events = %+v, want none: no text was stored", rm)
	}
	env.assertSequence(t, testCompany, testAlice)
}

func TestRemovedOnlyOnceForTwoDeletions(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.applyOne(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	first := deletion("test:a1", testAlice, 2*time.Minute)
	// The same deletion delivered again, and a second deletion event of the
	// same message.
	res := env.control(t, first, first, deletion("test:a1", testAlice, 3*time.Minute))
	if res[0].Events != 1 || res[1].Outcome != OutcomeDuplicate || res[1].Events != 0 || res[2].Events != 0 {
		t.Fatalf("results = %+v, want one event in total", res)
	}
	if rm := env.removedOf(t, testCompany, testAlice); len(rm) != 1 {
		t.Errorf("removed events = %d, want 1", len(rm))
	}
	env.assertSequence(t, testCompany, testAlice)
}

func TestRemovedOnAuthorRemovalInTwoConversations(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.admit(t, testCompany2, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute),
		// Same source time as a2: the ID breaks the tie.
		customerMsg(testCompany, testAlice, "test:a1", 2*time.Minute),
		companyMsg(testCompany, testAlice, "test:c1", 3*time.Minute),
		customerMsg(testCompany, testAlice, "test:a0", 4*time.Minute),
		customerMsg(testCompany2, testAlice, "test:a3", 3*time.Minute),
	)
	r := env.control(t, removal(testAlice, 5*time.Minute))[0]
	if r.Removed[event.RemoveAuthorRemoved] != 4 || r.Closed[event.CloseCustomerRemoved] != 2 {
		t.Fatalf("result = %+v, want 4 removed events and 2 closes", r)
	}

	evs := env.eventsOf(t, testCompany, testAlice)
	wantTypes := []event.ConversationType{
		event.TypeOpened, event.TypeMessageAdded, event.TypeMessageAdded, event.TypeAnswered, event.TypeMessageAdded,
		event.TypeMessageRemoved, event.TypeMessageRemoved, event.TypeMessageRemoved, event.TypeClosed,
	}
	if got := types(evs); !reflect.DeepEqual(got, wantTypes) {
		t.Fatalf("types = %v, want %v", got, wantTypes)
	}
	var ids []string
	for _, ev := range env.removedOf(t, testCompany, testAlice) {
		ids = append(ids, ev.MessageID)
		if ev.RemoveReason != event.RemoveAuthorRemoved || ev.Direction != event.FromCustomer || ev.Session != 1 ||
			!ev.SourceTime.Equal(t0.Add(5*time.Minute)) {
			t.Errorf("removed event = %+v", ev)
		}
	}
	// Source time order, then by ID. The company message stays.
	if want := []string{"test:a1", "test:a2", "test:a0"}; !reflect.DeepEqual(ids, want) {
		t.Errorf("removed IDs = %v, want %v", ids, want)
	}
	if !env.hasText(t, "test:c1") {
		t.Error("the company message was removed")
	}

	rm2 := env.removedOf(t, testCompany2, testAlice)
	if len(rm2) != 1 || rm2[0].MessageID != "test:a3" {
		t.Errorf("second conversation: removed events = %+v, want one for test:a3", rm2)
	}
	if evs2 := env.eventsOf(t, testCompany2, testAlice); evs2[len(evs2)-1].Type != event.TypeClosed {
		t.Errorf("second conversation: last event %s, want closed", evs2[len(evs2)-1].Type)
	}
	env.assertSequence(t, testCompany, testAlice)
	env.assertSequence(t, testCompany2, testAlice)

	// The removal delivered again changes nothing.
	before := len(env.outbox(t, 0))
	env.control(t, removal(testAlice, 6*time.Minute))
	if n := len(env.outbox(t, 0)); n != before {
		t.Errorf("a second removal added %d events", n-before)
	}
}

func TestRemovedOnCompanyRemoval(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		companyMsg(testCompany, testAlice, "test:c1", 2*time.Minute),
		customerMsg(testCompany, testBob, "test:b1", 3*time.Minute),
		companyMsg(testCompany, testBob, "test:c2", 4*time.Minute),
	)
	r := env.control(t, removal(testCompany, 5*time.Minute))[0]
	if r.Removed[event.RemoveAuthorRemoved] != 2 {
		t.Fatalf("removed = %v, want 2", r.Removed)
	}
	for who, id := range map[string]string{testAlice: "test:c1", testBob: "test:c2"} {
		rm := env.removedOf(t, testCompany, who)
		if len(rm) != 1 || rm[0].MessageID != id || rm[0].Direction != event.FromCompany {
			t.Errorf("%s: removed events = %+v, want one for %s", who, rm, id)
		}
		env.assertSequence(t, testCompany, who)
	}
}

func TestRemovedAfterCloseWhileTextIsKept(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.applyOne(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	// Close at 31m, text kept until 41m.
	env.applyOne(t, customerMsg(testCompany, testBob, "test:b1", 35*time.Minute))
	env.sweep(t, 0)
	if c := env.conv(t, testCompany, testAlice); c["st"] != "c" {
		t.Fatal("conversation not closed")
	}
	env.control(t, deletion("test:a1", testAlice, 36*time.Minute))
	evs := env.eventsOf(t, testCompany, testAlice)
	want := []event.ConversationType{event.TypeOpened, event.TypeClosed, event.TypeMessageRemoved}
	if got := types(evs); !reflect.DeepEqual(got, want) {
		t.Fatalf("types = %v, want %v", got, want)
	}
	if rm := evs[2]; rm.Session != 1 || rm.MessageID != "test:a1" {
		t.Errorf("removed event = %+v", rm)
	}
	if c := env.conv(t, testCompany, testAlice); c["st"] != "c" {
		t.Errorf("state = %q, want still closed", c["st"])
	}
	env.assertSequence(t, testCompany, testAlice)
}

func TestRemovedOnAuthorRemovalAfterClose(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.applyOne(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	// Close at 31m, text kept until 41m.
	env.applyOne(t, customerMsg(testCompany, testBob, "test:b1", 35*time.Minute))
	env.sweep(t, 0)
	r := env.control(t, removal(testAlice, 36*time.Minute))[0]
	if r.Removed[event.RemoveAuthorRemoved] != 1 || len(r.Closed) != 0 {
		t.Fatalf("result = %+v, want one removed event and no close", r)
	}
	evs := env.eventsOf(t, testCompany, testAlice)
	want := []event.ConversationType{event.TypeOpened, event.TypeClosed, event.TypeMessageRemoved}
	if got := types(evs); !reflect.DeepEqual(got, want) {
		t.Fatalf("types = %v, want %v", got, want)
	}
	if rm := evs[2]; rm.Session != 1 || rm.MessageID != "test:a1" || rm.RemoveReason != event.RemoveAuthorRemoved {
		t.Errorf("removed event = %+v", rm)
	}
	env.assertSequence(t, testCompany, testAlice)
}

func TestRemovedCarriesCurrentSession(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.applyOne(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	env.applyOne(t, customerMsg(testCompany, testBob, "test:b1", 35*time.Minute))
	env.sweep(t, 0)
	// A new session opens while the text of the first one is kept.
	if r := env.applyOne(t, customerMsg(testCompany, testAlice, "test:a2", 36*time.Minute)); r.Reopened != 1 {
		t.Fatalf("result = %+v, want a reopened session", r)
	}
	r := env.control(t, removal(testAlice, 37*time.Minute))[0]
	if r.Removed[event.RemoveAuthorRemoved] != 2 {
		t.Fatalf("removed = %v, want 2", r.Removed)
	}
	for _, ev := range env.removedOf(t, testCompany, testAlice) {
		if ev.Session != 2 {
			t.Errorf("removed event of %s: session %d, want the current session 2", ev.MessageID, ev.Session)
		}
	}
	env.assertSequence(t, testCompany, testAlice)
}

func TestNoRemovedAfterTextWasPurged(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.applyOne(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	env.applyOne(t, customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute))
	// Close at 32m, text purged at 42m.
	env.applyOne(t, customerMsg(testCompany, testBob, "test:b1", 45*time.Minute))
	env.sweep(t, 0)
	if env.hasText(t, "test:a1") {
		t.Fatal("text not purged")
	}
	before := len(env.eventsOf(t, testCompany, testAlice))
	res := env.control(t,
		deletion("test:a1", testAlice, 46*time.Minute),
		removal(testAlice, 47*time.Minute),
	)
	for i, r := range res {
		if r.Events != 0 || len(r.Removed) != 0 {
			t.Errorf("control %d: result = %+v, want no event", i, r)
		}
	}
	if n := len(env.eventsOf(t, testCompany, testAlice)); n != before {
		t.Errorf("events grew from %d to %d", before, n)
	}
	env.assertSequence(t, testCompany, testAlice)
}

func TestNoRemovedForMessagesKeptLimit(t *testing.T) {
	rules := testRules()
	rules.MessagesKept = 2
	env := newTestEnv(t, rules)
	env.admit(t, testCompany, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute),
		customerMsg(testCompany, testAlice, "test:a3", 3*time.Minute),
	)
	if env.hasText(t, "test:a1") {
		t.Fatal("test:a1 not trimmed")
	}
	if rm := env.removedOf(t, testCompany, testAlice); len(rm) != 0 {
		t.Errorf("removed events = %+v, want none for the limit", rm)
	}
}

func TestRemovedWithoutConversationRecord(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.applyOne(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	// The record is lost, for example by its safety lifetime.
	env.rdb.Del(testCtx(t), DefaultKeyPrefix+"c:"+convKey(testCompany, testAlice))
	before := len(env.outbox(t, 0))
	r := env.control(t, deletion("test:a1", testAlice, 2*time.Minute))[0]
	if r.Outcome != OutcomeRemoved || r.RemovedWithoutEvent != 1 || r.Events != 0 {
		t.Fatalf("result = %+v, want the text removed without an event", r)
	}
	if env.hasText(t, "test:a1") {
		t.Error("text still stored")
	}
	if n := len(env.outbox(t, 0)); n != before {
		t.Errorf("outbox grew from %d to %d", before, n)
	}
}

func TestRemovedMetrics(t *testing.T) {
	m, err := newMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	m.observe(Result{
		Removed:             map[event.RemoveReason]int{event.RemoveDeleted: 1, event.RemoveAuthorRemoved: 3},
		RemovedWithoutEvent: 2,
	})
	for reason, want := range map[event.RemoveReason]float64{event.RemoveDeleted: 1, event.RemoveAuthorRemoved: 3} {
		if got := testutil.ToFloat64(m.messagesRemoved.WithLabelValues(string(reason))); got != want {
			t.Errorf("messages_removed_total{reason=%q} = %v, want %v", reason, got, want)
		}
	}
	if got := testutil.ToFloat64(m.removedNoEvent); got != 2 {
		t.Errorf("removed_without_event_total = %v, want 2", got)
	}
}
