package conv

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

func TestOpenAddAnswer(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)

	res := env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute),
		companyMsg(testCompany, testAlice, "test:c1", 6*time.Minute+500*time.Microsecond),
		companyMsg(testCompany, testAlice, "test:c2", 7*time.Minute),
	)
	for i, r := range res {
		if r.Outcome != OutcomeStored {
			t.Fatalf("message %d: outcome = %q", i, r.Outcome)
		}
	}
	if !res[2].Answered || res[2].ResponseTime != 5*time.Minute+500*time.Microsecond {
		t.Errorf("answer: answered = %v, response = %s, want 5m0.0005s", res[2].Answered, res[2].ResponseTime)
	}
	if res[3].Answered {
		t.Errorf("second company message answered again")
	}

	evs := env.eventsOf(t, testCompany, testAlice)
	wantTypes := []event.ConversationType{
		event.TypeOpened, event.TypeMessageAdded, event.TypeMessageAdded, event.TypeAnswered, event.TypeMessageAdded,
	}
	if got := types(evs); !reflect.DeepEqual(got, wantTypes) {
		t.Fatalf("event types = %v, want %v", got, wantTypes)
	}
	for i, ev := range evs {
		if ev.Seq != uint64(i+1) || ev.Session != 1 {
			t.Errorf("event %d: seq %d session %d, want seq %d session 1", i, ev.Seq, ev.Session, i+1)
		}
		if !ev.OpenedAt.Equal(t0.Add(time.Minute)) || ev.Lang != "en" {
			t.Errorf("event %d: opened at %s lang %q", i, ev.OpenedAt, ev.Lang)
		}
	}
	ans := evs[3]
	if ans.ResponseTime != 5*time.Minute+500*time.Microsecond || ans.MessageID != "test:c1" {
		t.Errorf("answered event = %+v", ans)
	}
	last := evs[4]
	if last.CustomerMessages != 2 || last.CompanyMessages != 2 {
		t.Errorf("counters = %d/%d, want 2/2", last.CustomerMessages, last.CompanyMessages)
	}
	if !last.SourceTime.Equal(t0.Add(7 * time.Minute)) {
		t.Errorf("source time = %s, want exact microseconds of t0+7m", last.SourceTime)
	}
}

func TestSessionLanguageIsFirstCustomerMessage(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	first := customerMsg(testCompany, testAlice, "test:a1", time.Minute)
	first.Lang = "de"
	second := customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute)
	second.Lang = "fr"
	env.apply(t, first, second)
	for _, ev := range env.eventsOf(t, testCompany, testAlice) {
		if ev.Lang != "de" {
			t.Errorf("seq %d: lang %q, want de", ev.Seq, ev.Lang)
		}
	}
}

func TestCloseAtDeadline(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	deadline := t0.Add(31 * time.Minute)

	aliceOpen := func() bool { return env.conv(t, testCompany, testAlice)["st"] == "o" }

	// Another conversation in the same partition moves the watermark to
	// one microsecond before the deadline: nothing closes.
	env.apply(t, customerMsg(testCompany, testBob, "test:b1", 31*time.Minute-time.Microsecond))
	env.sweep(t, 0)
	if !aliceOpen() {
		t.Fatal("closed before the deadline")
	}
	// A watermark in another partition does not close it either.
	env.rdb.HSet(testCtx(t), DefaultKeyPrefix+"wm", "1", fmt.Sprint(t0.Add(5*time.Hour).UnixMicro()))
	env.sweep(t, 0)
	if !aliceOpen() {
		t.Fatal("closed by the watermark of another partition")
	}

	// The watermark reaches the deadline exactly.
	env.apply(t, customerMsg(testCompany, testBob, "test:b2", 31*time.Minute))
	if r := env.sweep(t, 0); r.Closed[event.CloseInactivity] != 1 || aliceOpen() {
		t.Fatalf("closed = %v, want one inactivity close", r.Closed)
	}
	evs := env.eventsOf(t, testCompany, testAlice)
	closed := evs[len(evs)-1]
	if closed.Type != event.TypeClosed || closed.CloseReason != event.CloseInactivity || !closed.SourceTime.Equal(deadline) {
		t.Errorf("close event = %+v, want closed at %s", closed, deadline)
	}
	if c := env.conv(t, testCompany, testAlice); c["st"] != "c" {
		t.Errorf("state = %q, want closed", c["st"])
	}

	// The text stays 10 minutes after the close, in source time.
	env.apply(t, customerMsg(testCompany, testBob, "test:b3", 40*time.Minute))
	env.sweep(t, 0)
	if !env.hasText(t, "test:a1") {
		t.Fatal("text removed before close + 10 minutes")
	}
	env.apply(t, customerMsg(testCompany, testBob, "test:b4", 41*time.Minute))
	if r := env.sweep(t, 0); r.Purged != 1 {
		t.Errorf("purged = %d, want 1", r.Purged)
	}
	if env.hasText(t, "test:a1") || len(env.messageIDs(t, testCompany, testAlice)) != 0 {
		t.Error("text still stored after close + 10 minutes")
	}
}

func TestReopenContinuesSequence(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		companyMsg(testCompany, testAlice, "test:c1", 2*time.Minute),
	)
	// The next customer message comes after the deadline. It closes the
	// first session at its deadline and opens session 2 in one step,
	// without a sweep.
	r := env.applyOne(t, customerMsg(testCompany, testAlice, "test:a2", time.Hour))
	if r.Opened != 1 || r.Reopened != 1 || r.Closed[event.CloseInactivity] != 1 {
		t.Fatalf("result = %+v", r)
	}
	evs := env.eventsOf(t, testCompany, testAlice)
	want := []struct {
		typ     event.ConversationType
		session uint64
		nc, nk  uint64
	}{
		{event.TypeOpened, 1, 1, 0},
		{event.TypeMessageAdded, 1, 1, 1},
		{event.TypeAnswered, 1, 1, 1},
		{event.TypeClosed, 1, 1, 1},
		{event.TypeOpened, 2, 1, 0},
	}
	if len(evs) != len(want) {
		t.Fatalf("events = %v", types(evs))
	}
	for i, w := range want {
		ev := evs[i]
		if ev.Type != w.typ || ev.Session != w.session || ev.Seq != uint64(i+1) ||
			ev.CustomerMessages != w.nc || ev.CompanyMessages != w.nk {
			t.Errorf("event %d = %s session %d seq %d counters %d/%d, want %s session %d seq %d counters %d/%d",
				i, ev.Type, ev.Session, ev.Seq, ev.CustomerMessages, ev.CompanyMessages,
				w.typ, w.session, i+1, w.nc, w.nk)
		}
	}
	if !evs[3].SourceTime.Equal(t0.Add(32 * time.Minute)) {
		t.Errorf("close time = %s, want last message + 30m", evs[3].SourceTime)
	}
	if !evs[4].OpenedAt.Equal(t0.Add(time.Hour)) || !evs[4].Reopened() {
		t.Errorf("session 2 opened at %s", evs[4].OpenedAt)
	}
}

func TestSessionRecordForgottenAfter24Hours(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	env.admit(t, testCompany, 26*time.Hour)
	env.apply(t, customerMsg(testCompany, testAlice, "test:a2", 26*time.Hour))
	evs := env.eventsOf(t, testCompany, testAlice)
	last := evs[len(evs)-1]
	if last.Type != event.TypeOpened || last.Session != 1 || last.Seq != 1 {
		t.Errorf("after 24h: %s session %d seq %d, want opened session 1 seq 1", last.Type, last.Session, last.Seq)
	}
}

func TestCompanyMessageWithNoOpenConversation(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	r := env.applyOne(t, companyMsg(testCompany, testAlice, "test:c1", time.Minute))
	if r.Outcome != OutcomeNoOpenConversation {
		t.Errorf("outcome = %q, want %q", r.Outcome, OutcomeNoOpenConversation)
	}
	if env.conv(t, testCompany, testAlice) != nil || env.hasText(t, "test:c1") {
		t.Error("state was stored for a dropped message")
	}
}

func TestMessagesKeptLimit(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	var msgs []event.Message
	for i := range 55 {
		msgs = append(msgs, customerMsg(testCompany, testAlice, fmt.Sprintf("test:a%02d", i), time.Duration(i+1)*time.Second))
	}
	env.apply(t, msgs...)
	ids := env.messageIDs(t, testCompany, testAlice)
	if len(ids) != 50 || ids[0] != "test:a05" || ids[49] != "test:a54" {
		t.Fatalf("kept %d messages from %s to %s, want 50 from test:a05 to test:a54", len(ids), ids[0], ids[len(ids)-1])
	}
	for i := range 5 {
		if env.hasText(t, fmt.Sprintf("test:a%02d", i)) {
			t.Errorf("text of trimmed message %d is still stored", i)
		}
	}
	if c := env.conv(t, testCompany, testAlice); c["nc"] != "55" {
		t.Errorf("customer counter = %s, want 55", c["nc"])
	}
}

func TestLateMessageInsertedInSourceOrder(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		customerMsg(testCompany, testAlice, "test:a3", 3*time.Minute),
		// Arrives last, written between the two.
		companyMsg(testCompany, testAlice, "test:a2", 2*time.Minute),
	)
	got := env.messageIDs(t, testCompany, testAlice)
	if want := []string{"test:a1", "test:a2", "test:a3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
	// The deadline follows the newest message, not the late one.
	if c := env.conv(t, testCompany, testAlice); c["last"] != fmt.Sprint(t0.Add(3*time.Minute).UnixMicro()) {
		t.Errorf("last = %s", c["last"])
	}
}

func TestWatermark(t *testing.T) {
	env := newTestEnv(t, testRules())
	ctx := testCtx(t)
	apply := func(p int32, at time.Duration) {
		t.Helper()
		_, err := env.store.ApplyMessages(ctx, []PartitionedMessage{{Partition: p, Message: customerMsg(testCompany, testAlice, nextID("m"), at)}})
		if err != nil {
			t.Fatal(err)
		}
	}
	apply(0, 10*time.Minute)
	apply(1, 5*time.Minute)
	apply(0, 3*time.Minute) // older: the watermark does not go back

	wms, err := env.store.Watermarks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !wms[0].Equal(t0.Add(10*time.Minute)) || !wms[1].Equal(t0.Add(5*time.Minute)) {
		t.Fatalf("watermarks = %v", wms)
	}

	// A new store on the same Redis, as after a restart, sees the same.
	again, err := NewStore(env.rdb, testRules(), "")
	if err != nil {
		t.Fatal(err)
	}
	wms2, err := again.Watermarks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wms, wms2) {
		t.Errorf("after restart = %v, want %v", wms2, wms)
	}
}

func TestIdempotentRedelivery(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)

	inputs := []any{
		customerMsg(testCompany2, testAlice, "test:n1", time.Minute), // not admitted, changes a set
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute),
		companyMsg(testCompany, testAlice, "test:c1", 3*time.Minute),
		companyMsg(testCompany, testBob, "test:c2", 3*time.Minute), // no open conversation
		deletion("test:a2", testAlice, 4*time.Minute),
		deletion("test:missing", testBob, 4*time.Minute),
		removal(testCarol, 4*time.Minute),
		customerMsg(testCompany, testAlice, "test:a3", 2*time.Hour), // closes and reopens
	}
	for i, in := range inputs {
		var first, second Result
		switch v := in.(type) {
		case event.Message:
			first = env.applyOne(t, v)
			before := env.snapshot(t)
			second = env.applyOne(t, v)
			if after := env.snapshot(t); after != before {
				t.Errorf("input %d: second delivery changed the state:\nbefore\n%s\nafter\n%s", i, before, after)
			}
		case event.Control:
			first = env.control(t, v)[0]
			before := env.snapshot(t)
			second = env.control(t, v)[0]
			if after := env.snapshot(t); after != before {
				t.Errorf("input %d: second delivery changed the state", i)
			}
		}
		if first.Outcome == OutcomeDuplicate {
			t.Errorf("input %d: first delivery was a duplicate", i)
		}
		if second.Outcome != OutcomeDuplicate || second.Events != 0 || len(second.Outboxes) != 0 {
			t.Errorf("input %d: second delivery = %+v, want a duplicate with no events", i, second)
		}
	}
}

func TestSameMessageUnderNewEventID(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	m := customerMsg(testCompany, testAlice, "test:a1", time.Minute)
	env.applyOne(t, m)
	m.EventID = nextID("other")
	// Not marked as an edit: a duplicate.
	if r := env.applyOne(t, m); r.Outcome != OutcomeDuplicate {
		t.Errorf("outcome = %q, want duplicate", r.Outcome)
	}
}
