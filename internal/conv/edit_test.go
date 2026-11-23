package conv

import (
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// edit returns an edit of m: the same message ID, a new event ID, new text
// and new language tags.
func edit(m event.Message) event.Message {
	m.EventID = nextID("edit")
	m.Edited = true
	m.Text = "synthetic edited reply"
	m.Langs = []string{"de"}
	m.Lang = "de"
	return m
}

func (e *testEnv) text(t *testing.T, msgID string) (text, langs string) {
	t.Helper()
	v, err := e.rdb.HMGet(testCtx(t), DefaultKeyPrefix+"x:"+msgID, "x", "lg").Result()
	if err != nil {
		t.Fatal(err)
	}
	text, _ = v[0].(string)
	langs, _ = v[1].(string)
	return text, langs
}

// editState is the state an edit must not change: everything except the
// text hash of the edited message and the markers.
func (e *testEnv) editState(t *testing.T, msgID string) string {
	t.Helper()
	text, langs := e.text(t, msgID)
	return stripLine(e.snapshot(t), DefaultKeyPrefix+"x:"+msgID+" ") + "|" + text + "|" + langs
}

func stripLine(s, prefix string) string {
	out := ""
	for _, line := range splitLines(s) {
		if len(line) >= len(prefix) && line[:len(prefix)] == prefix {
			continue
		}
		out += line + "\n"
	}
	return out
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := range len(s) {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func TestEditOfStoredMessage(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	orig := customerMsg(testCompany, testAlice, "test:a1", time.Minute)
	env.apply(t, orig, customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute))

	before := stripLine(env.snapshot(t), DefaultKeyPrefix+"x:test:a1 ")
	ed := edit(orig)
	// The edit carries a later source time, which must not move the
	// deadline of the conversation.
	ed.SourceTime = t0.Add(20 * time.Minute)
	r := env.applyOne(t, ed)
	if r.Outcome != OutcomeEdited || r.Events != 0 || r.Opened != 0 || r.Admitted != 0 {
		t.Fatalf("result = %+v, want edited and nothing else", r)
	}
	if text, langs := env.text(t, "test:a1"); text != "synthetic edited reply" || langs != "de" {
		t.Errorf("text %q langs %q", text, langs)
	}
	// Only the text hash and the watermark (a record of the partition) may
	// change.
	after := stripLine(env.snapshot(t), DefaultKeyPrefix+"x:test:a1 ")
	if stripLine(after, DefaultKeyPrefix+"wm ") != stripLine(before, DefaultKeyPrefix+"wm ") {
		t.Errorf("edit changed other state:\nbefore\n%s\nafter\n%s", before, after)
	}
}

func TestEditDeliveredTwice(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	orig := customerMsg(testCompany, testAlice, "test:a1", time.Minute)
	env.applyOne(t, orig)
	ed := edit(orig)
	env.applyOne(t, ed)
	before := env.editState(t, "test:a1")
	if r := env.applyOne(t, ed); r.Outcome != OutcomeDuplicate {
		t.Errorf("second delivery: outcome = %q, want duplicate", r.Outcome)
	}
	if after := env.editState(t, "test:a1"); after != before {
		t.Error("second delivery of the edit changed the state")
	}
}

func TestEditOfUnknownMessage(t *testing.T) {
	t.Run("original never seen", func(t *testing.T) {
		env := newTestEnv(t, testRules())
		env.admit(t, testCompany, 0)
		r := env.applyOne(t, edit(customerMsg(testCompany, testAlice, "test:u1", time.Minute)))
		if r.Outcome != OutcomeEditDropped || r.Opened != 0 || r.Events != 0 {
			t.Fatalf("result = %+v, want edit_dropped", r)
		}
		if env.conv(t, testCompany, testAlice) != nil || env.hasText(t, "test:u1") {
			t.Error("the edit opened a conversation or stored text")
		}
	})
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	// Seen but never stored: the company of this message is not admitted.
	orig := customerMsg(testCompany2, testAlice, "test:n1", time.Minute)
	if r := env.applyOne(t, orig); r.Outcome != OutcomeNotAdmitted {
		t.Fatalf("original: %q", r.Outcome)
	}
	// Admit the company now, so an edit taken for a new message would open
	// a conversation.
	env.admit(t, testCompany2, 2*time.Minute)
	r := env.applyOne(t, edit(orig))
	if r.Outcome != OutcomeEditDropped || r.Opened != 0 || r.Events != 0 {
		t.Fatalf("result = %+v, want edit_dropped", r)
	}
	if env.conv(t, testCompany2, testAlice) != nil || env.hasText(t, "test:n1") {
		t.Error("the edit opened a conversation or stored text")
	}
}

func TestEditAfterDeletion(t *testing.T) {
	t.Run("deletion after the message", func(t *testing.T) {
		env := newTestEnv(t, testRules())
		env.admit(t, testCompany, 0)
		orig := customerMsg(testCompany, testAlice, "test:a1", time.Minute)
		env.applyOne(t, orig)
		env.control(t, deletion("test:a1", testAlice, 2*time.Minute))
		if r := env.applyOne(t, edit(orig)); r.Outcome != OutcomeEditDropped {
			t.Errorf("outcome = %q, want edit_dropped", r.Outcome)
		}
		if env.hasText(t, "test:a1") {
			t.Error("the edit stored deleted text")
		}
	})
	t.Run("pending deletion wins", func(t *testing.T) {
		env := newTestEnv(t, testRules())
		env.admit(t, testCompany, 0)
		env.control(t, deletion("test:a1", testAlice, 2*time.Minute))
		orig := customerMsg(testCompany, testAlice, "test:a1", time.Minute)
		env.applyOne(t, orig)
		if r := env.applyOne(t, edit(orig)); r.Outcome != OutcomeEditDropped {
			t.Errorf("outcome = %q, want edit_dropped", r.Outcome)
		}
		if env.hasText(t, "test:a1") {
			t.Error("the edit stored text of a deleted message")
		}
	})
}

func TestEditOfClosedConversationWithKeptText(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	orig := customerMsg(testCompany, testAlice, "test:a1", time.Minute)
	env.applyOne(t, orig)
	// Close at 31m, text kept until 41m.
	env.apply(t, customerMsg(testCompany, testBob, "test:b1", 35*time.Minute))
	env.sweep(t, 0)
	if c := env.conv(t, testCompany, testAlice); c["st"] != "c" {
		t.Fatal("conversation not closed")
	}
	events := len(env.outbox(t, 0))
	r := env.applyOne(t, edit(orig))
	if r.Outcome != OutcomeEdited || r.Opened != 0 {
		t.Fatalf("result = %+v, want edited without a new session", r)
	}
	if c := env.conv(t, testCompany, testAlice); c["st"] != "c" || c["s"] != "1" {
		t.Errorf("conversation = %v, want still closed, session 1", c)
	}
	if text, _ := env.text(t, "test:a1"); text != "synthetic edited reply" {
		t.Errorf("text = %q", text)
	}
	if n := len(env.outbox(t, 0)); n != events {
		t.Errorf("outbox grew from %d to %d", events, n)
	}
}
