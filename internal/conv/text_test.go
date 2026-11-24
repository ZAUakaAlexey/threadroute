package conv

import (
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

func TestNewTextReader(t *testing.T) {
	if _, err := NewTextReader(nil, ""); err == nil {
		t.Error("NewTextReader(nil) error = nil, want an error")
	}
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { rdb.Close() })
	r, err := NewTextReader(rdb, "")
	if err != nil {
		t.Fatalf("NewTextReader() error = %v", err)
	}
	if r.prefix != DefaultKeyPrefix {
		t.Errorf("prefix = %q, want %q", r.prefix, DefaultKeyPrefix)
	}
}

func (e *testEnv) reader(t testing.TB) *TextReader {
	t.Helper()
	r, err := NewTextReader(e.rdb, "")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (e *testEnv) read(t testing.TB, company, customer string) []StoredMessage {
	t.Helper()
	msgs, err := e.reader(t).Messages(testCtx(t), company, customer)
	if err != nil {
		t.Fatalf("Messages() error = %v", err)
	}
	return msgs
}

func storedIDs(msgs []StoredMessage) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}

func TestTextReaderUnknownConversation(t *testing.T) {
	env := newTestEnv(t, testRules())
	msgs := env.read(t, testCompany, testAlice)
	if msgs == nil || len(msgs) != 0 {
		t.Errorf("Messages() = %#v, want an empty list", msgs)
	}
	if _, ok, err := env.reader(t).Message(testCtx(t), testCompany, testAlice, "test:a1"); ok || err != nil {
		t.Errorf("Message() ok = %v, err = %v, want not found and no error", ok, err)
	}
}

func TestTextReaderEmptyConversation(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	// The conversation exists and counts a message, but its only text was
	// deleted before it arrived.
	env.control(t, deletion("test:a1", testAlice, 2*time.Minute))
	env.applyOne(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	if c := env.conv(t, testCompany, testAlice); c == nil {
		t.Fatal("conversation not opened")
	}
	if msgs := env.read(t, testCompany, testAlice); len(msgs) != 0 {
		t.Errorf("Messages() = %+v, want none", msgs)
	}
}

func TestTextReaderMessagesInOrder(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute),
		companyMsg(testCompany, testAlice, "test:c1", 3*time.Minute),
		// Late, and the same source time as a2: the ID breaks the tie.
		customerMsg(testCompany, testAlice, "test:a1", 2*time.Minute),
		customerMsg(testCompany, testAlice, "test:a0", time.Minute),
		// Another conversation of the same customer is not listed.
		customerMsg(testCompany2, testAlice, "test:x1", time.Minute),
	)
	msgs := env.read(t, testCompany, testAlice)
	if got, want := storedIDs(msgs), []string{"test:a0", "test:a1", "test:a2", "test:c1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
	c1 := msgs[3]
	want := StoredMessage{
		ID:         "test:c1",
		Direction:  event.FromCompany,
		SourceTime: t0.Add(3 * time.Minute),
		Text:       "synthetic reply test:c1",
	}
	if !reflect.DeepEqual(c1, want) {
		t.Errorf("message = %+v, want %+v", c1, want)
	}
	if msgs[0].Direction != event.FromCustomer || msgs[0].Edited {
		t.Errorf("first message = %+v", msgs[0])
	}

	one, ok, err := env.reader(t).Message(testCtx(t), testCompany, testAlice, "test:c1")
	if err != nil || !ok || !reflect.DeepEqual(one, want) {
		t.Errorf("Message() = %+v, %v, %v, want %+v", one, ok, err, want)
	}
	// A message of another conversation is not readable through this one.
	if _, ok, err := env.reader(t).Message(testCtx(t), testCompany, testAlice, "test:x1"); ok || err != nil {
		t.Errorf("Message() of another conversation: ok = %v, err = %v", ok, err)
	}
}

func TestTextReaderRemovedBetweenReads(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.apply(t,
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),
		customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute),
		customerMsg(testCompany, testAlice, "test:a3", 3*time.Minute),
	)
	if got := storedIDs(env.read(t, testCompany, testAlice)); len(got) != 3 {
		t.Fatalf("first read = %v, want 3 messages", got)
	}
	env.control(t, deletion("test:a2", testAlice, 4*time.Minute))
	if got, want := storedIDs(env.read(t, testCompany, testAlice)), []string{"test:a1", "test:a3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("second read = %v, want %v", got, want)
	}
	if _, ok, _ := env.reader(t).Message(testCtx(t), testCompany, testAlice, "test:a2"); ok {
		t.Error("Message() found a deleted message")
	}

	// A text hash that went away between the list and the read of one
	// message is left out.
	env.rdb.Del(testCtx(t), DefaultKeyPrefix+"x:test:a3")
	if got, want := storedIDs(env.read(t, testCompany, testAlice)), []string{"test:a1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("read with a lost text hash = %v, want %v", got, want)
	}
}

func TestTextReaderEditedMessage(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	orig := customerMsg(testCompany, testAlice, "test:a1", time.Minute)
	env.apply(t, orig, customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute))
	env.applyOne(t, edit(orig))
	msgs := env.read(t, testCompany, testAlice)
	if len(msgs) != 2 {
		t.Fatalf("messages = %+v", msgs)
	}
	if !msgs[0].Edited || msgs[0].Text != "synthetic edited reply" || !msgs[0].SourceTime.Equal(t0.Add(time.Minute)) {
		t.Errorf("edited message = %+v", msgs[0])
	}
	if msgs[1].Edited {
		t.Errorf("message without an edit reports edited: %+v", msgs[1])
	}
}

func TestTextReaderNeverWrites(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.applyOne(t, customerMsg(testCompany, testAlice, "test:a1", time.Minute))
	ctx := testCtx(t)
	textKey := DefaultKeyPrefix + "x:test:a1"
	msgsKey := DefaultKeyPrefix + "m:" + convKey(testCompany, testAlice)
	// Shorten the lifetimes, so that an extension would show.
	env.rdb.PExpire(ctx, textKey, time.Hour)
	env.rdb.PExpire(ctx, msgsKey, time.Hour)
	before := env.snapshot(t)
	env.read(t, testCompany, testAlice)
	if _, _, err := env.reader(t).Message(ctx, testCompany, testAlice, "test:a1"); err != nil {
		t.Fatal(err)
	}
	if after := env.snapshot(t); after != before {
		t.Errorf("state changed by a read.\nbefore:\n%s\nafter:\n%s", before, after)
	}
	for _, k := range []string{textKey, msgsKey} {
		if ttl := env.rdb.PTTL(ctx, k).Val(); ttl > time.Hour {
			t.Errorf("%s: lifetime %s, want at most 1h: a read extended it", k, ttl)
		}
	}
}
