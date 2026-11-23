package conv

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// Synthetic, clearly fake test data (decision 0004).
const (
	testCompany  = "did:plc:testcompany00000000000000"
	testCompany2 = "did:plc:testcompanytwo00000000000"
	testAlice    = "did:plc:testalice0000000000000000"
	testBob      = "did:plc:testbob000000000000000000"
	testCarol    = "did:plc:testcarol0000000000000000"
)

// t0 is the source time where every test starts.
var t0 = time.Date(2026, 1, 1, 12, 0, 0, 123456000, time.UTC)

func testCustomer(i int) string {
	return fmt.Sprintf("did:plc:testcustomer%014d", i)
}

func testCtx(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// testRules are the defaults with a threshold of 3, so tests stay short.
func testRules() Rules {
	r := DefaultRules()
	r.AdmissionThreshold = 3
	return r
}

type testEnv struct {
	rdb   *redis.Client
	store *Store
}

// realRedisAddr is set by the integration tests. The store tests then run
// against a real Redis, emptied before each test, instead of miniredis.
var realRedisAddr string

func newTestEnv(t testing.TB, rules Rules) *testEnv {
	t.Helper()
	addr := realRedisAddr
	if addr == "" {
		addr = miniredis.RunT(t).Addr()
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	if realRedisAddr != "" {
		if err := rdb.FlushAll(testCtx(t)).Err(); err != nil {
			t.Fatal(err)
		}
	}
	st, err := NewStore(rdb, rules, "")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return &testEnv{rdb: rdb, store: st}
}

// seq gives every built event a new event ID.
var seq int

func nextID(kind string) string {
	seq++
	return fmt.Sprintf("test:%s:%d", kind, seq)
}

// customerMsg is a message from customer to company at t0+at.
func customerMsg(company, customer, msgID string, at time.Duration) event.Message {
	return event.Message{
		EventID:    nextID("msg"),
		Channel:    event.ChannelBluesky,
		SourceTime: t0.Add(at),
		MessageID:  msgID,
		CompanyID:  company,
		CustomerID: customer,
		Direction:  event.FromCustomer,
		ParentID:   "test:parent:" + msgID,
		Text:       "synthetic reply " + msgID,
		Langs:      []string{"en"},
		Lang:       "en",
	}
}

// companyMsg is a message from company to customer at t0+at.
func companyMsg(company, customer, msgID string, at time.Duration) event.Message {
	m := customerMsg(company, customer, msgID, at)
	m.Direction = event.FromCompany
	return m
}

func deletion(msgID, author string, at time.Duration) event.Deletion {
	return event.Deletion{
		EventID:    nextID("del"),
		Channel:    event.ChannelBluesky,
		SourceTime: t0.Add(at),
		MessageID:  msgID,
		AuthorID:   author,
	}
}

func removal(author string, at time.Duration) event.AuthorRemoval {
	return event.AuthorRemoval{
		EventID:    nextID("rm"),
		Channel:    event.ChannelBluesky,
		SourceTime: t0.Add(at),
		AuthorID:   author,
		Reason:     event.RemovalInactive,
	}
}

// apply applies message events in partition 0 and returns the results.
func (e *testEnv) apply(t testing.TB, msgs ...event.Message) []Result {
	t.Helper()
	pms := make([]PartitionedMessage, len(msgs))
	for i, m := range msgs {
		pms[i] = PartitionedMessage{Partition: 0, Message: m}
	}
	res, err := e.store.ApplyMessages(testCtx(t), pms)
	if err != nil {
		t.Fatalf("ApplyMessages() error = %v", err)
	}
	return res
}

func (e *testEnv) applyOne(t testing.TB, m event.Message) Result {
	t.Helper()
	return e.apply(t, m)[0]
}

func (e *testEnv) control(t testing.TB, cs ...event.Control) []Result {
	t.Helper()
	res, err := e.store.ApplyControls(testCtx(t), cs)
	if err != nil {
		t.Fatalf("ApplyControls() error = %v", err)
	}
	return res
}

func (e *testEnv) sweep(t testing.TB, partition int32) Result {
	t.Helper()
	r, err := e.store.Sweep(testCtx(t), partition, 100)
	if err != nil {
		t.Fatalf("Sweep() error = %v", err)
	}
	return r
}

// admit makes company admitted at t0+at with the threshold of customers.
func (e *testEnv) admit(t testing.TB, company string, at time.Duration) {
	t.Helper()
	n := e.store.Rules().AdmissionThreshold
	for i := range n {
		e.applyOne(t, customerMsg(company, testCustomer(900+i), fmt.Sprintf("test:admit:%s:%d:%d", company, at.Microseconds(), i), at))
	}
}

func convKey(company, customer string) string {
	return string(event.ConversationKey(company, customer))
}

// conv returns the conversation hash, or nil.
func (e *testEnv) conv(t testing.TB, company, customer string) map[string]string {
	t.Helper()
	h, err := e.rdb.HGetAll(testCtx(t), DefaultKeyPrefix+"c:"+convKey(company, customer)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(h) == 0 {
		return nil
	}
	return h
}

// messageIDs returns the stored message IDs of a conversation in source
// time order.
func (e *testEnv) messageIDs(t testing.TB, company, customer string) []string {
	t.Helper()
	ids, err := e.rdb.ZRange(testCtx(t), DefaultKeyPrefix+"m:"+convKey(company, customer), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func (e *testEnv) hasText(t testing.TB, msgID string) bool {
	t.Helper()
	n, err := e.rdb.Exists(testCtx(t), DefaultKeyPrefix+"x:"+msgID).Result()
	if err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// outbox returns the events in the outbox of a partition.
func (e *testEnv) outbox(t testing.TB, partition int32) []event.Conversation {
	t.Helper()
	entries, err := e.store.Outbox(testCtx(t), partition, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]event.Conversation, len(entries))
	for i, en := range entries {
		if en.Err != nil {
			t.Fatalf("outbox entry %s: %v", en.ID, en.Err)
		}
		out[i] = en.Event
	}
	return out
}

// eventsOf returns the outbox events of one conversation.
func (e *testEnv) eventsOf(t testing.TB, company, customer string) []event.Conversation {
	t.Helper()
	var out []event.Conversation
	for _, ev := range e.outbox(t, 0) {
		if ev.CompanyID == company && ev.CustomerID == customer {
			out = append(out, ev)
		}
	}
	return out
}

func types(evs []event.Conversation) []event.ConversationType {
	out := make([]event.ConversationType, len(evs))
	for i, ev := range evs {
		out[i] = ev.Type
	}
	return out
}

// snapshot dumps the Redis state that must not depend on the order or the
// number of deliveries: every key except the event ID markers and the
// remembered control events, with lifetimes left out. Outbox entries are
// listed without their stream IDs.
func (e *testEnv) snapshot(t testing.TB) string {
	t.Helper()
	ctx := testCtx(t)
	keys, err := e.rdb.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		rest := strings.TrimPrefix(k, DefaultKeyPrefix)
		if strings.HasPrefix(rest, "ap:") || strings.HasPrefix(rest, "pd:") || strings.HasPrefix(rest, "pr:") {
			continue
		}
		typ, err := e.rdb.Type(ctx, k).Result()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "%s (%s):", k, typ)
		switch typ {
		case "hash":
			h, _ := e.rdb.HGetAll(ctx, k).Result()
			fields := make([]string, 0, len(h))
			for f, v := range h {
				fields = append(fields, f+"="+v)
			}
			slices.Sort(fields)
			fmt.Fprint(&b, fields)
		case "zset":
			z, _ := e.rdb.ZRangeWithScores(ctx, k, 0, -1).Result()
			fmt.Fprint(&b, z)
		case "set":
			s, _ := e.rdb.SMembers(ctx, k).Result()
			slices.Sort(s)
			fmt.Fprint(&b, s)
		case "stream":
			// Events of one conversation keep their order. The order
			// between conversations is not defined, so it is left out.
			xs, _ := e.rdb.XRange(ctx, k, "-", "+").Result()
			perConv := map[string][]string{}
			for _, x := range xs {
				fields := make([]string, 0, len(x.Values))
				for f, v := range x.Values {
					fields = append(fields, fmt.Sprintf("%s=%v", f, v))
				}
				slices.Sort(fields)
				id := fmt.Sprint(x.Values["co"], "|", x.Values["cu"])
				perConv[id] = append(perConv[id], fmt.Sprint(fields))
			}
			ids := make([]string, 0, len(perConv))
			for id := range perConv {
				ids = append(ids, id)
			}
			slices.Sort(ids)
			for _, id := range ids {
				fmt.Fprint(&b, perConv[id])
			}
		default:
			v, _ := e.rdb.Get(ctx, k).Result()
			fmt.Fprint(&b, v)
		}
		b.WriteString("\n")
	}
	return b.String()
}
