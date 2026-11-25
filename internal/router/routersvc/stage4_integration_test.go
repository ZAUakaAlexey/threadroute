//go:build integration

package routersvc

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// testOperatorEventsInTopic: operator events, the refresh and the previous
// pair of requeues reach the routing topic in order per family and key,
// and a "message removed" event writes nothing (decision 0011).
func testOperatorEventsInTopic(t *testing.T, h *harness) {
	sc := h.newScenario(t)
	// No offer expires while the test works with it, also on a slow
	// machine: every requeue below has its own reason, not offer_expired.
	sc.rcfg.AcceptTimeout = longAcceptTimeout
	// A long refresh, so no refresh comes between the steps of the test
	// (it would change the operator sequence numbers it checks). At the end
	// the test clock moves past it, and the loop publishes the snapshots
	// again.
	sc.refresh = 3 * time.Minute
	// The team is not released by time, so the refresh is the only change
	// of the operators there. The release time is the highest that the
	// stand settings allow (2 hours of model time). The move of the clock
	// at the end is the refresh plus a second of machine time, about 60
	// minutes of model time at the speed factor of 20, which leaves about
	// 60 minutes of model time (3 minutes of machine time) for the rest of
	// the test after the team load fell to 0.
	sc.rcfg.TeamReleaseAfter = opapi.MaxTeamReleaseAfter
	// Small teams, so three conversations of one company form one.
	sc.rcfg.TeamFormLoad, sc.rcfg.TeamReleaseLoad, sc.rcfg.TeamMinSize, sc.rcfg.TeamMaxSize = 3, 1, 1, 1
	// The test is about operator events, not the team cap. When the team
	// forms, one of the two operators is offline, and the default share of
	// one half of one online operator gives a cap of 0, so the cap is off
	// here.
	sc.rcfg.TeamMaxShare = 1
	r := sc.start()
	ctx := t.Context()
	api := r.api
	rd := sc.reader()

	s, err := api.Settings(ctx)
	if err != nil || s.AssignmentTargetUS != testSLATarget.Microseconds() || s.FirstResponseTargetUS != testSLATarget.Microseconds() {
		t.Fatalf("settings %+v, %v", s, err)
	}
	a, b := opID(1), opID(2)
	for _, op := range []string{a, b} {
		if _, err := api.PutOperator(ctx, op, opapi.OperatorRequest{Languages: []string{"en"}, Capacity: 1}); err != nil {
			t.Fatal(err)
		}
		wantResult(t, "online "+op, opapi.ResultDone)(api.Online(ctx, op))
		wantResult(t, "online again "+op, opapi.ResultAlreadyDone)(api.Online(ctx, op))
	}
	online := rd.waitEvent(a, "operator a online", isType(event.TypeOperatorOnline))
	if online.Seq != 2 || online.Presence != event.PresenceOnline || online.Capacity != 1 || len(online.Languages) != 1 {
		t.Errorf("online event %+v", online)
	}

	// offer_refused carries the refused offer.
	c := newStream(company(1), customer(1))
	sc.produce(c.opened())
	first := rd.waitEvent(c.key(), "first offer", isType(event.TypeOffered))
	wantResult(t, "refuse", opapi.ResultDone)(api.Refuse(ctx, first.OperatorID, first.OfferID))
	q := rd.waitEvent(c.key(), "queued after refuse", func(ev event.Routing) bool {
		return ev.Type == event.TypeQueued && ev.QueueReason == event.QueueOfferRefused
	})
	if q.PreviousOperatorID != first.OperatorID || q.PreviousOfferID != first.OfferID {
		t.Errorf("queued after refuse %+v, want previous %s %s", q, first.OperatorID, first.OfferID)
	}

	// A "message removed" event moves the applied number and writes
	// nothing. The conversation is resolved here, so no timer or worker
	// writes an event for it at the same time.
	second := rd.waitEvent(c.key(), "second offer", func(ev event.Routing) bool {
		return ev.Type == event.TypeOffered && ev.OfferID != first.OfferID
	})
	wantResult(t, "accept", opapi.ResultDone)(api.Accept(ctx, second.OperatorID, second.OfferID))
	wantResult(t, "resolve", opapi.ResultDone)(api.Resolve(ctx, second.OperatorID, second.OfferID))
	before := sc.count(`SELECT route_seq FROM conversations WHERE customer_id = $1`, c.customer)
	rm := c.removed()
	sc.produce(rm)
	waitFor(t, 30*time.Second, "removed event applied", func() bool {
		return sc.count(`SELECT conv_seq FROM conversations WHERE customer_id = $1`, c.customer) == int64(rm.Seq)
	})
	if n := sc.count(`SELECT route_seq FROM conversations WHERE customer_id = $1`, c.customer); n != before {
		t.Errorf("routing events of the conversation after the removal %d, want %d", n, before)
	}
	if n := sc.count(`SELECT count(*) FROM conversations WHERE customer_id = $1 AND state = 'resolved'`, c.customer); n != 1 {
		t.Error("the removal changed the state of the conversation")
	}

	// customer_message_after_resolve carries the resolved assignment.
	sc.produce(c.message())
	q = rd.waitEvent(c.key(), "queued after the customer message", func(ev event.Routing) bool {
		return ev.Type == event.TypeQueued && ev.QueueReason == event.QueueCustomerMessage
	})
	if q.PreviousOperatorID != second.OperatorID || q.PreviousOfferID != second.OfferID {
		t.Errorf("queued after the customer message %+v, want previous %s %s", q, second.OperatorID, second.OfferID)
	}

	// operator_offline carries the offer of the operator who went offline.
	third := rd.waitEvent(c.key(), "third offer", func(ev event.Routing) bool {
		return ev.Type == event.TypeOffered && ev.OfferID != first.OfferID && ev.OfferID != second.OfferID
	})
	wantResult(t, "offline", opapi.ResultDone)(api.Offline(ctx, third.OperatorID))
	q = rd.waitEvent(c.key(), "queued after offline", func(ev event.Routing) bool {
		return ev.Type == event.TypeQueued && ev.QueueReason == event.QueueOperatorOffline
	})
	if q.PreviousOperatorID != third.OperatorID || q.PreviousOfferID != third.OfferID {
		t.Errorf("queued after offline %+v, want previous %s %s", q, third.OperatorID, third.OfferID)
	}
	rd.waitEvent(third.OperatorID, "operator offline event", isType(event.TypeOperatorOffline))
	sc.produce(c.closed())
	rd.waitEvent(c.key(), "ended", isType(event.TypeEnded))

	// A team: three conversations of another company form it, and the
	// member gets an operator event with the company. The team family is
	// in the topic next to the other two.
	co := company(2)
	var team []*convStream
	for i := range 3 {
		s := newStream(co, customer(10+i))
		team = append(team, s)
		sc.produce(s.opened())
	}
	formed := rd.waitEvent(co, "team formed", isType(event.TypeTeamFormed))
	if len(formed.Members) != 1 {
		t.Fatalf("team_formed %+v", formed)
	}
	member := formed.Members[0]
	joined := rd.waitEvent(member, "operator event of the team join", func(ev event.Routing) bool {
		return ev.Type == event.TypeOperatorUpdated && ev.DedicatedCompanyID == co
	})
	if joined.Presence != event.PresenceOnline {
		t.Errorf("team join event %+v", joined)
	}
	for _, s := range team {
		sc.produce(s.closed())
	}
	for _, s := range team {
		rd.waitEvent(s.key(), "ended "+s.customer, isType(event.TypeEnded))
	}

	// The refresh publishes every operator again, with the next number.
	// Nothing else changes the operators from here on, so a higher op_seq
	// comes from the refresh. The numbers are read before the clock moves,
	// so a refresh that comes at once is not missed.
	seqs := map[string]int64{}
	for _, op := range []string{a, b} {
		seqs[op] = sc.count(`SELECT op_seq FROM operators WHERE id = $1`, op)
	}
	sc.clock.advance(sc.refresh + time.Second)
	for _, op := range []string{a, b} {
		seq := seqs[op]
		waitFor(t, 30*time.Second, "a refresh of "+op, func() bool {
			return sc.count(`SELECT op_seq FROM operators WHERE id = $1`, op) > seq
		})
		ev := rd.waitEvent(op, "the refresh of "+op+" in the topic", func(ev event.Routing) bool {
			return ev.Seq == uint64(seq+1)
		})
		if ev.Type != event.TypeOperatorUpdated {
			t.Errorf("refresh of %s is %s", op, ev.Type)
		}
	}
	if got := testutil.ToFloat64(r.svc.m.refreshed); got < 2 {
		t.Errorf("operators_refreshed_total = %v, want at least 2", got)
	}

	waitFor(t, 30*time.Second, "outbox sent", func() bool { return sc.unsent() == 0 })
	sc.matchOutbox(rd)
	for _, typ := range []event.RoutingType{event.TypeOperatorUpdated, event.TypeOperatorOnline, event.TypeOperatorOffline} {
		if got := testutil.ToFloat64(r.svc.m.published.WithLabelValues(string(typ))); got < 1 {
			t.Errorf("routing_published_total{type=%q} = %v", typ, got)
		}
	}
	if got := testutil.ToFloat64(r.svc.m.applied.WithLabelValues(string(event.TypeMessageRemoved), resultApplied)); got != 1 {
		t.Errorf("events_applied_total{message_removed, applied} = %v, want 1", got)
	}
}
