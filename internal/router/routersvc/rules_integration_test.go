//go:build integration

package routersvc

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// testRoutingRules: the rules of 2026-11-24 through the running service
// and its HTTP API (decision 0009, "Changes after the first live run of
// the console"). An operator with priority 1 wins a tie, a conversation in
// a language that no online operator has goes to the general queue, and
// offers_general_queue_total counts only those offers.
func testRoutingRules(t *testing.T, h *harness) {
	sc := h.newScenario(t)
	sc.rcfg.AcceptTimeout = longAcceptTimeout
	r := sc.start()
	ctx := t.Context()
	api := r.api

	// opID(1) sorts first, but opID(2) has priority 1.
	for _, op := range []struct {
		id       string
		priority int
	}{{opID(1), 0}, {opID(2), 1}} {
		resp, err := api.PutOperator(ctx, op.id, opapi.OperatorRequest{Languages: []string{"en", "und"}, Capacity: 2, Priority: op.priority})
		if err != nil || resp.Result != opapi.ResultDone || resp.Operator.Priority != op.priority {
			t.Fatalf("put %s: %+v, %v", op.id, resp, err)
		}
		wantResult(t, "online "+op.id, opapi.ResultDone)(api.Online(ctx, op.id))
	}

	rd := sc.reader()
	c1 := newStream(company(1), customer(1))
	sc.produce(c1.opened())
	if off := rd.waitEvent(c1.key(), "offer of c1", isType(event.TypeOffered)); off.OperatorID != opID(2) {
		t.Fatalf("c1 offered to %s, want %s (same load, priority 1)", off.OperatorID, opID(2))
	}

	// Nobody has "pl": the general queue takes it. opID(1) has the lower
	// load now.
	c2 := newStream(company(1), customer(2))
	c2.lang = "pl"
	sc.produce(c2.opened())
	off := rd.waitEvent(c2.key(), "offer of c2", isType(event.TypeOffered))
	if off.OperatorID != opID(1) {
		t.Fatalf("c2 offered to %s, want %s", off.OperatorID, opID(1))
	}
	waitFor(t, 10*time.Second, "general queue counter", func() bool {
		return testutil.ToFloat64(r.svc.m.generalQueue) == 1
	})
	if got := testutil.ToFloat64(r.svc.m.offers); got != 2 {
		t.Errorf("offers_total = %v, want 2", got)
	}

	// A PUT without a priority sets it back to 0.
	resp, err := api.PutOperator(ctx, opID(2), opapi.OperatorRequest{Languages: []string{"en", "und"}, Capacity: 2})
	if err != nil || resp.Result != opapi.ResultDone || resp.Operator.Priority != 0 {
		t.Fatalf("put without priority: %+v, %v", resp, err)
	}
	_, err = api.PutOperator(ctx, opID(2), opapi.OperatorRequest{Languages: []string{"en"}, Priority: opapi.MaxPriority + 1})
	if e, ok := errors.AsType[*opapi.Error](err); !ok || e.Code != opapi.CodeInvalidRequest {
		t.Fatalf("priority 10: %v, want %s", err, opapi.CodeInvalidRequest)
	}
	if got := testutil.ToFloat64(r.svc.m.generalQueue); got != 1 {
		t.Errorf("offers_general_queue_total = %v, want 1", got)
	}
}
