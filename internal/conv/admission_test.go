package conv

import (
	"fmt"
	"testing"
	"time"
)

func TestAdmission(t *testing.T) {
	type step struct {
		customer string
		at       time.Duration
		want     string // outcome
	}
	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "below the threshold nothing opens",
			steps: []step{
				{testAlice, 0, OutcomeNotAdmitted},
				{testBob, time.Minute, OutcomeNotAdmitted},
			},
		},
		{
			name: "the third different customer admits and opens",
			steps: []step{
				{testAlice, 0, OutcomeNotAdmitted},
				{testBob, time.Minute, OutcomeNotAdmitted},
				{testCarol, 2 * time.Minute, OutcomeStored},
			},
		},
		{
			name: "the same customer many times does not admit",
			steps: []step{
				{testAlice, 0, OutcomeNotAdmitted},
				{testAlice, time.Minute, OutcomeNotAdmitted},
				{testAlice, 2 * time.Minute, OutcomeNotAdmitted},
				{testAlice, 3 * time.Minute, OutcomeNotAdmitted},
				{testBob, 4 * time.Minute, OutcomeNotAdmitted},
			},
		},
		{
			name: "entries older than the window are dropped",
			steps: []step{
				{testAlice, 0, OutcomeNotAdmitted},
				{testBob, 10 * time.Minute, OutcomeNotAdmitted},
				// Alice is more than one hour older than this message.
				{testCarol, 61 * time.Minute, OutcomeNotAdmitted},
				{testAlice, 62 * time.Minute, OutcomeStored},
			},
		},
		{
			name: "an entry exactly one window old still counts",
			steps: []step{
				{testAlice, 0, OutcomeNotAdmitted},
				{testBob, 10 * time.Minute, OutcomeNotAdmitted},
				{testCarol, time.Hour, OutcomeStored},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(t, testRules())
			for i, s := range tt.steps {
				r := env.applyOne(t, customerMsg(testCompany, s.customer, fmt.Sprintf("test:m:%d", i), s.at))
				if r.Outcome != s.want {
					t.Fatalf("step %d: outcome = %q, want %q", i, r.Outcome, s.want)
				}
			}
		})
	}
}

func TestAdmissionStopsGrowingAtThreshold(t *testing.T) {
	r := testRules()
	r.CompanyLimit = 1
	env := newTestEnv(t, r)
	env.admit(t, testCompany2, 0)
	for i := range 6 {
		env.applyOne(t, customerMsg(testCompany, testCustomer(i), fmt.Sprintf("test:m:%d", i), time.Duration(i)*time.Minute))
	}
	n, err := env.rdb.ZCard(testCtx(t), DefaultKeyPrefix+"cand:"+testCompany).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("candidate set has %d members, want 3", n)
	}
}

func TestAdmissionEndsAfterIdle(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)

	// Within the idle period: still admitted.
	if r := env.applyOne(t, customerMsg(testCompany, testAlice, "test:m:1", 90*time.Minute)); r.Outcome != OutcomeStored {
		t.Fatalf("within idle: outcome = %q, want stored", r.Outcome)
	}
	// More than 2 hours after the previous customer message: ended.
	r := env.applyOne(t, customerMsg(testCompany, testBob, "test:m:2", 90*time.Minute+2*time.Hour+time.Second))
	if r.Outcome != OutcomeNotAdmitted || r.Ended != 1 {
		t.Fatalf("after idle: outcome = %q, ended = %d, want not_admitted and 1", r.Outcome, r.Ended)
	}
	// It must be earned again: Bob counts, two more are needed.
	base := 90*time.Minute + 2*time.Hour + time.Minute
	if r := env.applyOne(t, customerMsg(testCompany, testCarol, "test:m:3", base)); r.Outcome != OutcomeNotAdmitted {
		t.Fatalf("second customer: outcome = %q, want not_admitted", r.Outcome)
	}
	r = env.applyOne(t, customerMsg(testCompany, testAlice, "test:m:4", base+time.Minute))
	if r.Outcome != OutcomeStored || r.Admitted != 1 {
		t.Fatalf("third customer: outcome = %q, admitted = %d, want stored and 1", r.Outcome, r.Admitted)
	}
}

func TestCompanyLimit(t *testing.T) {
	r := testRules()
	r.CompanyLimit = 1
	env := newTestEnv(t, r)
	env.admit(t, testCompany, 0)

	// The second company reaches the threshold but the limit is reached.
	var refused int
	for i := range 4 {
		res := env.applyOne(t, customerMsg(testCompany2, testCustomer(i), fmt.Sprintf("test:b:%d", i), 90*time.Minute+time.Duration(i)*time.Minute))
		if res.Outcome != OutcomeNotAdmitted {
			t.Fatalf("message %d: outcome = %q, want not_admitted", i, res.Outcome)
		}
		refused += res.Refused
	}
	if refused != 2 {
		t.Errorf("refusals = %d, want 2 (third and fourth customer)", refused)
	}

	// After the idle period of the first company its admission no longer
	// counts, so the second company is admitted on its next message.
	res := env.applyOne(t, customerMsg(testCompany2, testCustomer(5), "test:b:5", 2*time.Hour+time.Minute))
	if res.Outcome != OutcomeStored || res.Admitted != 1 {
		t.Errorf("after idle of the first company: outcome = %q, admitted = %d", res.Outcome, res.Admitted)
	}
}

func TestCompanyLimitZeroMeansNoLimit(t *testing.T) {
	env := newTestEnv(t, testRules())
	for c := range 20 {
		company := fmt.Sprintf("did:plc:testcompany%014d", c)
		env.admit(t, company, time.Duration(c)*time.Second)
		r := env.applyOne(t, customerMsg(company, testAlice, fmt.Sprintf("test:c:%d", c), time.Duration(c)*time.Second))
		if r.Outcome != OutcomeStored {
			t.Fatalf("company %d: outcome = %q, want stored", c, r.Outcome)
		}
	}
	st, err := env.store.Stats(testCtx(t), 16)
	if err != nil {
		t.Fatal(err)
	}
	if st.AdmittedCompanies != 20 {
		t.Errorf("admitted companies = %d, want 20", st.AdmittedCompanies)
	}
}

func TestAdmittedCountIgnoresIdleCompanies(t *testing.T) {
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	env.admit(t, testCompany2, 3*time.Hour)
	st, err := env.store.Stats(testCtx(t), 16)
	if err != nil {
		t.Fatal(err)
	}
	// The newest watermark is t0+3h. The first company had no customer
	// message for 3 hours, so its admission has ended lazily.
	if st.AdmittedCompanies != 1 {
		t.Errorf("admitted companies = %d, want 1", st.AdmittedCompanies)
	}
}

func TestCompanyMessageOfNotAdmittedCompany(t *testing.T) {
	env := newTestEnv(t, testRules())
	r := env.applyOne(t, companyMsg(testCompany, testAlice, "test:m:1", 0))
	if r.Outcome != OutcomeNotAdmitted {
		t.Errorf("outcome = %q, want %q", r.Outcome, OutcomeNotAdmitted)
	}
	if got := env.outbox(t, 0); len(got) != 0 {
		t.Errorf("outbox has %d events, want 0", len(got))
	}
}
