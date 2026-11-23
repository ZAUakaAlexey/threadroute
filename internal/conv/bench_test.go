package conv

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkCustomerMessagesAdmitted applies customer messages of admitted
// companies against miniredis, in pipelined batches of 100 like the
// Service does. miniredis runs Lua in Go and is much slower than real Redis,
// so the number is a lower bound and a guard against regressions.
func BenchmarkCustomerMessagesAdmitted(b *testing.B) {
	const companies, customers, batch = 20, 200, 100
	env := newTestEnv(b, testRules())
	for c := range companies {
		env.admit(b, fmt.Sprintf("did:plc:testbenchco%016d", c), 0)
	}
	msgs := make([]PartitionedMessage, 0, batch)
	ctx := testCtx(b)

	b.ResetTimer()
	start := time.Now()
	for i := range b.N {
		m := customerMsg(
			fmt.Sprintf("did:plc:testbenchco%016d", i%companies),
			testCustomer(i%customers),
			fmt.Sprintf("test:bench:%d", i),
			// One message per 10 ms of source time: sessions stay open.
			time.Duration(i)*10*time.Millisecond,
		)
		msgs = append(msgs, PartitionedMessage{Partition: int32(i % 16), Message: m})
		if len(msgs) == batch || i == b.N-1 {
			res, err := env.store.ApplyMessages(ctx, msgs)
			if err != nil {
				b.Fatal(err)
			}
			for _, r := range res {
				if r.Outcome != OutcomeStored {
					b.Fatalf("outcome = %q", r.Outcome)
				}
			}
			msgs = msgs[:0]
		}
	}
	b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "msgs/s")
}
