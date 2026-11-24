// Package sim is the operator simulator (decision 0010). It plays a fixed
// set of operators against the operator API of the router and learns about
// offers from the routing topic. Nothing here is a real person: every
// number that comes out of a run with it describes simulated operators.
//
// # Start-up order
//
//  1. Read the settings of the router (speed factor, accept timeout,
//     default capacity) from the operator API. The simulator waits for the
//     router up to Config.StartWait and does not start without them.
//  2. Wait for the routing topic, which the router creates.
//  3. Create or update every operator: IDs sim-op-0001, sim-op-0002 and so
//     on, the languages from the scaled spread, each with "und" for the
//     general queue.
//  4. Read the end offsets of the routing topic and start the consumer
//     exactly there.
//  5. Put every operator offline.
//  6. Wait until the consumer has its partitions (up to StartWait), then
//     plan the shifts: an operator whose shift is in its online part goes
//     online at once, the others later. From here the scheduler runs and
//     /healthz answers 200.
//
// # Clean start
//
// The simulator keeps no state of its own across restarts: not the offers
// it was working on, not the shift plan, not the consumer position. A
// restart could therefore leave an operator in the router holding offers
// and conversations that the new process knows nothing about, so they
// would stay stuck with it. Step 5 prevents that: going offline ends every
// open offer of the operator and returns every conversation it holds to
// its queue (decision 0010), and the router then offers them again.
//
// The consumer is placed at the end offsets read in step 4, before step 5,
// so every routing event caused by the offline calls and later is read.
// The consumer group is kept for visibility and lag, but a committed offset
// from an earlier run is never used: the start position of a partition is
// the larger of its committed offset and the end offset read in step 4.
//
// # Time
//
// Every delay of the behaviour (accept delay, handling and resolve times,
// shifts) is model time. The simulator divides it by the speed factor of
// the router to get machine time, so operators and router timers keep the
// same proportion (decision 0009, "Clock"). All planned actions sit in one
// timer heap, ordered by model time, and one scheduler goroutine takes the
// due ones. The API calls run in a fixed set of workers; the calls of one
// operator always go to the same worker, in order. A call that gets a 503
// or no answer goes back into the heap with a backoff, so no worker sleeps.
//
// # Randomness
//
// One generator, seeded from the configuration, makes every decision. The
// draws happen at fixed points: the shift phases in operator order at
// start, the missed offer and the accept delay on "offered", the handling
// and resolve times on "accepted". So the same seed and the same event
// order give the same decisions, whatever the timing of the API answers.
//
// # Answers of the API
//
// "done" and "already_done" are success. A 409 rejection means the state
// moved on: what was planned for that offer is dropped and counted. A 503
// or no answer is retried with a backoff from 100 milliseconds up to 5
// seconds. When the router has given no answer for Config.OutageLimit, Run
// returns an error that wraps ErrOutage, so the restart policy starts the
// simulator again. Other error answers (400, 404, 500) are counted, logged
// and dropped.
package sim
