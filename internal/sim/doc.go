// Package sim is the operator simulator (decision 0010). It plays a set of
// operators against the operator API of the router and learns about offers
// from the routing topic. Nothing here is a real person: every
// number that comes out of a run with it describes simulated operators.
//
// # Start-up order
//
//  1. Read the settings of the router (speed factor, accept timeout,
//     default capacity) from the operator API. The simulator waits for the
//     router up to Config.StartWait and does not start without them. Then
//     report the sim values of the environment (see "Stand settings"); the
//     stored values decide the operator count, the capacity, the times and
//     the missed share.
//  2. Wait for the routing topic, which the router creates.
//  3. Create or update every operator: IDs sim-op-0001, sim-op-0002 and so
//     on to the count. The language of each number depends only on the
//     spread and the number (Spread.langOrder), so it is the same at every
//     start and after any change of the count. Each has "und" for the
//     general queue too.
//  4. Read the end offsets of the routing topic and start the consumer
//     exactly there.
//  5. Put every operator offline, and then the operators with higher
//     numbers that an earlier run left in the router, until the first
//     number the router does not know. Those count as put aside (see
//     "Stand settings"). The 404 of that first unknown number is expected
//     and not counted in the call metrics.
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
// # Stop
//
// On a clean stop (the context of Run ends), once the workers have
// returned, the simulator puts every operator offline through the API, so
// the router stops offering them conversations and returns what they hold
// to the queues. This is bounded to 10 seconds; operators not confirmed by
// then are counted in a log line and stay online in the router until the
// next start (step 5 above). After an error exit, for example an outage of
// the router, nothing is called: the restart does it.
//
// # Stand settings
//
// The router keeps the sim part of the stand settings (decision 0014, point
// 2A). After step 1 the simulator reports the values of its environment
// (opapi.PatternPutSimDefaults; a capacity of 0 is reported as the router
// default):
//
//   - Done or already done: the simulator runs with the stored current sim
//     part, which differs from the environment once the console changed
//     it, also across restarts.
//   - The router offers no stand settings (no stand block, or
//     CodeSettingsOff): it runs from its environment as before. It still
//     reads the settings every poll interval; when a stand block appears,
//     it reports and runs with the stored values from then on.
//   - Its environment is outside the bounds of opapi: a warning with the
//     fields, and it runs from its environment without live changes and
//     without the poll.
//   - No answer or a 503: retried up to Config.StartWait like the settings.
//
// Then it reads the settings every opapi.SettingsPollInterval. When the
// version changed and the sim part differs from the applied one, it applies
// it on the run, without a restart (point 3A): times and the missed share
// for the steps planned after the change, a new capacity sent to every
// active operator, and a new count. The active operators are always the
// numbers 1 to the count. Operators with higher numbers are put aside
// (offline, out of the shift cycle, still registered, every planned step
// dropped); for a higher count, numbers put aside come back and the missing
// ones are created, in number order. Each keeps the language of its number.
// So the operators and their languages depend only on the count, not on the
// changes or restarts before it. A failed poll changes nothing and counts
// on the outage clock like any call. See change.go.
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
// One generator, seeded from the configuration, makes every decision about
// offers. The draws happen at fixed points: the missed offer and the accept
// delay on "offered", the handling and resolve factors on "accepted". So
// the same seed and the same event order give the same decisions, whatever
// the timing of the API answers. The shift phase of an operator comes from
// the seed and its number alone, and every shift is counted from the start
// of the process, so an operator that a change creates or brings back has
// the same rhythm as if it had been there from the start. A run with
// changes still cannot be repeated from the seed alone (decision 0014): the
// changes come at times of their own and change which operators get offers.
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
