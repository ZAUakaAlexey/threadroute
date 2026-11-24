# 0010. Operators: the operator API and the simulator

Status: Accepted
Date: 2026-11-23

## Context

No real agent answers anyone in this project. Operators are simulated (decision 0007). Stage 4 adds a console where a person can act as an operator. The simulator and the console should use the same path, so that the console does not need a second implementation and the load tests exercise the real one.

## Decision

### Operator API

The router exposes an HTTP API for operator actions: go online or offline, accept or refuse an offer, send a reply marker, resolve a conversation. The simulator calls this API today. The gateway calls it for the console in stage 4.

- A reply is only a marker with a time in this stage. Nothing is posted to Bluesky, and no text is created. In stage 6 an LLM suggests a draft reply to the operator in the console. The draft is shown to the operator only and is still never posted to Bluesky.
- Every action is checked against the current state in the database, so a late or repeated call cannot break it: accepting an expired offer fails cleanly, resolving twice does nothing.
- The API is not reachable from the internet in stage 3. Authentication with the site's tokens is added with the gateway in stage 4.

### Details fixed before implementation

- **Offer identity.** Every offer has its own ID. Accept, refuse, reply and resolve name the operator and the offer ID, so a late call about an old offer can never act on a newer one. After an accept the same ID names the assignment.
- **Three outcomes.** A call answers "done", "already done" (a repeat, nothing changed) or a rejection with a reason code (the state moved on, for example the offer expired or the conversation was closed). Resolving a conversation that `conv` has already closed is a rejection, not "already done", because the operator did not end it.
- **Replies.** A reply marker carries an ID chosen by the caller, so a repeated call does not record a second reply.
- **Operators.** The simulator creates its operators through the API at start. A new operator is offline.
- **Priority.** Since 2026-11-24 the request that creates or changes an operator may carry a priority from 0 to 9. It is 0 when it is absent, and the answer of the API shows it. The router uses it only to break a tie between operators with the same load (decision 0009). The gateway sends 1 for the operators of people, and the simulator sends none.

### How operators learn about offers

Operators are told through the `routing` topic (decision 0009). The simulator consumes it. In stage 4 the gateway consumes it and pushes to browsers.

### The simulator

`sim` is a separate service that plays a fixed set of operators.

- **The set.** Operators are created from the configuration at start: how many, and how their languages are spread. The spread follows the measured languages of the stream, so that queues of small languages exist and can run short.
- **Presence.** Each operator works in shifts: online for a while, then offline. Going offline while holding conversations returns them to the queue, which exercises reassignment.
- **Accepting.** An operator accepts an offer after a short random delay. With a small probability they do not answer at all, so the accept timeout and the next offer are exercised.
- **Replying and resolving.** After accepting, the operator sends the reply marker after a random handling time, and resolves the conversation some time after that. The times come from a distribution with a long tail, because real handling times look like that.
- **Speed.** All simulator delays are divided by the same speed factor as the router timers (decision 0009). The simulator reads the factor from the router's API at start and does not start without it.
- **Repeatable runs.** The random generator takes a seed from the configuration, so a load test can be repeated with the same operator behaviour.

## Starting values

| Setting | Start value | Note |
|---|---|---|
| Number of operators | 200 | sized after the first live run of stage 2, from the number of open conversations |
| Languages | spread like the measured stream, every operator also handles the general queue | |
| Shift | 4 hours online, 30 minutes offline, with random offsets | |
| Accept delay | 1 to 8 seconds | |
| Missed offers | 3 percent | |
| Handling time until the reply | median 45 seconds, long tail | |
| Time from reply to resolve | median 3 minutes | |

## Choices confirmed by the project owner

- **One HTTP API for the simulator and the console.** Alternatives were operators simulated inside the router, and operator commands sent through Kafka.
- **The reply is a marker now, with LLM draft replies in stage 6.** Alternatives were a marker only, and a fixed placeholder text.

## Alternatives considered

- **Operators simulated inside the router.** Fewer services, but the console would need a different path, and the load tests would not exercise the API.
- **Operators that pull work.** Not chosen, see decision 0009.

## Consequences

- One more service and one more API to keep stable.
- The numbers that come out of this stage (waiting time, missed SLA targets) describe the simulated operators, not real people. Every report of them must say so.
- The simulator needs the routing events in order per conversation. It skips events it has already seen.
