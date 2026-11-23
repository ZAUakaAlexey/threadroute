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

### How operators learn about offers

Operators are told through the `routing` topic (decision 0009). The simulator consumes it. In stage 4 the gateway consumes it and pushes to browsers.

### The simulator

`sim` is a separate service that plays a fixed set of operators.

- **The set.** Operators are created from the configuration at start: how many, and how their languages are spread. The spread follows the measured languages of the stream, so that queues of small languages exist and can run short.
- **Presence.** Each operator works in shifts: online for a while, then offline. Going offline while holding conversations returns them to the queue, which exercises reassignment.
- **Accepting.** An operator accepts an offer after a short random delay. With a small probability they do not answer at all, so the accept timeout and the next offer are exercised.
- **Replying and resolving.** After accepting, the operator sends the reply marker after a random handling time, and resolves the conversation some time after that. The times come from a distribution with a long tail, because real handling times look like that.
- **Speed.** All simulator delays are divided by the same speed factor as the router timers (decision 0009).
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
