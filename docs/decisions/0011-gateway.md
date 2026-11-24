# 0011. Gateway: live data for browsers

Status: Proposed
Date: 2026-11-24

## Context

Stage 4 adds a console in the browser: an operator screen, a supervisor screen and a public page. The `gateway` service connects browsers to the pipeline over WebSocket (agreed in planning).

What exists today:

- The `conversations` topic (decision 0007) and the `routing` topic (decision 0009) carry every change of a conversation and of its routing, with a sequence number per key, and no post text. Both keep 24 hours.
- Message text lives only in Redis, written by `conv`, and in the `events` topic for 6 hours.
- Operator actions go through the HTTP API of the router (decision 0010). A Go client for it exists.
- Nothing publishes the presence of operators. The API has no way to list operators.
- When a post is deleted, `conv` removes its text from Redis but publishes no event about it.

A browser needs two things: the current state when it connects, and changes after that.

## Options and recommendations

### 1. Where the current state comes from

- **A. The gateway keeps its own view, built from the two topics.** At start it reads `conversations` and `routing` from the beginning and keeps the latest state of every key in memory. A restart is a new replay. One day is a few hundred thousand small events, so the replay takes seconds. The router is not changed, except that the presence of operators must be published as events (see "Contract changes").
- **B. The router gets read endpoints.** The gateway asks the router for a snapshot and then applies changes from the topics. The snapshot must carry the sequence numbers, so that the gateway can skip older events. The supervisor's operator list comes from the same endpoints.
- **C. The gateway reads the router's database tables.** The least code, but two services then depend on one schema.

Recommendation: **A**. The topics are already the record of what happened (decision 0001), every event carries a sequence number, and a view built only from them cannot disagree with them. B needs careful joining of a snapshot with a stream. C breaks the rule that a service owns its data.

### 2. Where message text comes from

- **A. The gateway reads text from Redis through a read-only function of the `conv` package.** One copy of the text. The key layout stays inside `conv`.
- **B. The gateway consumes the `events` topic and keeps its own copy of the text.** A second copy, and a second place that must handle deletions.
- **C. `conv` gets an HTTP endpoint for text.** One more API to keep stable.

Recommendation: **A**. Text is read when an operator opens a conversation and when a message is added to an open one. The gateway does not keep text after it has sent it.

### 3. Deleted posts on a screen

A browser that already shows a text must drop it when the author deletes the post (decision 0004).

- **A. `conv` publishes a "message removed" event** to `conversations`: the message ID and the conversation key, no text. The gateway tells the browsers that show this conversation to remove the text.
- **B. The gateway consumes `events.control` and finds the conversation itself.** This repeats the matching that `conv` already does.
- **C. No signal.** The browser reads the text again from time to time, so a deleted text stays on a screen until the next read.

Recommendation: **A**.

### 4. A slow browser

- **A. A queue per client with a limit; a full queue closes the connection.** The client connects again and gets the current state.
- **B. No queue of messages. The gateway remembers which keys changed for each client** and, on every tick, sends the current state of those keys. A slow client gets fewer and larger updates. The memory per client is limited by the number of keys it watches. A client whose write blocks for longer than a limit is closed.

Recommendation: **B**, with a tick of 250 milliseconds. Updates are states, not steps, so skipping an intermediate state loses nothing a person needs.

### 5. The WebSocket library

Checked on the Go module proxy on 2026-11-24:

| Library | Latest version | Released | Note |
|---|---|---|---|
| `github.com/coder/websocket` | v1.8.15 | 2026-06-15 | built on `net/http` and `context`, small API |
| `github.com/gorilla/websocket` | v1.5.3 | 2024-06-14 | the most widely used, no release for more than two years |
| `github.com/gobwas/ws` | v1.5.0 | 2026-11-03 | low level, more code on our side |

Recommendation: **`coder/websocket`**. It fits the code style of the project (`net/http`, `context` on every call) and it is maintained. The load test of stage 5 measures it; if it is the bottleneck, `gobwas/ws` is the next step.

### 6. What a client subscribes to

Not a fork, but the scope:

- An operator gets the offers and assignments of their own operator ID, and the text of the conversations they hold.
- A supervisor gets totals, queues by language, the operator list, teams and a list of conversations.
- The public page does not use WebSocket (decision 0013).

The gateway decides what a client may get from the identity it checked itself (decision 0012), never from what the client asks for.

## Choices confirmed by the project owner

On 2026-11-24 the project owner chose the recommended option in points 2 to 5: text is read from Redis through the `conv` package (2A), `conv` publishes a "message removed" event (3A), a slow browser gets the current state of changed keys without a queue (4B), and the library is `coder/websocket` (5). Point 1, where the current state comes from, is still open, so this decision stays proposed.

## Details that follow

- **One gateway process now.** It reads all partitions of both topics without a consumer group, from the beginning. A second process would do the same and hold the same view, so more processes can be added later behind the web server.
- **Actions.** The browser sends operator actions over the same WebSocket. The gateway calls the router's API with the existing client and returns the result. The gateway never changes routing state itself.
- **No post text in logs or metrics**, as in every other service.
- **Outages.** The stage 3 run showed that the router and the simulator did not stop when Kafka was paused, although the configuration says they should. The gateway does not rely on that behaviour. It reports "not ready" on its health endpoint while it cannot read the topics, and browsers show that the data is stale.
- **Where it runs** is not decided here. It depends on the numbers of the 24-hour measurement and gets its own decision.

## Contract changes this decision needs

- `internal/event`, conversation events: a new type "message removed" (option 3A).
- `internal/event`, routing events: presence events of an operator (online, offline, created or changed), keyed by the operator ID, with a sequence number per operator (option 1A).
- `internal/event`, routing events: a "queued" event that follows an assignment (operator went offline, or a customer wrote after a resolve) also carries the ID of the previous operator and of the ended offer. Today a consumer must remember them by conversation key.
- `internal/conv`: a read-only function for the text of one conversation (option 2A).

## Consequences

- One more service, and the first one that people reach from the internet.
- The gateway holds the state of one day in memory. Its size is measured in the first run.
- With option 1A the gateway is only as fresh as the topics: a delay of the router's outbox is a delay on the screen. In the stage 3 run that delay was under 0.1 seconds.
