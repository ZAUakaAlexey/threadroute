# 0007. Conversation lifecycle and state

Status: Accepted
Date: 2026-11-23

## Context

A conversation is a pair of a company and a customer (decision 0006). The `conv` service reads message events and control events, keeps the state of open conversations, and tells the router when a conversation needs an operator.

Live state goes to Redis and long-lived routing data to PostgreSQL (agreed earlier). Post text must have a short lifetime and must be removed when the author deletes it (decision 0004).

No real agent answers anyone. Operators are simulated. The only real reply in the data is the one the thread author writes themselves.

## Decision

### Lifecycle

1. **Opened.** The first customer message of an admitted company (decision 0008) opens a conversation.
2. **Updated.** Every later message of the same pair is added. Messages are ordered by source time, not by arrival.
3. **Answered by the company.** When the thread author replies to the customer, the time since the customer's first unanswered message is recorded as the real response time. It is kept as a metric next to the simulated response time, so the two can be compared.
4. **Closed.** No new message for the inactivity timeout.
5. **Reopened.** A new customer message after closing starts a new session of the same conversation. The router is told the previous operator, so it can prefer them.

### Events for other services

`conv` publishes conversation events (opened, message added, answered by the company, closed) to a topic `conversations`, keyed by the conversation key, with 16 partitions. The router and the gateway consume it. These events carry IDs, language, counters and times, but no post text.

### State in Redis

- One hash per open conversation: company, customer, language, counters, times, session number.
- The messages of a conversation as a sorted set by source time, with a limit on the number kept.
- An index from message ID to conversation, and from author to their messages, for deletions and author removals.
- A sorted set of close deadlines, checked by a sweeper.

### Deletions

- A deletion removes the message text and its index entries at once. Counters stay.
- An author removal deletes every stored message of that author. If the author was the customer of a conversation, the conversation is closed.

## Which clock the windows use

Every time window in this decision is measured in source time: the witnessed time carried by the events, not the clock of the machine.

The reason is that the same code must give the same result in three cases: live traffic, a catch-up after an outage that delivers hours of events in a minute, and a load test that replays a recording at 50 times the live speed. With a wall clock, a replay would squeeze an hour of traffic into 72 seconds, and a window of "one hour" would cover 50 hours of events.

The service keeps a watermark, the highest source time it has processed, and compares deadlines with the watermark. In live traffic the watermark is the current time minus a fraction of a second. Lifetimes of keys in Redis are only a safety net for cleaning up and are never the window itself.

The alternative was the wall clock, which is simpler but makes replay tests behave differently from live traffic.

## Choices made

| Question | Choice | Other options considered |
|---|---|---|
| Inactivity timeout | 30 minutes | 10 minutes (many reopenings), 60 minutes (more open conversations in memory) |
| A company message that arrives when no conversation is open | drop it and count it | hold it for up to 30 seconds. The counter shows how often this happens, so the rule can be changed with numbers |
| How long message text is kept | until the conversation closes, plus 10 minutes | a fixed hour from arrival, which can remove text from an open conversation |
| Messages kept per conversation | the last 50 | older ones are dropped from Redis and only counted |

## Consequences

- The real response time exists only for conversations where the thread author answered. Its share is measured in stage 2.
- Ordering by source time means a late message can be inserted before messages that were already shown. The gateway must handle that.
- A sweeper closes conversations, so closing can be late by one sweep interval.
- Redis memory grows with the number of open conversations. It is measured, and admission (decision 0008) is the control for it.
