# 0008. Which companies are served

Status: Accepted
Date: 2026-11-23

## Context

In the conversation model, the author of the first post of any thread is a "company" (decision 0006). There are far too many of them to serve, and most get one reply and nothing more. The agreed rule is to serve only busy accounts: a company enters the system when more than N different people replied to it within one hour.

## Measurement

One 20-minute sample of the live post stream, 2026-11-23 from 20:28 UTC, counts only:

- 108,405 new posts, about 90 per second on average.
- The sample contains a burst: a second reader that ran at the same time saw about 34,000 events in 10 seconds near the end of the sample, with no source error. It was most likely a bulk import of posts. Without the burst the rate was about 60 per second.
- 74% were posts without a reply, 19% customer messages (about 17 per second), 4% replies by the thread author to another person, 4% thread authors continuing their own thread. The burst probably raised the share of posts without a reply.
- 9,549 thread authors received at least one reply from another person, in 17,720 company and customer pairs.
- In 11% of the pairs the thread author answered the customer.
- The busiest thread author had 280 different repliers.

| At least N different repliers in the sample | Companies | Conversations | Share of all customer messages |
|---|---|---|---|
| 1 | 9,549 | 17,720 | 100% |
| 2 | 2,478 | 10,649 | 61% |
| 3 | 1,121 | 7,935 | 45% |
| 5 | 432 | 5,637 | 32% |
| 10 | 150 | 3,859 | 21% |
| 20 | 53 | 2,606 | 14% |
| 50 | 17 | 1,629 | 9% |

The sample is 20 minutes long and the rule uses one hour, so over an hour each row will be larger. The sample was taken at an evening peak. A full hour, and a quiet hour, are measured in stage 2 before the value is fixed.

## Decision

- A company is admitted when at least N different customers replied to it within a sliding hour. N is a configuration value.
- The count is kept in Redis, per company, as a small set of customer IDs with a lifetime of one hour. The set stops growing at N members, because only "reached N or not" matters.
- An admitted company stays admitted while it is active. Admission ends after a period with no customer message.
- Admission is decided in `conv`, which owns conversation state. `normalize` does not filter by company.

## Which clock the windows use

Every time window in this decision is measured in source time: the witnessed time carried by the events, not the clock of the machine.

The reason is that the same code must give the same result in three cases: live traffic, a catch-up after an outage that delivers hours of events in a minute, and a load test that replays a recording at 50 times the live speed. With a wall clock, a replay would squeeze an hour of traffic into 72 seconds, and a window of "one hour" would cover 50 hours of events.

The service keeps a watermark, the highest source time it has processed, and compares deadlines with the watermark. In live traffic the watermark is the current time minus a fraction of a second. Lifetimes of keys in Redis are only a safety net for cleaning up and are never the window itself.

The alternative was the wall clock, which is simpler but makes replay tests behave differently from live traffic.

## Choices made

| Question | Choice | Other options considered |
|---|---|---|
| Threshold N | 10 different customers within an hour, to start | 5 (432 companies and 5,637 conversations in the sample), 20 (53 and 2,606). N stays a configuration value |
| Messages that arrived before admission | conversations start from the moment of admission | keeping an hour of all replies in Redis to open earlier conversations on admission |
| When admission ends | after 2 hours without a customer message | after 24 hours |
| Limit on the number of admitted companies | a configuration value that can be a maximum or no limit | a fixed limit, or no setting at all |

The limit setting accepts 0 for "no limit". With a maximum set, new companies are not admitted once it is reached, and a metric counts the refusals. The default value is chosen after the one-hour measurement in stage 2.

## A full hour, measured later

A 65-minute run of the whole pipeline on 2026-11-24 from 03:53 UTC (default settings, no company limit, a quiet hour of the day):

- 269 companies were admitted, and none of the admissions ended.
- 5,245 conversations opened and 2,290 closed. 26 were reopened in a new session.
- The load is concentrated: 10 companies had 29 percent of the conversations, 50 companies had 67 percent. Half of the companies had 8 conversations or fewer.
- The highest number of conversations open at the same time for one company was 126. For 43 companies it reached 20 or more.
- 98 percent of the sessions were in English at that hour, and 15 languages appeared in total.

The company limit stays at 0 (no limit) by default. These numbers are the base for choosing a limit for a small server.

## Details fixed before implementation

- **No sweeper for admission.** The windows are checked when a customer message arrives, against the source time of that message. The service adds the customer to the company's set with that time, drops entries older than one hour before it, and counts. The same check ends an admission: if the company's previous customer message is more than 2 hours older than the new one, the company must reach the threshold again.
- **Open conversations at the end of admission.** There are none to handle. Admission ends after 2 hours without a customer message, and a conversation closes after 30 minutes without a message, so every conversation of the company is already closed.
- **A company's customers sit in different partitions.** The admission state of a company is therefore shared state in Redis, updated atomically, and not local to one partition consumer.

## Consequences

- With N = 10, about one fifth of customer messages belong to admitted companies in the sample. The rest are dropped after counting.
- The value of N directly sets the load on Redis, the router and the console. It is the main control for fitting the system on a small server.
- Companies come and go within hours. The routing decision for dedicated teams (stage 3) must define what happens to a team when its company leaves.
- Admission state is shared between partitions, and each customer message is checked in the order it is processed. In live traffic all partitions are within a second of each other, so this equals source order. When partitions are far apart in source time, for example during a long catch-up, a later message of one partition can be processed before earlier messages of another. The result can then differ from live traffic: an admission can end early and the earlier messages are counted as not admitted. This is accepted for now and is measured in the load tests.
