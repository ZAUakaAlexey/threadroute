# 0008. Which companies are served

Status: Proposed
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

- A company is admitted when N different customers replied to it within a sliding hour. N is a configuration value.
- The count is kept in Redis, per company, as a small set of customer IDs with a lifetime of one hour. The set stops growing at N members, because only "reached N or not" matters.
- An admitted company stays admitted while it is active. Admission ends after a period with no customer message.
- Admission is decided in `conv`, which owns conversation state. `normalize` does not filter by company.

## Open choices

### 1. The threshold N

| Option | In the 20-minute sample | Note |
|---|---|---|
| 5 | 432 companies, 5,637 conversations | many small companies, the highest load |
| **10 (recommended to start)** | 150 companies, 3,859 conversations | several thousand live conversations, which is the scale the console is built for, with a company count that is easy to show |
| 20 | 53 companies, 2,606 conversations | fewer companies, each one clearly busy |

### 2. Messages that arrived before a company was admitted

The first N-1 customers wrote before the company reached the threshold.

| Option | Note |
|---|---|
| **Start from the moment of admission (recommended)** | simple and cheap. Conversations of the earlier customers open only if they write again |
| Keep messages of not yet admitted companies for one hour and open their conversations on admission | closer to a real onboarding, but it keeps an hour of all replies in Redis (about 60,000 messages at the sampled rate) |

### 3. When admission ends

| Option | Note |
|---|---|
| **After 2 hours without a customer message (recommended)** | a company leaves soon after its thread goes quiet |
| After 24 hours | more stable set of companies, more idle ones |

### 4. A safety limit on the number of admitted companies

The agreed rule has no upper limit. A day with many viral threads could admit far more companies than the server can hold.

| Option | Note |
|---|---|
| **A configurable maximum, set high (recommended)** | when it is reached, new companies are not admitted and a metric counts the refusals. Normal days never touch it |
| No limit | follows the agreed rule exactly, with a risk of running out of memory on a small server |

## Consequences

- With N = 10, about one fifth of customer messages belong to admitted companies in the sample. The rest are dropped after counting.
- The value of N directly sets the load on Redis, the router and the console. It is the main control for fitting the system on a small server.
- Companies come and go within hours. The routing decision for dedicated teams (stage 3) must define what happens to a team when its company leaves.
