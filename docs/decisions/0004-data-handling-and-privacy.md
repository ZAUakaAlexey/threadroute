# 0004. Data handling and privacy

Status: Accepted
Date: 2026-11-23

## Context

The source data is public posts written by real people. Authors can delete a post or an account at any time, and Bluesky asks services that store content to honour deletions. Bluesky also recommends care when content is shown to visitors who are not logged in. Labels for adult or abusive content arrive late and cover only a small share of posts.

This project is a personal, non-commercial demo. It does not need post text for its public page.

## Decision

### What is stored and for how long

| Place | Content | Lifetime |
|---|---|---|
| `raw.bluesky` topic | source events with post text and author IDs | 24 hours, then removed by topic retention (up to about 25 hours, because Kafka deletes whole segments) |
| `events` topic | normalised message events with post text and author IDs | 6 hours (decision 0006) |
| `events.control` topic | deletions and author removals: IDs only, no text | 6 hours |
| `conversations` topic | conversation events: IDs, language, counters and times, no text | 24 hours |
| `routing` topic | routing events: author IDs, operator IDs (synthetic), language, times and counters, no text | 24 hours (decision 0009) |
| Redis, in memory | open conversations and recent message text | until the conversation closes plus 10 minutes (decision 0007), removed at once on a deletion event |
| Redis, append-only file on disk | every write to Redis, so also message text | until the next rewrite of the file, see "Deletions" |
| PostgreSQL | conversation and routing metadata, no post text | kept for statistics |
| Recordings for load tests | source events with post text and author IDs | private, 14 days, see below |

### Deletions

- A deleted post is removed from Redis as soon as the deletion event is processed.
- Redis keeps its data on disk in an append-only file, so that a restart does not lose the conversation state. The file is a log of writes. A removed text is gone from memory at once, but the write that stored it stays in the file until Redis rewrites the file. A rewrite starts when the file has grown by half since the last one. How long that takes on live traffic is measured, and a rewrite on a timer is added if it is longer than the 6 hours of the `events` topic. The project owner chose the file on disk over keeping Redis in memory only.
- An account event with a non-active status, or a `sync` event, removes all stored messages of that author from Redis.
- A single record cannot be removed from a Kafka topic. A deleted post leaves `raw.bluesky` within about 25 hours and `events` within about 7 hours.
- This document does not promise immediate removal everywhere. It describes what the system really does.

### Recordings for load tests

- Load tests replay recorded traffic. Recordings hold real text and real author IDs.
- Recordings are never stored in this repository. The paths are in `.gitignore`, and the default location is outside the project tree.
- A recording is copied only to the machine that runs a load test and is deleted with that machine after the test.
- A recording is deleted 14 days after it was made. This is long enough to repeat a test with the same input and compare results before and after an optimisation.
- Recordings can contain posts that were deleted later. This is an accepted risk for a private file with a limited lifetime.

### Repository

- Tests, examples and documentation use synthetic data only, with identifiers that are clearly fake.
- Logs never contain post text.

### Public page

- The public page shows aggregated metadata only: language, conversation counts, queue lengths, timers, event rates. It shows no post text and no author IDs or names.
- Post text is visible only after login, in the operator console.
- The page names the data source and links to this document.
- A configuration flag puts the whole page behind login.

## Consequences

- The public demo is less striking than a page with live text, but it carries no content or moderation risk.
- The 24-hour retention of `raw.bluesky` needs disk space for one day of posts. The topic has a total size limit (decision 0001), and disk space is checked before deployment.
- The project owner is responsible for the personal data in `raw.bluesky`, Redis and the recordings while they exist.
