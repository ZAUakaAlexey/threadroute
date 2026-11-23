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
| Redis | open conversations and recent message text | short TTL, removed at once on a deletion event |
| PostgreSQL | conversation and routing metadata, no post text | kept for statistics |
| Recordings for load tests | source events with post text and author IDs | private, 14 days, see below |

### Deletions

- A deleted post is removed from Redis as soon as the deletion event is processed.
- An account event with a non-active status, or a `sync` event, removes all stored messages of that author from Redis.
- A single record cannot be removed from the `raw.bluesky` topic. A deleted post leaves the topic within about 25 hours.
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
- The 24-hour retention of `raw.bluesky` needs disk space for one day of posts (about 4.5 GB of JSON before compression at the sampled rate). The topic size limit is set to match, and disk space is checked before deployment.
- The project owner is responsible for the personal data in `raw.bluesky`, Redis and the recordings while they exist.
