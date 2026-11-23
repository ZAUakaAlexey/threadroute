# 0006. The normalize service and the channel-agnostic event model

Status: Proposed
Date: 2026-11-23

## Context

`raw.bluesky` holds source events in the Bluesky format (decision 0001). Everything after it should work with events that do not depend on Bluesky, so that another channel can be added with a new connector and a new normaliser.

Decision 0003 already fixes two things for `normalize`: it removes duplicates with Redis keys that live 10 minutes, and it reads `raw.bluesky` and writes its output in one Kafka transaction.

The agreed conversation model: a conversation is a pair of a company (the author of the first post of a thread) and a customer (a person who replied in that thread).

This record decides what `normalize` emits and where.

## Decision

### What normalize does with each source event

| Source event | Result |
|---|---|
| Post that is not a reply | dropped and counted. A post without a reply is not part of any conversation |
| Reply whose author is not the thread author | a customer message. Company = thread author, customer = reply author |
| Reply by the thread author to another person | a company message. Company = thread author, customer = author of the message it answers |
| Reply by the thread author to their own post | dropped and counted. The author continues their own thread |
| Post deleted | a deletion |
| Account not active, or `sync` | an author removal |
| Anything that cannot be decoded | dropped, counted and logged without content |

The company and customer IDs are taken from the reply links of the post, so no request to the Bluesky API is needed.

### Model

One message event has: event ID, channel, source time (the witnessed time), message ID, company ID, customer ID, direction (from customer or from company), parent message ID, text, the language tags as sent, and one normalised language (the primary subtag of the first tag in lower case, or `und` when there is none).

### Topics

| Topic | Content | Key | Partitions |
|---|---|---|---|
| `events` | message events | conversation key: company ID and customer ID | 16 |
| `events.control` | deletions and author removals | author ID | 6 |

A deletion names a message but not its conversation, and an author removal touches many conversations, so neither can be keyed by a conversation. They go to a separate topic. The `conv` service consumes both topics.

## Open choices

### 1. Where a deletion is matched to its conversation

| Option | Note |
|---|---|
| **In `conv`, with its own index (recommended)** | `normalize` stays free of conversation state. `conv` keeps an index from message ID to conversation in Redis. A deletion that arrives before its message is remembered for a few minutes and applied when the message comes |
| In `normalize`, with an index in Redis | deletions travel in `events` with the conversation key and stay in order with the messages. `normalize` then holds state for every message |

### 2. Retention of `events`

| Option | Note |
|---|---|
| **6 hours (recommended)** | long enough to restart or redeploy `conv`. The text of a deleted post stays in this topic for up to that time |
| 24 hours | the same as the raw topic, twice the storage |

## Consequences

- About half of the post stream is dropped in `normalize`, because posts without a reply are not conversations. The share is measured in stage 2.
- Messages of one conversation are in one partition of `events`, but their order there is the order in which `normalize` read them, not source order (decision 0001). Consumers order by source time.
- A deletion and its message travel in different topics, so `conv` must handle a deletion that arrives first.
- `events` holds post text, so decision 0004 gets one more row when this record is accepted.
