# 0003. Two source hosts, time cursor in a compacted topic, deduplication in normalize

Status: Accepted
Date: 2026-11-23

## Context

Jetstream has two public hosts, and each one runs as a single node. There were outages in 2026. Ingest must continue after its own restart and after a host failure without losing events.

Facts about the source:

- Each event has a sequence number (`seq`) and a witnessed time. `seq` is only valid on the host that issued it. The witnessed time works on any host but is approximate between hosts.
- A resume is inclusive and delivery is at-least-once, so the same event can arrive again.
- The SDK supports failover hosts only with a time cursor.
- The subscription must include the kinds `commit`, `account` and `sync`. Without `account`, account deletions and takedowns are not delivered (checked on a live connection). A `sync` event means an author's repository was replaced, and the SDK documentation treats it as a removal of earlier records. Its behaviour on the live stream is still to be checked.

Kafka does not remove duplicate events on the broker side. Its idempotent producer only protects against a retry of the same send.

## Decision

### Hosts and cursor

1. `jetstream.us-east.bsky.network` is the primary host and `jetstream.us-west.bsky.network` is the failover host.
2. The resume position is the witnessed time of the last batch that Kafka acknowledged. On a resume the SDK rewinds it by a few seconds (5 by default) to cover clock differences between hosts.
3. The position is stored in the compacted topic `ingest.cursor` (one partition, key = source name, value = host, witnessed time, `seq` and the time of the write). Ingest writes it after every delivered batch and reads the topic to its end at start. An empty topic means "start from live".

### Event ID

Each record in `raw.bluesky` carries a header with an event ID built from the event content, not from `seq`, so it is the same on both hosts:

- commit: `c:<did>:<collection>:<rkey>:<rev>:<operation>`
- account: `a:<did>:<account seq>`
- sync: `s:<did>:<rev>`

### Deduplication

`raw.bluesky` can contain the same event more than once: a few seconds of events after every reader restart or host change, and more after a long outage.

`normalize` removes duplicates in two layers:

1. **Redis keys with a 10-minute lifetime.** For each event, `normalize` sets a key named after the event ID only if it does not exist yet. The value is the partition and offset of the raw record. If the key already exists with a different partition and offset, the event is a duplicate and is skipped. If it exists with the same partition and offset, the same record is being processed again after a failure, and it is processed.
2. **Kafka transactions.** `normalize` reads `raw.bluesky`, writes to `events` and commits its consumer offsets in one transaction. Consumers of `events` read only committed records. A crash in the middle of a batch therefore does not produce a second copy of a normalised event.

The partition and offset in the Redis value matter. Redis is not part of the Kafka transaction, so after an aborted transaction the keys are already set. Without the stored position, the retry would treat its own records as duplicates and drop them.

### Why 10 minutes

| Lifetime of a key | Keys in Redis at about 50 events per second | Covers |
|---|---|---|
| 2 minutes | about 6,000 | a quick restart |
| **10 minutes (chosen)** | about 30,000 | a slow deploy or a short outage |
| 1 hour | about 180,000 | a long outage, at a higher memory cost |

The numbers are checked again after the 24-hour measurement and before load tests at 50 times the live rate.

## Consequences

- A restart or a host change re-delivers a few seconds of events into `raw.bluesky`. They do not reach `events`.
- If the reader was down for longer than 10 minutes and the source replays older events, some duplicates can pass the Redis layer. Services after `normalize` must therefore be idempotent on the event ID. This is a rule for all later services.
- The stored cursor can be slightly behind the log after a crash. This only causes re-delivery, never loss.
- Cursor writes add one small record per batch (up to 64 events or 20 ms by default). Compaction keeps the topic small.
- `normalize` depends on Redis. If Redis is unavailable, `normalize` stops and the raw topic absorbs the delay.
- Transactions add latency to `normalize` (one commit per batch). The cost is measured in stage 2.
