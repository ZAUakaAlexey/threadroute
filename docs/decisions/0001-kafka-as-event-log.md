# 0001. Apache Kafka as the event log between services

Status: Accepted
Date: 2026-11-23

## Context

The system reads the public Bluesky post stream (about 50 post events per second in a short evening sample, to be measured over 24 hours) and turns it into conversations that are routed to operators.

The source does not wait for a slow reader. A client that falls too far behind is disconnected. The protocol defines a `ConsumerTooSlow` error for this. In a live check a reader that stopped for 60 seconds simply had its connection closed. Reading and processing must therefore be separated: the reader must never wait for a database, a cache or business logic.

Load tests will replay recorded traffic at up to 50 times the live rate, so the log must also be the entry point for a replay tool.

The first deployment is one small VPS (4 GB of RAM, shared with other services). If Kafka does not fit, the server is resized. Memory is measured before deployment.

## Options

1. **Apache Kafka.** The standard log for high-throughput systems: ordered partitions by key, consumer groups, retention by time and size, compacted topics, idempotent producers and transactions. It runs on the JVM and needs more memory and more configuration than the other options.
2. **NATS JetStream.** One small binary with deduplication by message ID and a key-value store built in. Much lighter, but less common in the industry and with a smaller set of delivery guarantees to learn from.
3. **RabbitMQ.** Good for task queues. Replay by time and partition ordering need custom work.
4. **Redis Streams.** Redis is already used for conversation state. A 24-hour log in memory does not fit the server.
5. **No log, process inside the reader.** Simplest, but any slow step blocks the reader and the source disconnects it.

## Decision

Use Apache Kafka 4.x in KRaft mode (no ZooKeeper), as a single node at first.

The Go client is `franz-go`. See "Go client" below.

### Topics

| Topic | Content | Key | Cleanup |
|---|---|---|---|
| `raw.bluesky` | source events exactly as the SDK decodes them, as JSON | author ID (DID) | delete, 24 hours and a size limit |
| `events` | channel-agnostic events produced by `normalize` | conversation key | delete, limits set in a later decision |
| `ingest.cursor` | the resume position of the source reader | source name | compact |

Proposed partition counts: 6 for `raw.bluesky`, 16 for `events`, 1 for `ingest.cursor`. A partition count can be raised later but not lowered, and raising it changes which partition a key maps to.

### What ingest does

Ingest holds no business logic and talks to nothing except the source and Kafka. For each event it takes from the SDK it:

- builds the record key (the author ID) and a content-based event ID (see decision 0003), and puts the event ID in a record header,
- serialises the event to JSON. The SDK carries every record twice, as a JSON object and as binary CBOR. The CBOR copy is dropped before serialising, because it holds no extra information and roughly doubles the size of a record,
- produces it to `raw.bluesky` with `acks=all` and idempotence enabled.

It does not filter, enrich or store events anywhere else.

## Go client

| Client | Build | Transactions | Note |
|---|---|---|---|
| **`franz-go` (chosen)** | pure Go | yes | modern API, includes an admin client and an in-memory fake cluster for tests |
| `confluent-kafka-go` | cgo, wraps `librdkafka` | yes | maintained by Confluent, option names match the Kafka documentation |
| `IBM/sarama` | pure Go | yes | the oldest client, a heavier API |
| `segmentio/kafka-go` | pure Go | no | simple API, ruled out because `normalize` needs transactions |

`franz-go` is chosen because it supports everything this project needs without cgo. Binaries are static, images can start from an empty base, cross-compilation works from any machine, and the vulnerability scanner sees all the code.

What is given up: `confluent-kafka-go` uses the same option names as the Kafka documentation, which transfer directly to clients in other languages. To keep that knowledge visible, the project maps each Kafka setting to its `franz-go` option:

| Kafka setting | `franz-go` option |
|---|---|
| `acks=all` | `kgo.RequiredAcks(kgo.AllISRAcks())`, the default |
| `enable.idempotence=true` | on by default, turned off with `kgo.DisableIdempotentWrite()` |
| `compression.type=zstd` | `kgo.ProducerBatchCompression(kgo.ZstdCompression())`, the default is snappy |
| `delivery.timeout.ms` | `kgo.RecordDeliveryTimeout(...)`, unlimited by default |
| `buffer.memory` | `kgo.MaxBufferedRecords(...)` (50,000 by default) and `kgo.MaxBufferedBytes(...)` (unlimited by default) |
| `transactional.id` | `kgo.TransactionalID(...)` |
| `isolation.level=read_committed` | `kgo.FetchIsolationLevel(kgo.ReadCommitted())` |
| `group.id` | `kgo.ConsumerGroup(...)` |

The option names and defaults in this table were checked against `franz-go` v1.22.3.

## Why two topics instead of one

`raw.bluesky` holds events in the source format. A separate service, `normalize`, reads it and writes channel-agnostic events to `events`.

What this gives:

- **Reprocessing.** If the conversation model or the normalisation rules change, the last 24 hours can be processed again from `raw.bluesky` without asking the source.
- **Isolation.** Only ingest and `normalize` know the Bluesky format. Another channel means another raw topic and another normaliser, and nothing else changes.
- **One entry point for tests.** The record tool reads `raw.bluesky` and the replay tool writes to it, so a load test exercises the same path as live traffic, including normalisation.
- **A thin reader.** Ingest does almost no work per event, so it is unlikely to fall behind the source.

What it costs:

- **Storage.** Every event is stored twice for as long as both topics keep it.
- **Latency.** One more hop between the source and a conversation. The added delay is measured in stage 2.
- **One more service** to deploy, monitor and keep idempotent.
- **Ordering across authors is not kept.** `raw.bluesky` is keyed by author, so all events of one author stay in order (a post and its later deletion, for example). A conversation has two authors, and their events can sit in different partitions. `normalize` can therefore emit a company reply before the customer message it answers. Services after `normalize` must order messages inside a conversation by source time, not by arrival.

## Single node

Kafka runs as one node in KRaft combined mode (broker and controller in one process) with replication factor 1.

- If the broker is down, the pipeline stops until it is back.
- If the disk is lost, the topics are lost.

This is accepted for a demo because the source keeps its own replay window (about 36 hours by default, to be confirmed), so the reader can catch up after an outage.

A production setup would use at least three brokers, replication factor 3, `min.insync.replicas=2`, separate controller nodes and rack awareness. The producers already use `acks=all` and idempotence, so the application code would not change.

## When Kafka is unavailable

1. The producer's local buffer is bounded by a record count and a byte size, both set explicitly. When the buffer is full, a produce call blocks, so ingest stops taking batches from the SDK. Nothing is buffered without a limit.
2. The source sees a reader that does not read and disconnects it, or the connection idles.
3. By default `franz-go` retries a record without a time limit. Ingest sets a delivery timeout, so a record that cannot be delivered in time is reported as failed. With the idempotent producer the client only fails a record when that is safe for the sequence numbers, so in some cases a record stays in doubt for longer. In both cases the cursor is written only after a successful delivery report, so it never moves past an event that was not written. A failed record is fatal for the current run, and ingest restarts from the stored cursor.
4. When Kafka is back, ingest resumes from the cursor in `ingest.cursor`. Events delivered twice are removed later (decision 0003).
5. If the outage is longer than the source's replay window, the stored position is older than anything the source still keeps. With a time cursor the SDK then resumes from the oldest event that is still available and reports that the cursor was clamped. The connector logs the gap and counts it in a metric. Events older than the window are lost, and this is the only case where events are lost.

## Limits

- `raw.bluesky` uses `message.timestamp.type=LogAppendTime`: the broker stamps each record with the time it was written. Retention therefore counts from the moment a record entered the topic, not from the event time. Without this, events that arrive late (a catch-up after an outage, or a replay of an old recording) would be deleted at once, and the producer would treat them as timed out. The event time is kept in a record header and inside the payload.
- `raw.bluesky`: `retention.ms` of 24 hours and `retention.bytes` set per partition so that the topic never takes more than about 8 GB in total. Kafka deletes whole segments, so `segment.ms` is one hour and a record can live up to about 25 hours. A partition can also exceed its size limit by one open segment, so `segment.bytes` is set to a small value and the per-partition limit leaves room for it.
- Producers compress with zstd. A 29-minute run on 2026-11-23 measured 1,155 bytes of JSON per event and about 480 bytes per record on disk, index files included. That run held one burst of about 53,000 events in 10 seconds, so a daily size is not projected from it. The 24-hour measurement gives that number, and the 8 GB limit is reviewed then.
- The broker heap is capped (512 MB to start with). Total memory use is measured before deployment, and the server is resized if needed.

## Consequences

- Real experience with partitions, consumer groups, retention, compaction, idempotent producers and transactions.
- More memory and more configuration than JetStream. The broker is the largest process in the project.
- The Kafka client is pure Go, so builds need no C toolchain and binaries are static.
- Kafka does not drop duplicate events on the broker side. `raw.bluesky` is at-least-once and can contain repeated events after every reader restart. Removing them is the job of `normalize` (decision 0003).
- Tests of the bus code run against the in-memory fake cluster from `franz-go`. Integration tests against a real Kafka container are slower and need Docker.
