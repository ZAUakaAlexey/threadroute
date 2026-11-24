# threadroute

threadroute reads the public Bluesky post stream, turns it into conversations and routes them to simulated support agents. It is a learning project about high-load Go services and real-time interfaces. The operators are simulated: no real person answers anything. This is a personal, non-commercial project.

## Status

Stages 1 and 2 of 6 are implemented:

- Stage 1: ingest of the live stream into Kafka, and the recorder tool for load tests.
- Stage 2: `normalize` and `conv`. Raw source events become channel-agnostic events without duplicates, and conversations with live state in Redis.

Operators do not exist in the code yet. Conversations are built and published, but nobody is assigned to them. Routing and simulated operators come in stage 3.

Planned stages, not done yet:

- Stage 3: `router` and `sim`. Queues, assignment and SLA timers in PostgreSQL, and simulated operators.
- Stage 4: `gateway` and the React console. Real-time updates over WebSocket, and the public page.
- Stage 5: load tests. The replay tool plays recorded traffic at many times the live rate.
- Stage 6: classification of conversations with an LLM, used for routing.

## Architecture

```
                 wss (zstd)                       Kafka (KRaft, 1 node)
  Jetstream   ---------------->   ingest   ---->  raw.bluesky     (6 partitions, key = author DID)
  us-east (primary)               |        ---->  ingest.cursor   (1 partition, compacted)
  us-west (failover)              |
                                  +-- :9464  /metrics  /healthz  /debug/pprof/

  raw.bluesky  ---->  normalize  ---->  events          (16 partitions, key = company|customer)
                       |   |       ---->  events.control  (6 partitions, key = author)
                       |   +-- :9466  /metrics  /healthz  /debug/pprof/
                       |
                     Redis  (dedup keys of normalize, conversation state of conv)
                       |
  events          ---->  conv  ---->  conversations  (16 partitions, key = company|customer)
  events.control  ---->    +-- :9467  /metrics  /healthz  /debug/pprof/

  raw.bluesky  ---->  record  ---->  compressed segment files outside the repository
                        +-- :9465  /metrics  /healthz  /debug/pprof/
```

`ingest` reads the Bluesky Jetstream through the official SDK, which handles reconnects and failover between two hosts. For every batch it writes one Kafka record per event to `raw.bluesky` (`acks=all`, idempotent, zstd) and, only after Kafka has acknowledged all of them, the batch position to the compacted topic `ingest.cursor`. On start it reads that position and resumes from it, so a restart repeats a few seconds of events and never skips any. Delivery is at-least-once: duplicates are removed later in `normalize`. If Kafka is down for longer than the delivery timeout, ingest exits with an error and the container restart policy starts it again. `normalize` reads `raw.bluesky` and turns each source event into a channel-agnostic event: a customer or company message in `events`, or a deletion or author removal in `events.control`. Posts that are not replies, and authors that continue their own thread, are dropped and counted. It removes duplicates with Redis keys that live 10 minutes, and writes its output and its consumer offsets in one Kafka transaction, so a crash does not produce a second copy. `conv` reads both topics, decides which companies are busy enough to serve (10 different customers within one hour), and keeps the open conversations of those companies in Redis: messages, counters, sessions and close deadlines, all measured in source time. Every change is published to `conversations` through an outbox in Redis, with a sequence number per conversation, and these events carry no post text. `record` is a separate tool that copies the raw topic into files for load tests. The reasons for these choices are in [docs/decisions/](docs/decisions/).

## Run it locally

Prerequisites: Docker with Compose v2, GNU Make, and Go 1.27.4 for the tests (`GOTOOLCHAIN=go1.27.4` is set by the Makefile).

```sh
make up        # builds the images, starts Kafka, Redis, ingest, normalize and conv
make logs      # follows the logs (JSON lines)
```

Ingest writes to `raw.bluesky` a few seconds after the start. normalize waits for that topic and creates `events` and `events.control`, and conv waits for those and creates `conversations`. Each one waits up to 2 minutes and then exits, and the restart policy starts it again, so a cold start needs no manual steps. Every published port listens on `127.0.0.1` only:

| Port | What |
|---|---|
| 9464 | ingest: `/metrics`, `/healthz`, `/debug/pprof/` |
| 9465 | record (only with the `record` profile): the same endpoints |
| 9466 | normalize: the same endpoints |
| 9467 | conv: the same endpoints |
| 6379 | Redis for tools on the host, for example `redis-cli -p 6379 info memory`. Set `THREADROUTE_REDIS_HOST_PORT` if the port is taken |
| 9094 | Kafka for tools on the host, for example `kcat -b localhost:9094 -L` |

Useful metrics on `http://127.0.0.1:9464/metrics`:

- `threadroute_source_events_total`: events received, by kind
- `threadroute_source_lag_seconds`: now minus the witnessed time of the last batch
- `threadroute_bus_produced_records_total`: records acknowledged by Kafka, by topic
- `process_resident_memory_bytes`: memory of the ingest process

On `http://127.0.0.1:9466/metrics` (normalize). The counters change only when a transaction commits, so `raw_records_total` always equals the sum of `emitted_total`, `dropped_total` and `duplicates_total`:

- `threadroute_normalize_raw_records_total`: raw records read
- `threadroute_normalize_emitted_total`: events written, by kind (customer and company message, deletion, author removal)
- `threadroute_normalize_dropped_total`: records with no output, by reason
- `threadroute_normalize_duplicates_total`: duplicates removed
- `threadroute_normalize_lag_seconds`: now minus the source time of the last committed record
- `threadroute_normalize_transactions_total`: transactions by result (committed, aborted)

On `http://127.0.0.1:9467/metrics` (conv):

- `threadroute_conv_messages_total`: message events by direction and result (stored, not admitted, no open conversation, duplicate, edited, edit dropped)
- `threadroute_conv_companies_admitted`, `threadroute_conv_admissions_total`: admitted companies now, and admissions so far
- `threadroute_conv_conversations_open`, `threadroute_conv_conversations_opened_total`, `threadroute_conv_conversations_closed_total`: conversations, closes by reason
- `threadroute_conv_response_time_seconds`: real response time of the company, in source time
- `threadroute_conv_published_total`: conversation events confirmed by Kafka, by type
- `threadroute_conv_outbox_pending`: conversation events not yet published
- `threadroute_conv_watermark_lag_seconds`: now minus the newest source time, per partition of `events`

To start the recorder too (it writes to `$HOME/threadroute-recordings` on the host):

```sh
docker compose -f deploy/compose.yaml --profile record up -d --build
```

On Linux, create the recordings directory first and make it writable for the container user (uid 65532).

Stop the stack with `make down`. To also remove the Kafka data volume:

```sh
docker compose -f deploy/compose.yaml --profile record down -v
```

Other targets: `make build`, `make test` (unit tests with the race detector, no Docker), `make test-integration` (real Kafka and Redis through Testcontainers, one package at a time, needs Docker), `make lint`, `make vuln`.

## Configuration

All commands read `THREADROUTE_*` environment variables. An unset or empty variable takes its default. `deploy/compose.yaml` sets the values for the containers and reads overrides from a `.env` file (`make up` passes the root `.env` if it exists). See [.env.example](.env.example). No secrets exist yet: the Redis password is empty by default.

Ingest (`cmd/ingest`):

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_KAFKA_BROKERS` | `localhost:9092` | Kafka seed brokers, comma-separated `host:port` |
| `THREADROUTE_SOURCE_PRIMARY_HOST` | `jetstream.us-east.bsky.network` | Jetstream host read first |
| `THREADROUTE_SOURCE_FAILOVER_HOST` | `jetstream.us-west.bsky.network` | Jetstream host used when the primary fails |
| `THREADROUTE_SOURCE_COLLECTION` | `app.bsky.feed.post` | the only collection ingest subscribes to |
| `THREADROUTE_METRICS_ADDR` | `:9464` | listen address of metrics, pprof and health |
| `THREADROUTE_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `THREADROUTE_PRODUCER_MAX_BUFFERED_RECORDS` | `10000` | records in the producer buffer before a write blocks |
| `THREADROUTE_PRODUCER_MAX_BUFFERED_BYTES` | `33554432` (32 MiB) | bytes in the producer buffer before a write blocks |
| `THREADROUTE_PRODUCER_DELIVERY_TIMEOUT` | `2m` | how long a record may wait for Kafka before ingest fails |
| `THREADROUTE_RAW_RETENTION` | `24h` | retention of `raw.bluesky`, at least `1h` |
| `THREADROUTE_RAW_MAX_BYTES` | `8000000000` | size limit of the whole `raw.bluesky` topic |

Record (`cmd/record`). `THREADROUTE_KAFKA_BROKERS` and `THREADROUTE_LOG_LEVEL` work as above.

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_RECORD_GROUP_ID` | `threadroute-record` | consumer group |
| `THREADROUTE_RECORD_START` | `end` | where a new group starts: `end` or `beginning` |
| `THREADROUTE_RECORD_DIR` | `$HOME/threadroute-recordings` | output directory, absolute, never inside a git working tree |
| `THREADROUTE_RECORD_SEGMENT_MAX_BYTES` | `268435456` (256 MiB) | uncompressed size at which a segment file is closed |
| `THREADROUTE_RECORD_SEGMENT_MAX_AGE` | `1h` | age at which a segment file is closed |
| `THREADROUTE_RECORD_RETENTION` | `336h` (14 days) | segment files older than this are deleted, at most 14 days |
| `THREADROUTE_RECORD_MAX_TOTAL_BYTES` | no limit | size limit of all segment files |
| `THREADROUTE_RECORD_METRICS_ADDR` | `:9465` | listen address of metrics, pprof and health |

`THREADROUTE_RECORD_HOST_DIR` is read only by `deploy/compose.yaml`: the host directory mounted as `/recordings` (default `$HOME/threadroute-recordings`).

Normalize (`cmd/normalize`) and conv (`cmd/conv`) share these. `THREADROUTE_KAFKA_BROKERS` and `THREADROUTE_LOG_LEVEL` work as above.

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_REDIS_ADDR` | `localhost:6379` | Redis address, Redis 7.0 or newer |
| `THREADROUTE_REDIS_PASSWORD` | empty | Redis password, never logged |
| `THREADROUTE_REDIS_DB` | `0` | Redis database number |

`THREADROUTE_REDIS_HOST_PORT` is read only by `deploy/compose.yaml`: the host port of Redis (default `6379`).

Normalize:

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_NORMALIZE_GROUP_ID` | `threadroute-normalize` | consumer group on `raw.bluesky` |
| `THREADROUTE_NORMALIZE_TRANSACTIONAL_ID` | `threadroute-normalize` | Kafka transactional ID, unique per replica |
| `THREADROUTE_NORMALIZE_START` | `beginning` | where a new group starts: `beginning` or `end` |
| `THREADROUTE_NORMALIZE_MAX_BATCH_RECORDS` | `1000` | most raw records in one transaction |
| `THREADROUTE_NORMALIZE_DEDUP_KEY_LIFETIME` | `10m` | lifetime of a deduplication key |
| `THREADROUTE_NORMALIZE_RAW_TOPIC_WAIT` | `2m` | wait for `raw.bluesky` at start |
| `THREADROUTE_NORMALIZE_METRICS_ADDR` | `:9466` | listen address of metrics, pprof and health |
| `THREADROUTE_NORMALIZE_PRODUCER_MAX_BUFFERED_RECORDS` | `10000` | records in the producer buffer before a write blocks |
| `THREADROUTE_NORMALIZE_PRODUCER_MAX_BUFFERED_BYTES` | `33554432` (32 MiB) | bytes in the producer buffer before a write blocks |
| `THREADROUTE_NORMALIZE_PRODUCER_DELIVERY_TIMEOUT` | `2m` | how long a record may wait for Kafka |

Conv. Windows are in source time unless noted.

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_CONV_GROUP_ID` | `threadroute-conv` | consumer group on `events` and `events.control` |
| `THREADROUTE_CONV_METRICS_ADDR` | `:9467` | listen address of metrics, pprof and health |
| `THREADROUTE_CONV_SWEEP_INTERVAL` | `1s` | how often the sweeper runs (wall clock) |
| `THREADROUTE_CONV_INPUT_TOPIC_WAIT` | `2m` | wait for the input topics at start |
| `THREADROUTE_CONV_ADMISSION_THRESHOLD` | `10` | different customers within the window that admit a company |
| `THREADROUTE_CONV_ADMISSION_WINDOW` | `1h` | sliding window of the admission count |
| `THREADROUTE_CONV_ADMISSION_IDLE` | `2h` | admission ends after this long without a customer message |
| `THREADROUTE_CONV_COMPANY_LIMIT` | `0` (no limit) | most admitted companies |
| `THREADROUTE_CONV_INACTIVITY_TIMEOUT` | `30m` | a conversation closes after this long without a message |
| `THREADROUTE_CONV_TEXT_KEPT_AFTER_CLOSE` | `10m` | text stays in Redis this long after a close |
| `THREADROUTE_CONV_MESSAGES_KEPT` | `50` | newest messages per conversation that keep their text |
| `THREADROUTE_CONV_SESSION_RECORD_KEPT` | `24h` | the session record outlives a close this long |
| `THREADROUTE_CONV_EARLY_DELETION_KEPT` | `10m` | a deletion that came before its message is remembered this long |
| `THREADROUTE_CONV_APPLIED_LIFETIME` | `7h` | wall-clock lifetime of an applied event ID marker |
| `THREADROUTE_CONV_KEY_MARGIN` | `7h` | added to windows to get wall-clock key lifetimes |

## Data and privacy

The source data is public posts written by real people. The raw topic keeps them for about 24 hours, and `events` for about 6 hours. `events.control` and `conversations` hold IDs, times and counters but no post text. Redis holds the text of messages in open conversations until the conversation closes plus 10 minutes, and removes it at once when the author deletes the post or the account. Redis runs in memory only, so no post text is written to its disk. The cost is that a Redis restart loses the conversation state. Recordings for load tests are private files outside this repository that are deleted after 14 days. Logs never contain post text, and tests, examples and documentation use only synthetic data. The details, including how deletions are handled, are in [decision 0004](docs/decisions/0004-data-handling-and-privacy.md).

## Decisions

Architecture decisions are recorded in [docs/decisions/](docs/decisions/). A decision is written and agreed before the code that depends on it.

## Results

### Stage 1: ingest

Conditions for every row: 29 minutes on 2026-11-23 from 21:02 UTC, the local compose stack on a laptop (Apple M1 Max, Docker Desktop), one Kafka node, events of kinds commit, account and sync for the post collection.

| Measurement | Value | Note |
|---|---|---|
| Live events per second, typical | 48 (median of 10-second windows) | 49 on average outside the burst below |
| Live events per second, average | 79 | 138,039 events in 1,745 seconds, including one burst |
| Live events per second, peak | about 5,300 for 10 seconds | one burst of about 53,000 events, with no source error |
| Lag behind the source | 0.16 s median, 0.26 s at the 95th percentile, 0.52 s highest sample | sampled every 10 seconds, so the lag inside the burst was not seen |
| Raw topic size on disk | 67 MiB for 144,452 records, about 480 bytes per record | zstd, includes index files. JSON payloads were 1,155 bytes per event before compression |
| Ingest memory | 45 to 90 MB resident, 62 MB median | the highest value was during the burst |
| Kafka memory | 416 to 558 MiB for the container | broker heap capped at 512 MB |
| Errors and restarts | 0 produce errors, 0 source errors, 0 restarts | |

What this run does not show: a full day, a quiet hour, or the busiest hour. Bursts like the one above were seen twice in about one hour of watching, so a daily size is not projected from this sample. A 24-hour measurement comes next.

### Stage 2: the whole pipeline

Conditions for every row: 26 minutes on 2026-11-24 from 03:04:31 to 03:30:41 UTC, a cold start of the local compose stack on a laptop (Apple M1 Max, Docker Desktop), one Kafka node, default configuration (admission threshold 10 customers within one hour). Redis ran with an append-only file (`appendfsync always`) in this run; the compose file now runs Redis in memory only. During the run ingest was stopped for 20 seconds, and normalize and conv were restarted once each. Operators do not exist yet, so no conversation was answered by an operator. These numbers describe one laptop run at night (UTC), not a full day.

| Measurement | Value | Note |
|---|---|---|
| Raw events read by normalize | 266,638 | equal to the records in `raw.bluesky`. Emitted 29,220 + dropped 237,339 + duplicates 79, difference 0 |
| Live events per second, typical | 40 (median of 10-second windows) | four bursts: about 53,000 events in 13 s at 03:05, 23,000 in 12 s at 03:23, 31,000 in 13 s at 03:25 and 53,000 in 13 s at 03:26 UTC |
| Raw events that became message events | 8.1% (21,682) | the bursts were mostly posts without a reply. 87% of raw events were posts without a reply |
| Customer and company messages | 82% and 18% (17,794 and 3,888) | |
| Duplicates removed by normalize after the ingest stop | 78 | none of them reached `events` |
| Companies admitted | 123 | in the first 26 minutes of a one-hour window |
| Conversations opened | 2,334 | none closed: the inactivity timeout is 30 minutes |
| Customer messages stored | 2,863 of 17,794 (16%) | the rest belonged to companies that were not admitted |
| Answered by the company | 32 | real response time about 105 s median and 750 s at the 95th percentile, from histogram buckets |
| Conversation events | 2,927 | sequence numbers per conversation start at 1 with no gap and no repeat |
| Lag of normalize | 0.13 s median, 0.27 s at the 95th percentile, 2.7 s highest sample | sampled about every 12 seconds |
| Memory, median and highest | ingest 49 / 80 MiB, normalize 35 / 43 MiB, conv 32 / 40 MiB, Kafka 574 / 672 MiB, Redis 17 / 28 MiB | container memory from `docker stats` |
| Redis at the end | 71,832 keys, 21 MiB used | grew by about 35 MiB per hour; most keys live 7 to 8 hours |
| Topic size on disk at the end | `raw.bluesky` 113 MiB, `events` 10.4 MiB, `events.control` 1.1 MiB, `conversations` 1.3 MiB | |
| Restart of normalize and conv | normalize: first commit 1.5 s after the start, lag 0.14 s. conv: ready after 0.4 s, highest partition watermark lag back to its usual 3 to 7 s within 2 s | ingest was paused for 5 s before each restart and the service was down for under 1 s, so the catch-up was small. No crash, no restart by the restart policy |

What this run does not show: closes, reopened sessions and the end of an admission (the run was shorter than the 30-minute timeout), a full hour of admission, or a day. The same burst arrived a second time about 21 minutes after the first, after two short stalls of the source stream. Why it came again is not known: a replay by the source, a host change of the reader, or a side effect of the short pauses of ingest during the run are all possible. 205 message events of it came back after their 10-minute deduplication key had expired and passed normalize; conv recognised all of them as duplicates.

These tables are filled only with real measurements, together with the conditions they were taken under.

## License

MIT, see [LICENSE](LICENSE).
