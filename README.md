# threadroute

threadroute reads the public Bluesky post stream, turns it into conversations and routes them to simulated support agents. It is a learning project about high-load Go services and real-time interfaces. The operators are simulated: no real person answers anything. This is a personal, non-commercial project.

## Status

Stages 1 to 3 of 6 are implemented:

- Stage 1: ingest of the live stream into Kafka, and the recorder tool for load tests.
- Stage 2: `normalize` and `conv`. Raw source events become channel-agnostic events without duplicates, and conversations with live state in Redis.
- Stage 3: `router` and `sim`. Queues, assignment, dedicated teams and SLA timers in PostgreSQL, and simulated operators that work through the operator API.

The operators are simulated. Every number about waiting times or SLA targets describes the simulator and its settings, not real people.

Planned stages, not done yet:

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

  conversations  ---->  router  ---->  routing  (16 partitions, key = company|customer, or company for team events)
                         |   |
                         |   +-- :9468  /metrics  /healthz  /debug/pprof/
                         |   +-- :9470  operator API (HTTP, no authentication in stage 3)
                         |
                       PostgreSQL  (operators, conversations, offers, teams, SLA timers, outbox)

  routing  ---->  sim  ---->  operator API of the router (online, offline, accept, reply, resolve)
                   +-- :9469  /metrics  /healthz  /debug/pprof/

  raw.bluesky  ---->  record  ---->  compressed segment files outside the repository
                        +-- :9465  /metrics  /healthz  /debug/pprof/
```

`ingest` reads the Bluesky Jetstream through the official SDK, which handles reconnects and failover between two hosts. For every batch it writes one Kafka record per event to `raw.bluesky` (`acks=all`, idempotent, zstd) and, only after Kafka has acknowledged all of them, the batch position to the compacted topic `ingest.cursor`. On start it reads that position and resumes from it, so a restart repeats a few seconds of events and never skips any. Delivery is at-least-once: duplicates are removed later in `normalize`. If Kafka is down for longer than the delivery timeout, ingest exits with an error and the container restart policy starts it again. `normalize` reads `raw.bluesky` and turns each source event into a channel-agnostic event: a customer or company message in `events`, or a deletion or author removal in `events.control`. Posts that are not replies, and authors that continue their own thread, are dropped and counted. It removes duplicates with Redis keys that live 10 minutes, and writes its output and its consumer offsets in one Kafka transaction, so a crash does not produce a second copy. `conv` reads both topics, decides which companies are busy enough to serve (10 different customers within one hour), and keeps the open conversations of those companies in Redis: messages, counters, sessions and close deadlines, all measured in source time. Every change is published to `conversations` through an outbox in Redis, with a sequence number per conversation, and these events carry no post text. `record` is a separate tool that copies the raw topic into files for load tests.

`router` reads `conversations` and keeps the routing state in PostgreSQL. A conversation that opens, reopens, or gets a customer message after the operator resolved it goes into a queue: its language, plus its company when that company has a dedicated team. Assignment workers pick, in one database transaction with `FOR UPDATE SKIP LOCKED`, an online operator with the language, a free slot and the right group, with the lowest load. The operator gets an offer and has 20 seconds to accept; otherwise the conversation goes back to its queue, with its waiting time kept, and that operator is skipped for it. A slot is freed when the operator resolves the conversation, or when `conv` closes it. A company with 20 active conversations gets a dedicated team taken from the shared pool, sized by its load, and loses it after 15 minutes below 8. Two SLA targets are measured per session (60 seconds to accept, 120 seconds to the first reply), and a miss raises the priority. Every state change writes its routing events into an outbox table in the same transaction, and one publisher sends them to `routing` in order, with a sequence number per key. Timers run on the machine clock multiplied by a speed factor (1 for live traffic). At start the router first applies the backlog of `conversations` without making offers, then queues the conversations that are still open (reason `catch_up`). `sim` plays 200 operators against the operator API, the same API the console will use in stage 4. It reads `routing` to learn about offers, accepts after 1 to 8 seconds, leaves 3 percent of offers to time out, replies after a handling time (median 45 seconds) and resolves some minutes later (median 3 minutes), with long tails, and works in shifts (4 hours online, 30 minutes offline). On a clean stop it puts its operators offline, so the router returns their conversations to the queues; at start it puts them offline first and then starts the shifts. The reasons for these choices are in [docs/decisions/](docs/decisions/).

## Run it locally

Prerequisites: Docker with Compose v2, GNU Make, and Go 1.27.4 for the tests (`GOTOOLCHAIN=go1.27.4` is set by the Makefile).

```sh
make up        # builds the images, starts Kafka, Redis, PostgreSQL, ingest, normalize, conv, router and sim
make logs      # follows the logs (JSON lines)
```

Ingest writes to `raw.bluesky` a few seconds after the start. normalize waits for that topic and creates `events` and `events.control`, and conv waits for those and creates `conversations`. The router starts when PostgreSQL is healthy, runs its migrations, waits for `conversations` and creates `routing`. sim waits for the operator API and for `routing`. Each one waits up to 2 minutes and then exits, and the restart policy starts it again, so a cold start needs no manual steps. Every published port listens on `127.0.0.1` only:

| Port | What |
|---|---|
| 9464 | ingest: `/metrics`, `/healthz`, `/debug/pprof/` |
| 9465 | record (only with the `record` profile): the same endpoints |
| 9466 | normalize: the same endpoints |
| 9467 | conv: the same endpoints |
| 9468 | router: the same endpoints. `/healthz` answers 200 after the catch-up |
| 9469 | sim: the same endpoints |
| 9470 | operator API of the router, for example `curl 127.0.0.1:9470/v1/settings`. It has no authentication in stage 3 |
| 6379 | Redis for tools on the host, for example `redis-cli -p 6379 info memory`. Set `THREADROUTE_REDIS_HOST_PORT` if the port is taken |
| 5432 | PostgreSQL for tools on the host, for example `psql -h 127.0.0.1 -U threadroute threadroute`. Set `THREADROUTE_POSTGRES_HOST_PORT` if the port is taken |
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
- `threadroute_normalize_transactions_aborted_total`: aborted transactions by reason. `rebalance`: partitions were revoked or lost. `other`: not committed, with no error and no rebalance; on Kafka 4.x this is mostly a transaction without output records whose offset commit the broker refused with `CONCURRENT_TRANSACTIONS`, because the previous transaction was still ending (see `internal/normalize`). `error`: the service stops. In the first two cases the batch is read again, so nothing is lost or repeated

On `http://127.0.0.1:9467/metrics` (conv):

- `threadroute_conv_messages_total`: message events by direction and result (stored, not admitted, no open conversation, duplicate, edited, edit dropped)
- `threadroute_conv_companies_admitted`, `threadroute_conv_admissions_total`: admitted companies now, and admissions so far
- `threadroute_conv_conversations_open`, `threadroute_conv_conversations_opened_total`, `threadroute_conv_conversations_closed_total`: conversations, closes by reason
- `threadroute_conv_response_time_seconds`: real response time of the company, in source time
- `threadroute_conv_published_total`: conversation events confirmed by Kafka, by type
- `threadroute_conv_outbox_pending`: conversation events not yet published
- `threadroute_conv_watermark_lag_seconds`: now minus the newest source time, per partition of `events`

On `http://127.0.0.1:9468/metrics` (router). The counters count what this process did since its start; the gauges are sampled from PostgreSQL every 5 seconds:

- `threadroute_router_events_applied_total`: conversation events by type and result (queued, pending, ended, applied, duplicate, reset, rejected)
- `threadroute_router_catchup_active`, `threadroute_router_catchup_seconds`, `threadroute_router_catchup_queued`: the catch-up at start
- `threadroute_router_offers_total`, `threadroute_router_offers_expired_total`: offers made, and offers ended by the accept timeout
- `threadroute_router_sla_missed_total`: missed SLA targets, by target
- `threadroute_router_team_changes_total`: teams formed, resized, released, and conversations moved between queues
- `threadroute_router_conversations`, `threadroute_router_queue_waiting`, `threadroute_router_queue_oldest_wait_seconds`: conversations by state, and the queues by language and group
- `threadroute_router_operators`, `threadroute_router_live_slots`, `threadroute_router_active_teams`
- `threadroute_router_routing_published_total`, `threadroute_router_outbox_unsent`, `threadroute_router_outbox_oldest_unsent_seconds`, `threadroute_router_outbox_pruned_total`: the outbox

On `http://127.0.0.1:9469/metrics` (sim):

- `threadroute_sim_operators_online`: operators the router confirmed as online
- `threadroute_sim_offers_seen_total`, `threadroute_sim_accepts_total`, `threadroute_sim_offers_missed_total`, `threadroute_sim_replies_total`, `threadroute_sim_resolves_total`
- `threadroute_sim_api_calls_total`: operator API calls by action and result (done, already_done, rejected, unavailable, failed)
- `threadroute_sim_consumer_lag_seconds`: time from the append of a routing record until sim read it

To start the recorder too (it writes to `$HOME/threadroute-recordings` on the host):

```sh
docker compose -f deploy/compose.yaml --profile record up -d --build
```

On Linux, create the recordings directory first and make it writable for the container user (uid 65532).

Stop the stack with `make down`. To also remove the data volumes of Kafka, Redis and PostgreSQL:

```sh
docker compose -f deploy/compose.yaml --profile record down -v
```

Other targets: `make build`, `make test` (unit tests with the race detector, no Docker), `make test-integration` (real Kafka, Redis and PostgreSQL through Testcontainers, one package at a time, needs Docker), `make lint`, `make vuln`, `make generate` (regenerates the sqlc query code in `internal/pg/pgdb` with the pinned sqlc version, needs a C compiler; CI fails when the committed code differs).

## Configuration

All commands read `THREADROUTE_*` environment variables. An unset or empty variable takes its default. `deploy/compose.yaml` sets the values for the containers and reads overrides from a `.env` file (`make up` passes the root `.env` if it exists). See [.env.example](.env.example). The only secret is the PostgreSQL password. In compose its default is `threadroute-local-only`, which is meant for the local stack on `127.0.0.1` and nothing else; set `THREADROUTE_POSTGRES_PASSWORD` in `.env` to change it. The Redis password is empty by default.

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

PostgreSQL, used by the router:

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_POSTGRES_DSN` | `postgres://threadroute@localhost:5432/threadroute?sslmode=disable` | connection string, as a URL or as key=value pairs. It can hold the password and is never logged |
| `THREADROUTE_POSTGRES_MAX_CONNS` | `8` | most connections in the pool |
| `THREADROUTE_POSTGRES_CONNECT_WAIT` | `1m` | how long a service waits at start until PostgreSQL answers |

`THREADROUTE_POSTGRES_PASSWORD` and `THREADROUTE_POSTGRES_HOST_PORT` are read only by `deploy/compose.yaml`: the password of the database user (compose builds the DSN of the router from it; URL-safe characters only) and the host port of PostgreSQL (default `5432`).

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

Router (`cmd/router`). `THREADROUTE_KAFKA_BROKERS`, `THREADROUTE_LOG_LEVEL` and the PostgreSQL variables work as above. Routing durations are model time: machine time multiplied by the speed factor.

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_ROUTER_GROUP_ID` | `threadroute-router` | consumer group on `conversations`. A new group starts at the oldest record |
| `THREADROUTE_ROUTER_METRICS_ADDR` | `:9468` | listen address of metrics, pprof and health |
| `THREADROUTE_ROUTER_API_ADDR` | `127.0.0.1:9470` | listen address of the operator API. Compose sets `:9470` inside the container and publishes it on `127.0.0.1` only |
| `THREADROUTE_ROUTER_INPUT_TOPIC_WAIT` | `2m` | wait for `conversations` at start |
| `THREADROUTE_ROUTER_SPEED_FACTOR` | `1` | multiplies the machine clock of every routing timer; the replay speed in a load test, up to 10000 |
| `THREADROUTE_ROUTER_OPERATOR_CAPACITY` | `4` | capacity of an operator created without one (1 to 100) |
| `THREADROUTE_ROUTER_ACCEPT_TIMEOUT` | `20s` | how long an operator has to accept an offer |
| `THREADROUTE_ROUTER_ASSIGNMENT_TARGET` | `60s` | SLA target from entering the queue until an accept |
| `THREADROUTE_ROUTER_FIRST_RESPONSE_TARGET` | `120s` | SLA target until the first reply |
| `THREADROUTE_ROUTER_TEAM_FORM_LOAD` | `20` | active load of a company that forms a team |
| `THREADROUTE_ROUTER_TEAM_RELEASE_LOAD` | `8` | a team is released when the load stays below this ... |
| `THREADROUTE_ROUTER_TEAM_RELEASE_AFTER` | `15m` | ... for this long |
| `THREADROUTE_ROUTER_TEAM_MIN_SIZE`, `THREADROUTE_ROUTER_TEAM_MAX_SIZE` | `2`, `20` | bounds of the team size |
| `THREADROUTE_ROUTER_ASSIGN_WORKERS` | `4` | assignment workers (1 to 64) |
| `THREADROUTE_ROUTER_ASSIGN_INTERVAL` | `250ms` | how often an idle worker looks at the queues (machine time) |
| `THREADROUTE_ROUTER_EXPIRE_INTERVAL`, `THREADROUTE_ROUTER_SLA_INTERVAL`, `THREADROUTE_ROUTER_TEAM_INTERVAL` | `1s`, `1s`, `10s` | how often offers, SLA targets and team sizes are checked (model time) |
| `THREADROUTE_ROUTER_PUBLISH_INTERVAL` | `100ms` | how often an idle publisher looks at the outbox |
| `THREADROUTE_ROUTER_PUBLISH_BATCH` | `500` | most outbox rows sent in one round |
| `THREADROUTE_ROUTER_OUTBOX_KEEP`, `THREADROUTE_ROUTER_PRUNE_INTERVAL` | `1h`, `1m` | how long a sent outbox row is kept, and how often old rows are deleted |
| `THREADROUTE_ROUTER_COUNTS_INTERVAL` | `5s` | how often the state gauges are sampled |
| `THREADROUTE_ROUTER_OUTAGE_LIMIT` | `2m` | a loop that fails without a break this long (database or Kafka down) stops the process, and the restart policy starts it again |

Sim (`cmd/sim`). `THREADROUTE_KAFKA_BROKERS` and `THREADROUTE_LOG_LEVEL` work as above. Behaviour durations are model time: sim divides them by the speed factor, which it reads from the router at start.

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_SIM_ROUTER_URL` | `http://127.0.0.1:9470` | base URL of the operator API. Compose sets `http://router:9470` |
| `THREADROUTE_SIM_START_WAIT` | `2m` | wait for the router settings and the routing topic at start |
| `THREADROUTE_SIM_GROUP_ID` | `threadroute-sim` | consumer group on `routing`. sim always starts at the end of the topic |
| `THREADROUTE_SIM_METRICS_ADDR` | `:9469` | listen address of metrics, pprof and health |
| `THREADROUTE_SIM_OUTAGE_LIMIT` | `2m` | when the router has not answered this long, sim stops with an error |
| `THREADROUTE_SIM_OPERATORS` | `200` | number of operators (1 to 9999), IDs `sim-op-0001` and so on |
| `THREADROUTE_SIM_LANGUAGES` | `en:176,ja:6,de:4,fr:4,es:4,pt:3,ko:3` | language spread, scaled to the operator count. Every operator also serves the general queue (`und`) |
| `THREADROUTE_SIM_CAPACITY` | `0` | capacity of each operator; 0 takes the router default |
| `THREADROUTE_SIM_SHIFT_ONLINE`, `THREADROUTE_SIM_SHIFT_OFFLINE` | `4h`, `30m` | shift parts, with a random offset per operator |
| `THREADROUTE_SIM_ACCEPT_DELAY_MIN`, `THREADROUTE_SIM_ACCEPT_DELAY_MAX` | `1s`, `8s` | uniform delay before an accept |
| `THREADROUTE_SIM_MISSED_OFFER_SHARE` | `0.03` | share of offers left to time out on purpose |
| `THREADROUTE_SIM_HANDLING_MEDIAN` | `45s` | median of the log-normal time from accept to reply, each draw capped at 20 times the median |
| `THREADROUTE_SIM_RESOLVE_MEDIAN` | `3m` | median of the time from reply to resolve, capped the same way |
| `THREADROUTE_SIM_SEED` | `1` | seed of the random generator |

## Data and privacy

The source data is public posts written by real people. The raw topic keeps them for about 24 hours, and `events` for about 6 hours. `events.control` and `conversations` hold IDs, times and counters but no post text. Redis holds the text of messages in open conversations until the conversation closes plus 10 minutes, and removes it at once when the author deletes the post or the account. Redis writes every change to an append-only file on disk, so its state survives a restart. The text of a deleted post is removed from memory at once, but it can stay in that file until the next rewrite of the file, which happens when the file has grown by half since the last rewrite (and is at least 32 MB). This is the same kind of limit as the raw topic (24 hours) and `events` (6 hours). The routing data (the `routing` topic, about 24 hours, and the PostgreSQL tables) holds author IDs as company and customer IDs, synthetic operator IDs, languages, times and counters, and no post text. Recordings for load tests are private files outside this repository that are deleted after 14 days. Logs never contain post text, and tests, examples and documentation use only synthetic data. The details, including how deletions are handled, are in [decision 0004](docs/decisions/0004-data-handling-and-privacy.md).

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

The cause of the bursts was found later. A 65-minute run on 2026-11-24 from 03:53 UTC showed that each burst is 96 to 99 percent records of one account: 10,000 to 53,000 "create" operations in about 10 seconds. The source then sends the same records again, with the same event IDs, 8 to 45 minutes later. Five accounts produced 57 percent of the 324,251 raw events of that hour, and the typical rate without them was about 35 events per second. The numbers in the table above are kept as they were measured.

### Stage 2: the whole pipeline

Conditions for every row: 26 minutes on 2026-11-24 from 03:04:31 to 03:30:41 UTC, a cold start of the local compose stack on a laptop (Apple M1 Max, Docker Desktop), one Kafka node, default configuration (admission threshold 10 customers within one hour). Redis ran with an append-only file (`appendfsync always`) in this run, as the compose file does now. During the run ingest was stopped for 20 seconds, and normalize and conv were restarted once each. Operators do not exist yet, so no conversation was answered by an operator. These numbers describe one laptop run at night (UTC), not a full day.

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

What this run does not show: closes, reopened sessions and the end of an admission (the run was shorter than the 30-minute timeout), a full hour of admission, or a day. The same burst arrived a second time about 21 minutes after the first, after two short stalls of the source stream. The cause is now known from the 65-minute run below: each burst is 96 to 99 percent records of one account (10,000 to 53,000 create operations in about 10 seconds), and the source sends the same records again, with the same event IDs, 8 to 45 minutes later. In that hour five accounts produced 57 percent of the 324,251 raw events, and the typical rate without them was about 35 events per second. 205 message events of the repeated burst came back after their 10-minute deduplication key had expired and passed normalize; conv recognised all of them as duplicates.

A second, longer run: 65 minutes on 2026-11-24 from 03:53 UTC, the whole pipeline with default settings and no company limit, a quiet hour of the day (decision 0008). Latency and memory of this run are not reported, because tests were running on the same laptop at the same time.

| Measurement | Value | Note |
|---|---|---|
| Raw events | 324,251 | five accounts produced 57 percent of them, see the bursts above |
| Companies admitted | 269 | none of the admissions ended |
| Conversations opened | 5,245 | 2,290 closed, 26 reopened in a new session |
| Concentration | 10 companies had 29 percent of the conversations, 50 companies had 67 percent | half of the companies had 8 conversations or fewer |

### Stage 3: routing with simulated operators

Conditions for every row: 45 minutes on 2026-11-24 from 06:56:47 to 07:41:59 UTC, a cold start of the local compose stack on a laptop (Apple M1 Max, Docker Desktop), one Kafka node, live traffic of a morning hour (UTC). Default settings of every service, except one: the router kept sent outbox rows for 20 minutes instead of 1 hour, so pruning ran inside the run. Redis wrote an append-only file with `appendfsync always`, as the compose file does now. sim was restarted at minute 15 and the router at minute 25; nothing else was disturbed in the window.

The operators are simulated: 200 of them, with the language spread, shifts and timing of the simulator defaults. The waiting times and SLA misses below describe those settings in one laptop run. They are not load-test results, and they say nothing about real support agents.

| Measurement | Value | Note |
|---|---|---|
| Raw events read by normalize | 230,412 | emitted 32,103 + dropped 198,309 + duplicates 0, difference 0. Bursts of about 53,000 events at minutes 12 and 23 (the second one 11 minutes after the first, like the repeats described above) and about 30,000 at minutes 40 to 41 |
| Transactions of normalize | 40,122 committed, 465 aborted (1.1%) | all 465 with reason `other` and no output record, 0 by a rebalance, 0 by an error |
| Companies admitted | 124 | in the first 45 minutes of a one-hour window |
| Conversations opened | 1,196 sessions | 1,155 new and 41 reopened. 188 closed (139 inactivity, 49 author removed) |
| Languages of the opened sessions | en 78%, de 10%, es 6%, nl 1.6%, ko 1.1%, 11 others | the operator spread was made for one hour at night with 98% English |
| Routing events in the window | 6,615 | per key from 1, no gap, no repeat, in order; 0 offers for a conversation that had ended |
| Offers | 1,270 made, 1,213 accepted | 33 expired by the accept timeout, 23 ended because the operator went offline, 0 refused |
| Replies and resolves | 1,142 reply markers (1,021 first replies), 897 resolves | |
| Waiting time from queue to accept | median 5.3 s, 95th percentile 206 s, highest 1,151 s | simulated operators. German and Spanish have 4 operators each (16 slots) for 118 and 75 sessions; at the end of the window 27 German and 20 Spanish conversations were waiting |
| Time to first response | median 51 s, 95th percentile 255 s, highest 906 s | simulated operators |
| SLA misses | 160 of the assignment target (60 s), 278 of the first response target (120 s) | |
| Sessions without an operator | 52 closed while waiting (3 in a language with no operator), 85 still waiting at the end of the window (42 in a language with no operator, mostly nl, tr, nb) | a known language without operators waits until the conversation closes; only `und` has a general queue |
| Dedicated teams | none formed | no company reached 20 active conversations |
| Restart of sim | 77 live offers and assignments before the stop, 0 right after; 0 older than the restart 5, 10 and 20 minutes later | sim put its operators offline on stop, the router returned their conversations to the queues, and work went on (670 accepts in the next 20 minutes) |
| Restart of the router | stopped in 0.3 s, ready 0.2 s after the start, catch-up 12 ms with 0 conversations queued as `catch_up` | the backlog of such a short stop is empty, so this run does not measure a long catch-up |
| Database invariants | 0 violations in 84 checks | at most one live offer per conversation, slots within capacity, no conversation both waiting and held, no offline operator with a slot |
| Outbox | unsent rows: median 0, highest 2; oldest unsent row at most 0.09 s | 2,298 sent rows pruned |
| Memory, median and highest | router 44 / 63 MiB, sim 11 / 12 MiB, PostgreSQL 72 / 77 MiB, Kafka 630 / 778 MiB, Redis 16 / 23 MiB, ingest 52 / 80 MiB, normalize 34 / 40 MiB, conv 28 / 36 MiB | container memory from `docker stats`, every 30 s |
| CPU, median | Kafka 11%, PostgreSQL 3.3%, ingest 3.0%, normalize 2.1%, router 1.4%, conv 1.0%, sim 0.2% | of one core |
| PostgreSQL at the end | 13.3 MiB database; outbox 3.3 MiB, conversations 0.7 MiB, offers 0.5 MiB | |
| Redis append-only file | 48 KiB at the start, 24.5 MiB at the end (about 33 MiB per hour); 0 rewrites | the first rewrite starts at 32 MB, so in this run deleted text stayed in the file for the whole 45 minutes |
| Topic size on disk at the end | `raw.bluesky` 104 MiB, `events` 13.2 MiB, `conversations` 1.0 MiB, `routing` 2.0 MiB | |

After the window, three more tests ran:

- Redis restart: Redis answered again after 1.5 s with all its keys (62,085 before, 62,125 after). conv logged two failed sweeps and went on without a restart: 1,008 open conversations before, 1,036 one minute later. Sequence numbers in `conversations` did not start again: no gap and no repeat over the restart.
- Kafka paused for 185 seconds, longer than the 2-minute outage limit: no service exited or restarted. The router kept its API and made offers in PostgreSQL, but could not publish them, so 386 routing events waited in the outbox and sim could not see the offers; they ran into the accept timeout. The router logged one failed loop step during the pause and one recovery after it, and did not exit. Why the 2-minute outage limit did not stop it is not determined yet. The outbox was empty again within 15 seconds, one routing event was sent twice (an exact copy), and in the next 5 minutes 421 offers were made and 408 accepted. A stopped broker (connections refused) was not tested.
- `docker compose down -v` left no container, volume or network of the project.

What this run does not show: a long catch-up of the router, dedicated teams, a rewrite of the Redis file, a full day, or a busy hour.

These tables are filled only with real measurements, together with the conditions they were taken under.

## License

MIT, see [LICENSE](LICENSE).
