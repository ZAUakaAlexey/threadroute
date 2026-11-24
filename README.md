# threadroute

threadroute reads the public Bluesky post stream, turns it into conversations and routes them to simulated support agents. It is a learning project about high-load Go services and real-time interfaces. The operators are simulated: no real person answers anything. This is a personal, non-commercial project.

## Status

Stages 1 to 4 of 6 are implemented:

- Stage 1: ingest of the live stream into Kafka, and the recorder tool for load tests.
- Stage 2: `normalize` and `conv`. Raw source events become channel-agnostic events without duplicates, and conversations with live state in Redis.
- Stage 3: `router` and `sim`. Queues, assignment, dedicated teams and SLA timers in PostgreSQL, and simulated operators that work through the operator API.
- Stage 4: `gateway` and the console in `web/`. Live updates over WebSocket for an operator screen and a supervisor screen, and a public page with totals only. Team rules and simulator numbers can be changed from the console while the stand runs, with a trend strip of the last hour (decision 0014).

The operators are simulated. Every number about waiting times or SLA targets describes the simulator and its settings, not real people. A person who opens the console can take a seat as one more operator next to the simulated ones.

Planned stages, not done yet:

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
                       PostgreSQL  (operators, conversations, offers, teams, SLA timers, outbox, stand settings)

  routing  ---->  sim  ---->  operator API of the router (online, offline, accept, reply, resolve, settings)
                   +-- :9469  /metrics  /healthz  /debug/pprof/

  conversations  ---->  gateway  <---->  browsers (WebSocket /v1/ws, HTTP /v1/public/snapshot)
  routing        ---->    |   |            through the console web server on :9473
                          |   +-- :9471  /metrics  /healthz  /debug/pprof/
                          |   +-- :9472  /v1/ws  /v1/public/snapshot
                          +-- text of watched conversations from Redis (read only)
                          +-- operator actions of people -> operator API of the router
                          +-- stand settings and changes -> operator API of the router
                          +-- "who am I" of a cookie or token -> site API (optional)

  raw.bluesky  ---->  record  ---->  compressed segment files outside the repository
                        +-- :9465  /metrics  /healthz  /debug/pprof/
```

`ingest` reads the Bluesky Jetstream through the official SDK, which handles reconnects and failover between two hosts. For every batch it writes one Kafka record per event to `raw.bluesky` (`acks=all`, idempotent, zstd) and, only after Kafka has acknowledged all of them, the batch position to the compacted topic `ingest.cursor`. On start it reads that position and resumes from it, so a restart repeats a few seconds of events and never skips any. Delivery is at-least-once: duplicates are removed later in `normalize`. If Kafka is down for longer than the delivery timeout, ingest exits with an error and the container restart policy starts it again. `normalize` reads `raw.bluesky` and turns each source event into a channel-agnostic event: a customer or company message in `events`, or a deletion or author removal in `events.control`. Posts that are not replies, and authors that continue their own thread, are dropped and counted. It removes duplicates with Redis keys that live 10 minutes, and writes its output and its consumer offsets in one Kafka transaction, so a crash does not produce a second copy. `conv` reads both topics, decides which companies are busy enough to serve (10 different customers within one hour), and keeps the open conversations of those companies in Redis: messages, counters, sessions and close deadlines, all measured in source time. Every change is published to `conversations` through an outbox in Redis, with a sequence number per conversation, and these events carry no post text. `record` is a separate tool that copies the raw topic into files for load tests.

`router` reads `conversations` and keeps the routing state in PostgreSQL. A conversation that opens, reopens, or gets a customer message after the operator resolved it goes into a queue: its language, plus its company when that company has a dedicated team. Assignment workers pick, in one database transaction with `FOR UPDATE SKIP LOCKED`, an online operator with the language, a free slot and the right group. Among those the order is: the previous operator of the conversation (after a reopen, or a customer message after a resolve), then a member of the team (for a team queue), then the lowest load, then the highest operator priority, then the operator ID. The priority is a whole number from 0 to 9 that the caller of the operator API sets; the gateway gives people 1 and `sim` sends none, so a person with no conversation gets the next one in their languages before an idle simulated operator. A conversation whose language is unknown (`und`) goes to the general queue, which the operators with `und` serve. The same operators also take a conversation of the shared queue whose language no online operator of the shared pool has. Such a conversation keeps its language and its place in the queue, and the router checks the rule again at every pick, so the first operator with the language who comes online gets the next one. While at least one online operator of the shared pool has the language, the conversation waits for them, also when they are all busy. A team member never serves through the general queue. The operator gets an offer and has 20 seconds to accept; otherwise the conversation goes back to its queue, with its waiting time kept, and that operator is skipped for it. A slot is freed when the operator resolves the conversation, or when `conv` closes it. A company with 20 active conversations gets a dedicated team taken from the shared pool, sized by its load, and loses it after 15 minutes below 8. Team members serve only their company. A conversation in a team queue goes to a member with its language and a free slot first; when no member can take it (all are full, or the free ones refused it or let it time out), an operator of the shared pool takes it by the rules of the shared queue, the general queue rule included (the overflow, decision 0009). The conversation stays in its team queue with its priority and its waiting time, and the router checks the rule again at every pick. When every member and every pool operator who could take a conversation has been skipped for it, the skips are cleared once and it is offered again, members first. All teams together hold at most half of the online operators (the team cap, decision 0009). The router checks the cap only when an operator would join a team: a team that should grow gets only what the cap leaves, and a new team forms only when the cap leaves room for the minimum team size; otherwise the company stays in the shared queues and is tried again at the next team check. When the room is not enough for every company, the company with the highest active load comes first. The cap never takes an operator out of a team: when pool operators go offline, the teams stop growing and get smaller only by the normal size check and the release rule. Two SLA targets are measured per session (60 seconds to accept, 120 seconds to the first reply), and a miss raises the priority. Every state change writes its routing events into an outbox table in the same transaction, and one publisher sends them to `routing` in order, with a sequence number per key. Timers run on the machine clock multiplied by a speed factor (1 for live traffic). At start the router first applies the backlog of `conversations` without making offers, then queues the conversations that are still open (reason `catch_up`). `sim` plays 200 operators against the operator API, the same API the gateway uses for people in the console. It reads `routing` to learn about offers, accepts after 1 to 8 seconds, leaves 3 percent of offers to time out, replies after a handling time (median 45 seconds) and resolves some minutes later (median 3 minutes), with long tails, and works in shifts (4 hours online, 30 minutes offline). On a clean stop it puts its operators offline, so the router returns their conversations to the queues; at start it puts them offline first and then starts the shifts. The team numbers above and the five main numbers of `sim` are starting values: they can be changed from the console while the stand runs (see "Stand settings from the console").

`gateway` connects browsers to all of this (decision 0011). At start it reads `conversations` and `routing` from the beginning, without a consumer group, and keeps the latest state of every conversation, operator, queue and team in memory. `/healthz` answers 200 when it has read up to the end offsets it saw at start and its data is fresh, and 503 while it cannot read the topics or is behind them. The topics are the only source of this view, so the view cannot disagree with them; to keep operators in a view that is built from topics with 24 hours of retention, the router publishes the snapshot of every operator again every 6 hours. Message text is not in the topics: the gateway reads it from Redis through a read-only function of `conv`, only for a conversation that a full-level screen watches, and does not keep it after it has sent it. When an author deletes a post, `conv` publishes a "message removed" event and the gateway tells the screens that show it to drop the text. A browser gets the current state when it connects and then, at most every 250 milliseconds, the new state of the rows that changed. A slow browser gets fewer and larger updates instead of a queue. A person who takes the operator role becomes an operator of their own with an ID that starts with `web-`, made by the gateway (decision 0012). Their actions go through the gateway to the operator API of the router, like the actions of `sim`. When the connection closes, the gateway waits 15 seconds (so a reload keeps the conversations) and then puts the operator offline, and the router returns their conversations to the queues. The gateway also passes changes of the stand settings to the router, tells every screen about a new set, and keeps one hour of trend points in memory for the supervisor screen (decision 0014, see "Stand settings from the console"). The public page does not use a WebSocket: it asks for a small JSON snapshot every two seconds, which the gateway builds once a second and the web server caches for one second. The protocol between the gateway and the browser is in [docs/gateway-protocol.md](docs/gateway-protocol.md), and the console itself is described in [web/README.md](web/README.md). The reasons for these choices are in [docs/decisions/](docs/decisions/).

Access levels (decision 0012). The gateway decides the level from the site sign-in (a cookie or a token) that it checks with the site API, or from the local role of a developer stand (decision 0014); the browser never asks for a level.

| Level | Who | Sees | Can do |
|---|---|---|---|
| Public | anyone | totals only, no keys (decision 0013) | nothing |
| Demo operator | anyone, limited number (5 operators at the same time) | their offers and conversations without text and author IDs, the supervisor screen without them. Accounts and messages appear under pseudonyms that change every day | accept, refuse, mark a reply, resolve |
| Full | users on the owner's list (a role of the site, `THREADROUTE_GATEWAY_FULL_ROLES`) | the same with text and author IDs | the same |

The right to change the stand settings is separate from the level: it comes from a second list of site roles, `THREADROUTE_GATEWAY_SETTINGS_ROLES` (decision 0014). Everyone sees the current values.

"Mark replied" records a marker only. Nothing is ever posted to Bluesky.

## Run it locally

Prerequisites: Docker with Compose v2, GNU Make, and Go 1.27.4 for the tests (`GOTOOLCHAIN=go1.27.4` is set by the Makefile).

```sh
make up        # builds the images, starts Kafka, Redis, PostgreSQL, ingest, normalize, conv, router, sim, gateway and console
make logs      # follows the logs (JSON lines)
```

Ingest writes to `raw.bluesky` a few seconds after the start. normalize waits for that topic and creates `events` and `events.control`, and conv waits for those and creates `conversations`. The router starts when PostgreSQL is healthy, runs its migrations, waits for `conversations` and creates `routing`. sim waits for the operator API and for `routing`. The gateway waits for both topics by itself and is ready a moment after the router. Each one waits up to 2 minutes and then exits, and the restart policy starts it again, so a cold start needs no manual steps. Every published port listens on `127.0.0.1` only:

| Port | What |
|---|---|
| 9464 | ingest: `/metrics`, `/healthz`, `/debug/pprof/` |
| 9465 | record (only with the `record` profile): the same endpoints |
| 9466 | normalize: the same endpoints |
| 9467 | conv: the same endpoints |
| 9468 | router: the same endpoints. `/healthz` answers 200 after the catch-up |
| 9469 | sim: the same endpoints |
| 9470 | operator API of the router, for example `curl 127.0.0.1:9470/v1/settings`. It has no authentication; on a server only the gateway may reach it. `PUT /v1/operators/{id}` takes `languages`, `capacity` (optional, the router default when absent) and `priority` (optional, 0 to 9, 0 when absent). A PUT replaces all three, so a PUT without `priority` sets it back to 0 |
| 9471 | gateway: `/metrics`, `/healthz`, `/debug/pprof/`. `/healthz` answers 200 when the view has caught up and is fresh |
| 9472 | gateway: `/v1/ws` and `/v1/public/snapshot` directly, without the web server. For tools and tests |
| 9473 | the console: <http://127.0.0.1:9473/>. Set `THREADROUTE_CONSOLE_HOST_PORT` if the port is taken |
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
- `threadroute_router_offers_general_queue_total`: the part of `offers_total` that went to an operator of the general queue (`und`) without the language of the conversation, because no online operator of the shared pool had it
- `threadroute_router_offers_team_overflow_total`: the part of `offers_total` of a conversation in a team queue that went to an operator of the shared pool, because no member of the team could take it. An offer that is both an overflow and under the general queue rule counts in both counters
- `threadroute_router_sla_missed_total`: missed SLA targets, by target
- `threadroute_router_team_changes_total`: teams formed, resized, released, and conversations moved between queues
- `threadroute_router_team_operators_online`, `threadroute_router_team_cap_operators`: online operators in teams, and the team cap in operators (the share times the online operators, rounded down)
- `threadroute_router_team_cap_stops_total`: how often the team cap stopped a company, by case: `form` (no new team, the cap left less than the minimum size) and `grow` (a new or existing team got fewer operators than its size). Counted once per company and team check, so a company that waits for room counts again at every check
- `threadroute_router_conversations`, `threadroute_router_queue_waiting`, `threadroute_router_queue_oldest_wait_seconds`: conversations by state, and the queues by language and group
- `threadroute_router_operators`, `threadroute_router_live_slots`, `threadroute_router_active_teams`
- `threadroute_router_routing_published_total`, `threadroute_router_outbox_unsent`, `threadroute_router_outbox_oldest_unsent_seconds`, `threadroute_router_outbox_pruned_total`: the outbox

On `http://127.0.0.1:9469/metrics` (sim):

- `threadroute_sim_operators_online`: operators the router confirmed as online
- `threadroute_sim_offers_seen_total`, `threadroute_sim_accepts_total`, `threadroute_sim_offers_missed_total`, `threadroute_sim_replies_total`, `threadroute_sim_resolves_total`
- `threadroute_sim_api_calls_total`: operator API calls by action and result (done, already_done, rejected, unavailable, failed)
- `threadroute_sim_consumer_lag_seconds`: time from the append of a routing record until sim read it

The console has three screens. All of them work without signing in:

- <http://127.0.0.1:9473/> is the public page: totals only, from `/v1/public/snapshot`.
- <http://127.0.0.1:9473/#/operator> takes a seat as a demo operator. Pick your languages, switch "Online", and accept the offer that comes. "Mark replied" and "Resolve" end the work. Nothing is posted to Bluesky.
- <http://127.0.0.1:9473/#/supervisor> shows the flow, the queues, the operators, the teams and the open conversations, with filters.

The console service is nginx with the static build of `web/` (`deploy/console.Dockerfile`, `deploy/console.nginx.conf`). It forwards `/v1/ws` (with the WebSocket upgrade) and `/v1/public/snapshot` to the gateway, so the browser talks to one origin. It runs as a non-root user with a read-only root filesystem, sets a Content-Security-Policy that allows only the page's own origin (with one exception: inline `<style>` elements, which the dialog of the console adds to lock the page scroll) and the usual security headers, caches the files under `assets/` for a year (their names hold a content hash) and `index.html` not at all, and caches the public snapshot for one second.

Without a site API (the default) every visitor gets the demo level: no post text and no author IDs. The full level needs a site that answers `GET <THREADROUTE_GATEWAY_SITE_API_URL>` with the bearer token of the browser as `{"user":{"id":1},"role":{"name":"<role>"}}`, a role listed in `THREADROUTE_GATEWAY_FULL_ROLES`, and a way for the browser to bring its sign-in: the site cookie (`THREADROUTE_GATEWAY_SITE_COOKIE`, when the console runs under the site's domain) or a console built with `THREADROUTE_CONSOLE_SITE_TOKEN_KEY`, the `localStorage` key where the site keeps its token. On a laptop the local role takes their place, see "Stand settings from the console".

Useful metrics on `http://127.0.0.1:9471/metrics` (gateway):

- `threadroute_gateway_view_rows`: rows of the in-memory view by kind (conversations, operators, queues, teams)
- `threadroute_gateway_events_total`: records read from the topics, by topic and result (applied, skipped, invalid)
- `threadroute_gateway_lag_seconds`, `threadroute_gateway_stale`: how long the gateway has been behind the topics, and the current stale reason
- `threadroute_gateway_connections`: open WebSocket connections by view and level
- `threadroute_gateway_messages_sent_total`, `threadroute_gateway_bytes_sent_total`: server messages and bytes by type
- `threadroute_gateway_tick_seconds`, `threadroute_gateway_slow_ticks_total`: time of one tick, and ticks that found a client still writing
- `threadroute_gateway_clients_closed_total`, `threadroute_gateway_requests_total`, `threadroute_gateway_site_api_checks_total`, `threadroute_gateway_text_reads_total`, `threadroute_gateway_operators_put_offline_total`

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

Router (`cmd/router`). `THREADROUTE_KAFKA_BROKERS`, `THREADROUTE_LOG_LEVEL` and the PostgreSQL variables work as above. Routing durations are model time: machine time multiplied by the speed factor. The six `TEAM_` values (form load, release load, release time, min and max size, max share) are starting values and the defaults of the console; see "Stand settings from the console" for what wins at a restart.

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
| `THREADROUTE_ROUTER_TEAM_MAX_SHARE` | `0.5` | share of the online operators that all teams together may hold, above 0 and at most 1. The cap is this share times the online operators, rounded down. `1` turns the cap off |
| `THREADROUTE_ROUTER_ASSIGN_WORKERS` | `4` | assignment workers (1 to 64) |
| `THREADROUTE_ROUTER_ASSIGN_INTERVAL` | `250ms` | how often an idle worker looks at the queues (machine time) |
| `THREADROUTE_ROUTER_EXPIRE_INTERVAL`, `THREADROUTE_ROUTER_SLA_INTERVAL`, `THREADROUTE_ROUTER_TEAM_INTERVAL` | `1s`, `1s`, `10s` | how often offers, SLA targets and team sizes are checked (model time) |
| `THREADROUTE_ROUTER_PUBLISH_INTERVAL` | `100ms` | how often an idle publisher looks at the outbox |
| `THREADROUTE_ROUTER_PUBLISH_BATCH` | `500` | most outbox rows sent in one round |
| `THREADROUTE_ROUTER_OUTBOX_KEEP`, `THREADROUTE_ROUTER_PRUNE_INTERVAL` | `1h`, `1m` | how long a sent outbox row is kept, and how often old rows are deleted |
| `THREADROUTE_ROUTER_COUNTS_INTERVAL` | `5s` | how often the state gauges are sampled |
| `THREADROUTE_ROUTER_OUTAGE_LIMIT` | `2m` | a loop that fails without a break this long (database or Kafka down) stops the process, and the restart policy starts it again |
| `THREADROUTE_ROUTER_OPERATOR_REFRESH` | `6h` | an operator whose last operator event is older than this (machine time) is published again, so the gateway keeps it in its view |

Sim (`cmd/sim`). `THREADROUTE_KAFKA_BROKERS` and `THREADROUTE_LOG_LEVEL` work as above. Behaviour durations are model time: sim divides them by the speed factor, which it reads from the router at start. The operators, the capacity, the two medians and the share of missed offers are starting values and the defaults of the console, like the team values of the router.

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_SIM_ROUTER_URL` | `http://127.0.0.1:9470` | base URL of the operator API. Compose sets `http://router:9470` |
| `THREADROUTE_SIM_START_WAIT` | `2m` | wait for the router settings and the routing topic at start |
| `THREADROUTE_SIM_GROUP_ID` | `threadroute-sim` | consumer group on `routing`. sim always starts at the end of the topic |
| `THREADROUTE_SIM_METRICS_ADDR` | `:9469` | listen address of metrics, pprof and health |
| `THREADROUTE_SIM_OUTAGE_LIMIT` | `2m` | when the router has not answered this long, sim stops with an error |
| `THREADROUTE_SIM_OPERATORS` | `200` | number of operators (1 to 9999), IDs `sim-op-0001` and so on |
| `THREADROUTE_SIM_LANGUAGES` | `en:176,ja:6,de:4,fr:4,es:4,pt:3,ko:3` | language spread, scaled to the operator count. The language of an operator depends only on the spread and its number. Every operator also serves the general queue (`und`) |
| `THREADROUTE_SIM_CAPACITY` | `0` | capacity of each operator; 0 takes the router default |
| `THREADROUTE_SIM_SHIFT_ONLINE`, `THREADROUTE_SIM_SHIFT_OFFLINE` | `4h`, `30m` | shift parts, with a random offset per operator |
| `THREADROUTE_SIM_ACCEPT_DELAY_MIN`, `THREADROUTE_SIM_ACCEPT_DELAY_MAX` | `1s`, `8s` | uniform delay before an accept |
| `THREADROUTE_SIM_MISSED_OFFER_SHARE` | `0.03` | share of offers left to time out on purpose |
| `THREADROUTE_SIM_HANDLING_MEDIAN` | `45s` | median of the log-normal time from accept to reply, each draw capped at 20 times the median |
| `THREADROUTE_SIM_RESOLVE_MEDIAN` | `3m` | median of the time from reply to resolve, capped the same way |
| `THREADROUTE_SIM_SEED` | `1` | seed of the random generator |

Gateway (`cmd/gateway`). `THREADROUTE_KAFKA_BROKERS`, `THREADROUTE_LOG_LEVEL` and the Redis variables work as above. Durations are machine time.

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_GATEWAY_METRICS_ADDR` | `:9471` | listen address of metrics, pprof and health |
| `THREADROUTE_GATEWAY_HTTP_ADDR` | `:9472` | listen address of `/v1/ws` and `/v1/public/snapshot` |
| `THREADROUTE_GATEWAY_PATH_PREFIX` | empty | put before both paths, for a web server that forwards a prefix without removing it. Compose sets it empty, because the console forwards the paths as they are |
| `THREADROUTE_GATEWAY_ROUTER_URL` | `http://127.0.0.1:9470` | base URL of the operator API. Compose sets `http://router:9470` |
| `THREADROUTE_GATEWAY_ROUTER_TIMEOUT` | `5s` | limit of one call of the operator API |
| `THREADROUTE_GATEWAY_SITE_API_URL` | empty | the "who am I" URL of the site API. Empty: no token is checked and every connection gets the demo level |
| `THREADROUTE_GATEWAY_SITE_API_TIMEOUT`, `THREADROUTE_GATEWAY_SITE_API_CACHE` | `3s`, `1m` | limit of one call, and how long a successful answer is kept (under a hash of the token). An error or a timeout gives the demo level |
| `THREADROUTE_GATEWAY_FULL_ROLES` | empty | comma-separated role names of the site that get the full level. Empty: nobody |
| `THREADROUTE_GATEWAY_SETTINGS_ROLES` | empty | comma-separated role names of the site that may change the stand settings. Empty: nobody. Independent of the full roles |
| `THREADROUTE_GATEWAY_SITE_COOKIE` | empty | name of the site's sign-in cookie, read from the WebSocket opening request and checked with the site API. Empty: not read. Keep the real name in your own `.env` |
| `THREADROUTE_GATEWAY_LOCAL_ROLE` | empty | one site role for every connection, without the site API, for a developer machine. Needs every allowed origin to be a loopback origin with a scheme; the gateway refuses to start otherwise |
| `THREADROUTE_GATEWAY_ALLOWED_ORIGINS` | `localhost:*,127.0.0.1:*` | browser origins that may open the WebSocket, as host patterns or `scheme://host` patterns. A page from the same host is always allowed. Compose sets the console address (`127.0.0.1:9473,localhost:9473`); a server sets the site's host. With the local role, write each one with its scheme, for example `http://127.0.0.1:9473` |
| `THREADROUTE_GATEWAY_TRUST_PROXY` | `false` | take the client address from the right-most `X-Forwarded-For` entry. Only behind a web server that sets it; compose sets `true` |
| `THREADROUTE_GATEWAY_DEMO_OPERATORS` | `5` | most demo operators at the same time; one more gets close code 4503 |
| `THREADROUTE_GATEWAY_MAX_CONNECTIONS`, `THREADROUTE_GATEWAY_MAX_CONNECTIONS_PER_ADDR` | `2000`, `20` | most WebSocket connections in total and per client address |
| `THREADROUTE_GATEWAY_TICK` | `250ms` | time between two state messages to one browser (250ms to 10s) |
| `THREADROUTE_GATEWAY_OFFLINE_GRACE` | `15s` | a person stays online this long after the connection closed |
| `THREADROUTE_GATEWAY_WRITE_TIMEOUT`, `THREADROUTE_GATEWAY_PING_INTERVAL` | `10s`, `20s` | a browser that does not take one message in time is closed; WebSocket ping interval |
| `THREADROUTE_GATEWAY_LAG_LIMIT`, `THREADROUTE_GATEWAY_SOURCE_TIMEOUT` | `10s`, `5s` | the data is stale when the gateway is behind the topics longer than the lag limit, or could not read them for the source timeout |
| `THREADROUTE_GATEWAY_FORGET_AFTER` | `1h` | a closed conversation, a released team and an offline person are kept this long after their last event |
| `THREADROUTE_GATEWAY_SNAPSHOT_FRAME_ROWS` | `500` | most conversation rows in one frame of a first state |

Console. These are read only by `deploy/compose.yaml`; the build settings of the console itself are in [web/README.md](web/README.md).

| Variable | Default | Meaning |
|---|---|---|
| `THREADROUTE_CONSOLE_HOST_PORT` | `9473` | host port of the console. The compose default of the allowed origins follows it |
| `THREADROUTE_CONSOLE_SITE_TOKEN_KEY` | empty | the `localStorage` key of the site token, built into the console. Empty: the console never sends a token |

### Stand settings from the console

Some numbers of the router and the simulator can be changed from the supervisor screen while the stand runs, without a restart (decision 0014). The router keeps the current set in PostgreSQL and uses the team part itself. The simulator reads its part from the router every 5 seconds and applies a new one step by step. The gateway checks every change (the right, the bounds and the version) and passes it to the router. The console only hides the controls; the check is in the gateway.

What can be changed:

| Setting | Field | Service | Default | Bounds | How fast it acts |
|---|---|---|---|---|---|
| Simulated operators | `sim.operators` | sim | 200 | 10 to 500 | within a minute. The active operators are always the numbers 1 to N. Fewer: the highest numbers go offline, and their conversations go back to the queue. More: operators put aside come back first, then new ones are created. The language of an operator depends only on the spread and its number, so the spread keeps its proportions at every count and no operator changes its language at a change or a restart |
| Capacity of an operator | `sim.capacity` | sim | 4 | 1 to 20 | within a minute. An operator above the new capacity keeps their conversations and gets no new offers until the load is below it |
| Handling time, median | `sim.handling_median_us` | sim | 45 s | 5 s to 10 min | for conversations accepted after the change |
| Resolve time, median | `sim.resolve_median_us` | sim | 3 min | 10 s to 30 min | for conversations replied to after the change |
| Share of missed offers | `sim.missed_offer_share` | sim | 0.03 | 0 to 0.5 | at once |
| Load that forms a team | `teams.form_load` | router | 20 | 2 to 500 | at the next team check (10 s) |
| Load that releases a team | `teams.release_load` | router | 8 | 1 to the form load minus 1 | after the release time |
| Release time | `teams.release_after_us` | router | 15 min | 1 min to 2 h | gradually |
| Team size, min and max | `teams.min_size`, `teams.max_size` | router | 2 and 20 | 1 to 50, min not above max | at the next team check (10 s) |
| Share of operators in teams | `teams.max_share` | router | 0.5 | 0.05 to 1 | growth at once, shrinking gradually |

The defaults are the values of the environment (the table above shows the code defaults). The whole set is checked together, with the same rules as at start. A team number that goes down does not force a team to shrink (decision 0009 has no forced exit): teams above it stop growing and get smaller as members leave, and the console says "acts gradually" for such values. Team sizes are the exception: the normal size check acts on them at the next team check. The speed factor, the timeouts and targets, worker counts, intervals, shifts and the language spread are not in the console.

Who may change them. Everyone sees the current values, read-only. Changing them needs a site role, and the gateway has two lists that do not depend on each other: `THREADROUTE_GATEWAY_FULL_ROLES` for the full level (post text and IDs) and `THREADROUTE_GATEWAY_SETTINGS_ROLES` for the right to change settings. A role can be in either list, in both or in none. The gateway learns the role in one of these ways, and the first that applies wins:

1. The local role, `THREADROUTE_GATEWAY_LOCAL_ROLE`: every connection gets this role, without the site API. It is for a stand on a developer machine, where there is no site cookie. The gateway starts with it only when every allowed origin is a loopback origin written with its scheme: `http://` or `https://`, then `localhost`, an address in `127.0.0.0/8` or `[::1]`, and an optional port, with no path and no `*`. It also refuses (HTTP 403) every connection whose `Origin` header is not such an origin, so a tool on port 9472 must send one. A public stand lists its public origin, so it cannot run with a local role.
2. The site cookie, `THREADROUTE_GATEWAY_SITE_COOKIE`: the gateway reads the site's sign-in cookie from the WebSocket opening request and checks it with the site API. This works when the console and the gateway are served under the site's domain. Page code never reads the cookie. Keep the real cookie name in your own `.env`.
3. A token in the first message of the console (`THREADROUTE_CONSOLE_SITE_TOKEN_KEY`), as before.
4. Nothing: the demo level, without the right to change settings.

To try it on the local stack:

1. Put these lines into the root `.env` (the role name is an example; any name works when both lists name it):

   ```sh
   THREADROUTE_GATEWAY_LOCAL_ROLE=guest
   THREADROUTE_GATEWAY_FULL_ROLES=guest
   THREADROUTE_GATEWAY_SETTINGS_ROLES=guest
   THREADROUTE_GATEWAY_ALLOWED_ORIGINS=http://127.0.0.1:9473,http://localhost:9473
   ```

   The allowed origins must be set, because the compose default is written without a scheme, and the gateway does not start with a local role and that default. With another `THREADROUTE_CONSOLE_HOST_PORT`, write that port here.
2. Run `make up`.
3. Open <http://127.0.0.1:9473/#/supervisor>. The "Stand settings" panel shows the current values with "Apply" and "Reset to defaults". The simulator part appears after `sim` has reported its values, a few seconds after its start. Without the local role the same panel is read-only.
4. Change a value, for example "Share of operators in teams", and press "Apply". The change shows in the list of last changes, marked `local`, and on the trend strip. `curl -s 127.0.0.1:9468/metrics | grep settings_value` shows the new value in the router.
5. A second change within 10 seconds is refused, and the console says when you can apply again.

A live check on the laptop is described under "Results", in "Stand settings from the console, live check on the laptop".

Where the values live. The team values of the router environment and the values that `sim` reports at its start are the defaults. While nothing is stored, the router uses its environment. The first report of `sim` already stores a set (version 1, marked `simulator` in the change log), with the team values of the router at that moment. From then on the stored set wins at every start, also over a new value in the environment. A new environment value only becomes the new default, and "Reset to defaults" in the console brings the defaults back. So on a stack that has run once, a new `THREADROUTE_ROUTER_TEAM_*` or `THREADROUTE_SIM_*` value of this list acts only after "Reset to defaults", or after the PostgreSQL volume is removed (`down -v`, which removes all routing data too). A router whose team values are outside the bounds runs with them as before and does not offer the settings; the console then shows no panel. A simulator whose values are outside the bounds runs from its environment and does not read the settings.

Limits and the change log. The stand takes at most one change per 10 seconds, for all users together. A change carries the version it was based on; when someone else changed the set first, it is refused, and the console shows the new values. "Reset to defaults" is one change like any other. Every change is written to a log table in PostgreSQL with its time, the new version, the old and the new value of each changed field, and a short label of who made it: `user:` and the site user ID, `local` for the local role, or `simulator`. The gateway sends the newest 20 changes, and the console shows 5 of them with a button for the rest. The gateway sends the label only to connections that may change settings, and the console shows it only at the full level.

The trend strip on the supervisor screen has one point every 10 seconds for the last hour (at most 360 points): waiting conversations, the longest wait, resolves in the last minute, teams, operators in teams and operators online. Changes are marked on it from the change log. The points live in the memory of the gateway, so a gateway restart loses them, and there is a gap while the data is stale. A point holds counts only, so every level sees it.

New metrics:

- Router (`:9468`): `threadroute_router_settings_value{name}` (each value in use, by field name; durations in microseconds of model time), `threadroute_router_settings_version` (0 while nothing is stored or when the router does not offer the settings), `threadroute_router_settings_changes_total{source}` (`console` or `simulator`)
- Sim (`:9469`): `threadroute_sim_settings_value{name}`, `threadroute_sim_settings_version` (0 when `sim` runs from its environment), `threadroute_sim_settings_changes_total`, `threadroute_sim_operators_active` (in the shift cycle), `threadroute_sim_operators_put_aside` (put aside by a lower operator count)
- Gateway (`:9471`): `threadroute_gateway_settings_changes_total{outcome}` (`done`, `already_done` or the code of the rejection), `threadroute_gateway_stand_settings_version` (-1 when the router offers none), `threadroute_gateway_trend_points_total`, `threadroute_gateway_identities_total{source}` (`local`, `cookie`, `token` or `none`). `threadroute_gateway_site_api_checks_total` has the outcome `local` for the local role, and `threadroute_gateway_requests_total` counts `change_settings` too

What this is for: seeing how the rules behave, for example how teams form and shrink when a number changes, or how the queues react to more or fewer operators. It is not a way to measure one setting against another. A live change is mixed with the time of day: the traffic before and after the change is not the same. Numbers for such comparisons will come from a replay of a recorded day (stage 5, planned), not from the console.

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

A second, short run on the same day (12:58 to 13:08 UTC, cold start, same settings) checked three things the first one did not: the language picker of the operator screen (a language added, 0 policy violations with the menu and a tooltip open), the text in the supervisor's message dialog at the full level, and the operator screen after a gateway restart (row above).

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
| Sessions without an operator | 52 closed while waiting (3 in a language with no operator), 85 still waiting at the end of the window (42 in a language with no operator, mostly nl, tr, nb) | at the time of this run a known language without operators waited until the conversation closed, and only `und` had a general queue. Since 2026-11-24 the general queue also takes such a conversation (see stage 4) |
| Dedicated teams | none formed | no company reached 20 active conversations. Teams ran live later: 5 in the first stage 4 run, and 6 under the team cap in the short check with lowered thresholds below |
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

### Stage 4: gateway and console

Conditions for every row: 41 minutes on 2026-11-24 from 12:12:22 to 12:53:38 UTC, a cold start of the whole compose stack (with the gateway and the console) on the same laptop with Docker Desktop, live traffic of a midday hour (UTC). Default settings of every service, except the host ports of Redis and PostgreSQL (taken on this laptop) and the settings of the full level: a site API URL, one full role and a token key for the console. The images were built before, so the cold start does not include a build from scratch. The browser was a headless Chromium driven by Playwright, and some checks used a small WebSocket client; both talked to the console on `127.0.0.1:9473`. The full level used a local stub of the site API with a synthetic token, not the real site. The gateway was restarted twice and Kafka was paused once near the end, so the counters below are from a dump at minute 31, before those tests.

The operators are simulated, and so is the routing that a demo operator joins. The waiting times below describe the simulator and its settings in one laptop run.

| Measurement | Value | Note |
|---|---|---|
| Cold start | one `up -d --build`, no manual step. The gateway answered ready 12 s after the command started | 0 restarts of any container during the whole run |
| View of the gateway at minute 31 | 1,673 conversations, 218 operators, 17 queues, 5 teams | closed conversations stay in the view for 1 hour; the supervisor list holds only open ones (2,496 rows against 2,498 open in `conv` at minute 41) |
| Events applied by minute 31 | `conversations` 2,078, `routing` 9,208 | 0 skipped, 0 invalid, 0 fetch errors. `lag_seconds` was 0 in all 75 samples (every 30 s) |
| Bytes sent by minute 31 | `state` 5.9 MB, `messages` 30 KB, `welcome` 12 KB, `result` 5 KB, `status` 1 KB | about 30 connections in the window, mostly short test sessions |
| Tick | 7,330 ticks, mean 0.18 ms, 99.5% under 1.6 ms, all under 26 ms | 0 slow ticks |
| Memory, median and highest | gateway 10.5 / 13.6 MiB, console (nginx) 9.7 / 10.1 MiB | container memory from `docker stats` every 30 s. One day of state is not measured yet |
| First state of a supervisor | minute 5: 1 frame, 20 rows, 33 KB. Minute 31: 4 frames, about 1,700 rows, 556 KB (demo) and 703 KB (full). Minute 41: 5 frames, 2,496 rows, 801 KB (demo) | no WebSocket compression |
| Time from "online" to the first offer of a demo operator | 4 operators that went online together at minute 6: 374, 450, 463 and 469 s; one more at minute 5: no offer within 300 s. From minute 13 on: 2.1, 2.1, 2.4, 3.9, 3.9, 5.2, 5.4, 10.2 and 10.4 s | the router picks the operator with the lowest number of live offers and assignments, and on a tie the lowest operator ID. `sim-op-` sorts before `web-`, so a person gets an offer only when no simulated operator with the language has a lower or equal load. At minute 10, 54 of 184 online simulated operators had none. Decision 0012 promised under a minute; the early waits are longer and are reported here, routing was not changed. This row is from before the routing change of 2026-11-24 (operator priority and general queue); see the next run below |
| Demo operator, whole path | online, offer, accept, "mark replied", resolve: done in the browser and in 11 sessions of the WebSocket client | every result `done` |
| Demo level on the wire | 0 `did:`, 0 `at://` and 0 text fields in every demo frame of operators and supervisors, and in the public snapshot | the public snapshot is about 1 KB, cached by nginx for 1 s (HIT and EXPIRED headers seen) |
| Full level | with a token the stub accepts, an operator saw text in its conversation and a supervisor saw text in the message dialog (second run); a token without the role and a rejected token got the demo level | site API checks: 3 full, 1 signed in, 1 rejected, 1 cached, 26 without a token |
| Demo seats | the sixth demo operator was closed with 4503 `no_operator_seat`, and the console showed "All demo operator seats are taken" | one address could take all 5 seats |
| Wrong `Origin` | the WebSocket upgrade got HTTP 403, through nginx and on port 9472 | |
| Page reload | the operator was live again 2.3 s after the reload, with the same operator ID, still online and still holding its conversation | after the page was closed for 25 s, the operator was offline and held 0 conversations |
| Gateway restart (`docker compose restart gateway`) | ready 1.7 s after the restart began, after replaying 2,209 `conversations` and 9,770 `routing` events; the browser was live again 1.5 s after it lost the connection | a clean stop puts its own operators offline, so the orphan sweep had nothing to do. In a second run an operator who held a conversation came back after the restart as a new operator, offline, with 0 conversations, and the page gave no note about it |
| Gateway killed (SIGKILL) and started | ready 2 s after the kill, after replaying 2,561 and 11,263 events; the browser was live again after 3 s; the orphan sweep put the 1 operator of the killed process offline within 70 s | |
| Kafka paused for 40 s | `/healthz` answered 503 for about 32 s, from about 7 s after the pause until the unpause; the public snapshot said `stale` for about 31 s; the console showed the stale banner for 33.5 s and stopped its timers, and was live again right after the unpause | no container exited or restarted. A stopped broker (connections refused) was not tested |
| Console build | JavaScript 468 KB in 4 files (147 KB with gzip; the largest file 280 KB, 86 KB with gzip), CSS 31 KB (7 KB with gzip), fonts 75 KB, 600 KiB in total | image 60.6 MB |

What this run does not show: a deleted post that disappears from an open screen. `conv` published 42 "message removed" events in the run, but none of them was for one of the 4 conversations a full-level supervisor watched for 16 minutes, so this path was only tested with the fake gateway. It also does not show one day of gateway memory, a busy hour, or the real site API.

### Stage 4: after the routing change of 2026-11-24

The project owner accepted two changes of the routing rules after the run above (decision 0009, "Changes after the first live run of the console"): an operator priority that breaks a tie between operators with the same lowest load (the gateway gives people 1), and the general queue for a language that no online operator of the shared pool has.

Conditions: 24 minutes on 2026-11-24 from 13:50:55 to 14:14:56 UTC, a cold start of the whole compose stack on the same laptop (`up -d` with images built just before), live traffic of an afternoon hour (UTC). Default settings of every service, except the host ports of Redis and PostgreSQL. Only the demo level was used: no site API and no tokens. The demo operators were a small WebSocket client with the languages `en` and `und`, as the console sends by default. The operators are simulated, so the waiting times describe the simulator and its settings.

| Measurement | Value | Note |
|---|---|---|
| Cold start | the gateway answered ready 10 s after `up -d` started | 0 restarts of any of the 10 containers during the run |
| Time from "online" to the first offer of a demo operator | 3 operators together at minute 5: 3.1, 3.1 and 3.1 s. 3 together at minute 7: 0.7, 2.6 and 3.4 s. 3 together at minute 16: 1.0, 1.0 and 2.5 s | before the change: 374 to 469 s at minutes 5 to 6 (row above). Each try then accepted, marked a reply and resolved, all `done` |
| Offers under the general queue rule | 22 of 2,345 offers (`threadroute_router_offers_general_queue_total` and a count in PostgreSQL agree) | 1 by minute 9, 10 by minute 13, 20 by minute 22 |
| Wait from queue to accept, languages without a simulated operator | 22 accepted, in 7 languages (ar, nl, ca, zh, da, fi, sv): median 5.1 s, highest 8.0 s | in the stage 3 run such conversations waited until `conv` closed them, and in the first stage 4 run 3 to 11 minutes. No queue of such a language was seen in the 45 samples of `queue_oldest_wait_seconds` (every 30 s) |
| Wait from queue to accept, languages with simulated operators | 2,241 accepted: median 5.4 s, 95th percentile 26 s | the highest `queue_oldest_wait_seconds` in the samples: de 95 s, es 96 s, en 9 s. The wait is counted from the queue time, which a conversation keeps when it returns to the queue, so a few accepts that came after an earlier offer or assignment count the time with that operator too. The highest value of this column (780 s) is not explained and is left out. German and Spanish still have 4 simulated operators each, and the general queue does not help them, because those operators are online |
| Offers to people | 11 of 2,345 | 9 of them are the first offers of the tries above |
| Database invariants | 0 violations in 23 checks during the run and 1 at the end | the checks of stage 3, plus: every live offer goes to an operator with the language of the conversation, or with `und` for a conversation of the shared queue |
| Cost of the queries | `WaitingConversations` 1.9 ms before and 2.6 ms after for the first 100 rows, 2.8 ms and 3.2 ms for all of them; `PickOperator` 1.2 ms and 1.4 ms for `en`, 0.04 ms (nothing found) and 1.4 ms (general queue) for a language without operators | `EXPLAIN (ANALYZE, BUFFERS)`, median of 7 runs, on a test database with 205 operators, 2,500 waiting conversations in 15 languages, 402 live slots. A first version that checked the rule with a sub-query per waiting conversation took 49 ms, so the rule reads the languages of the online shared pool once per statement. No index was added |

### Team cap, short check on the laptop

The project owner accepted a cap on the operators that teams hold on 2026-11-24 (decision 0009, "A cap on the operators that teams hold"). At laptop traffic few companies reach the default team threshold, so this check used lowered thresholds to make the cap bind.

Conditions: about 20 minutes on 2026-11-24 from 14:58:35 to 15:19:20 UTC, a cold start of the whole compose stack on the same laptop (`up -d --build`), live traffic of an afternoon hour (UTC). Default settings of every service, except the host ports of Redis and PostgreSQL and three router settings: `THREADROUTE_ROUTER_TEAM_FORM_LOAD=6`, `THREADROUTE_ROUTER_TEAM_RELEASE_LOAD=2` and `THREADROUTE_ROUTER_TEAM_MAX_SHARE=0.2`. No console, no site API. Router metrics were sampled every 30 s (41 samples), the database invariants every 60 s. The operators are simulated, so the waiting times describe the simulator and these lowered thresholds.

| Measurement | Value | Note |
|---|---|---|
| Online operators, operators in teams, cap | online 180 to 185; in teams 0, 3, 10, 21, 26, 29, 35 in the first 3.5 minutes, then 36 or 37; cap 36 or 37 | the teams reached the cap at minute 4 and stayed there |
| Samples with the teams above the cap | 7 of 41, each by 1 (37 against 36) | not caused by a join: at 15:08:37 a pool operator came online (cap 37), at 15:08:45 a team grew to exactly 37, and at 15:09:22 and 15:09:27 two pool operators went offline (cap 36). A replay of all 456 operator events of the outbox found 43 joins and 0 joins that left the teams above the cap |
| Teams | 6 formed, all in the first 4 minutes; 36 resizes; 0 released | |
| `team_cap_stops_total` | `form` 0 at minute 6, then 6, 115, 394, 786, 1,236 and 1,811 at the end; `grow` 4 at minute 4.5, then 556 at the end | counted once per company and team check (every 10 s), so a company that waits for room counts again at every check |
| Shared pool | `en` shared queue: oldest wait 0 s in 39 of 41 samples, 13.6 s and 17.8 s in the last two; waiting in shared queues 0 to 35 | 1,943 offers made by the end |
| Team queues | waiting 0 at minute 4, 101 at minute 10, 303 at the end; oldest `en` team wait 107 s at minute 7, 383 s at minute 12, 980 s at minute 20 | see the known gaps |
| Restarts | 0 for each of the 10 containers | |
| Database invariants | 0 violations in 21 checks during the run and 1 at the end | the checks of stage 3, plus the language rule of the live offers |

### Team overflow, short check on the laptop

The check above showed that a team that the cap keeps small makes its company wait while the shared pool is free. The project owner accepted the overflow rule on 2026-11-24 (decision 0009, "Overflow from a team to the shared pool"): when no member of the team can take a conversation of the team queue, an operator of the shared pool takes it by the rules of the shared queue. This check repeats the cap check with the same lowered settings, so the two runs can be compared.

Conditions: about 21 minutes on 2026-11-24 from 15:49:47 to 16:10:47 UTC, a cold start of the whole compose stack on the same laptop (`up -d --build`), live traffic of an afternoon hour (UTC), about 30 minutes after the cap check, so the traffic is not the same. Default settings of every service, except the host ports of Redis and PostgreSQL and the same three router settings: `THREADROUTE_ROUTER_TEAM_FORM_LOAD=6`, `THREADROUTE_ROUTER_TEAM_RELEASE_LOAD=2` and `THREADROUTE_ROUTER_TEAM_MAX_SHARE=0.2`. No console, no site API. Router metrics were sampled every 30 s (42 samples), the database invariants every 60 s. The operators are simulated, so the waiting times describe the simulator and these lowered thresholds.

| Measurement | This run | Cap check without the overflow | Note |
|---|---|---|---|
| Online operators, operators in teams, cap | online 180 to 185; in teams 36 or 37 from minute 4; cap 36 or 37 | the same | 7 of 42 samples had the teams 1 above the cap, each after pool operators went offline. A replay of the 452 operator events of the outbox found 42 joins and 0 joins that left the teams above the cap |
| Teams | 9 formed, all in the first 4 minutes; 25 resizes; 0 released | 6 formed, 36 resizes, 0 released | |
| Team queues | waiting 0 in every sample until minute 15, then 5 to 29, and 51, 65 and 90 in the last three samples; oldest team wait 0 until minute 15, then 13 to 86 s | waiting 101 at minute 10 and 303 at the end; oldest wait 383 s at minute 12 and 980 s at the end | |
| Shared queues | waiting 0 to 8 until minute 15, then 14 to 155 at the end; oldest `en` wait 0 until minute 15, then 7 to 102 s | `en`: oldest wait 0 s in 39 of 41 samples, 13.6 s and 17.8 s in the last two | the other shared languages (German, Spanish and others with few operators) had an oldest wait of up to 152 s from minute 11 |
| Offers | 2,233 by the end; 354 of them overflow from a team queue to the pool (`offers_team_overflow_total`), 25 under the general queue rule | 1,943 | the outbox at the end: 2,278 offered events, 572 from a team queue to a member and 365 from a team queue to the pool (the counter was sampled about 30 s earlier) |
| Live slots and waiting conversations | minute 15: 664 live, 3 waiting; end: 676 live, 245 waiting | minute 15: 531 live, 161 waiting; end: 666 live, 338 waiting | the demand (live plus waiting) was about the same in both runs. Online capacity was about 720 slots (180 operators of 4) |
| Free slots, one snapshot right after the end | 36 of 716: 33 in the shared pool, all with operators of small languages (every online pool operator with `en` was full), 3 of team members | | one snapshot only; which group the free slots were in during the run was not sampled |
| `team_cap_stops_total` | `form` 2,295 and `grow` 850 at the end | `form` 1,811 and `grow` 556 | counted once per company and team check |
| Restarts | 0 for each of the 10 containers | 0 | |
| Database invariants | 0 violations in 21 checks during the run and 1 at the end | 0 | the checks of the cap check with the language rule relaxed to "the language or `und`", plus a new one: at the time of each offer, read from the outbox, the operator was in the shared pool or a member of the team of that team queue with the language |
| Cost of the queries | `WaitingConversations` 7.4 ms before and 1.0 ms after for the first 100 rows, 7.8 ms and 2.8 ms for all of them (40 ms and 2.0 ms with a generic plan); `PickOperator` 0.06 to 1.0 ms before and 0.4 to 1.4 ms after, 1.3 ms for an overflow | | `EXPLAIN (ANALYZE, BUFFERS)`, median of 7 runs, on a test database with 200 operators (100 online in 30 teams), 2,500 waiting conversations in 15 languages, half of them in team queues, 520 live slots. The old `WaitingConversations` compared the group of each operator with each conversation (a nested loop over 2,500 rows); the new one reads the free languages of the pool and the free (company, language) pairs of the members once per statement. The overflow test in `PickOperator` also runs once per statement. No index was added |

What the comparison shows: with about the same demand, the overflow used the slots that the strict teams left free. Until minute 15 the team queues stayed empty and the shared queues kept up, where the cap check had 161 conversations waiting at that minute, almost all of them in team queues. After minute 15 the demand was above the online capacity in both runs. Then the shared `en` queue did not keep up any more: its oldest wait reached 102 s at the end, against 18 s in the cap check. The overflow spreads the wait over all companies, where the cap check put it on the team companies alone. The two runs saw different 20 minutes of the stream, so the last minutes are not an exact comparison.

### Stand settings from the console, live check on the laptop

This check shows how fast a change from the console acts on the stand (decision 0014). It does not compare one setting with another; see "What this is for" in "Stand settings from the console".

Conditions: one laptop, on 2026-11-24 (22:00 to 22:45 UTC), the whole compose stack on live traffic for about 45 minutes. Default settings at start (200 simulated operators). The real console was driven by a headless browser with the local role, in two browser windows. Metrics were read every 2 to 5 seconds, so the times below are known to about 5 seconds. Each time is the first read that showed the effect, so it is an upper bound. The operators are simulated, so the queue numbers describe the simulator and these settings.

13 settings versions were applied from the console. Every applied change was visible in the status line of the console, in the answer of `GET /v1/settings`, as a row of the change log table, in the settings metrics of the router, the simulator and the gateway, as a mark on the trend strip, and in the second browser window without a reload. The round trip of one Apply was 25 to 30 ms.

Time from Apply to effect:

| Change | Effect | Time after Apply |
|---|---|---|
| Simulated operators 200 to 60 | the simulator put 140 operators aside | within 2.4 s |
| | the router showed 54 operators online; waiting conversations went from 0 to 157 in that time | within 8.4 s |
| Simulated operators 60 to 200 | 182 operators online | within 3 s |
| | after the operators came back, teams grew to the cap again: 5 to 12 teams, 15 to 36 team operators | within 13 s |
| Share of operators in teams 50% to 20% | the new cap (27 to 10 operators) was in effect | within 1 s |
| Team size max 20 to 3 | teams were trimmed: online team operators 24 to 13 | within 11 s |

What did not happen:

- The share was lowered from 50% to 20% in the same change as the load that forms a team (20 to 6) and the load that releases it (8 to 2). After it, 24 online team operators stayed above the cap of 10 for the 6 minutes that were watched, and no new team formed. Decision 0009 has no forced exit from a team, so the teams only stop growing. In that time the cap stopped 1,153 attempts to form a team (`threadroute_router_team_cap_stops_total`).
- After the operators went back from 60 to 200, the queue did not drain. During the run the traffic grew from about 1.0 to between 3.5 and 4.3 messages per second, the operator slots were full (680 of 720), and about 1,300 conversations were waiting 12 minutes later. So this run shows how fast a setting acts, not how many operators the stand needs.

Other checks:

| Check | Result | Note |
|---|---|---|
| A value outside the bounds | refused | as described in "Stand settings from the console" |
| A second change 166 ms after the first | refused, then applied after the 10 seconds | |
| Two people change the set at the same time | one change applied; the other person was told and kept their edits | |
| Restart of the router, the simulator and the gateway, one at a time, with a stored set that is not the default (120 operators, team share 30%) | each came back within seconds with the stored set | the trend history started again after the gateway restart |
| Empty list of settings roles | the panel was read-only, and a change sent by hand was refused | |
| Database invariants | 0 violations of the routing rules in 44 checks | the same queries as in the earlier checks. One of them replays old offers against the languages the operators have now. After the simulator restart it reported 13 and then 14 rows in 7 of the 44 checks: offers made before the restart, compared with the changed languages (the defect below). With the languages at the time of each offer it reports 0 |
| Error-level log lines | 0 in the router, the simulator, conv, normalize and ingest | |
| Gateway | 20 to 27 MB of memory; mean tick from 0.06 ms to 0.39 ms as the view grew; 0 slow ticks | |

What the check found, and what was fixed after it:

- The first version of the simulator laid out the operators in blocks by language, so a restart at another count gave 14 operators another language. The rule is now "language by number": the language of an operator depends only on the language spread and its number, and the active operators are always the numbers 1 to N (decision 0014, point 3A). A second live check on a fresh stack went 200 to 120, restart, 200, 300, restart, 250, 500. No operator changed its language at any step (0 differences in the stored languages of all operators), the counts per language were exactly 176/6/4/4/4/3/3 at 200, and there was no error-level log line.
- The trend strip counted offline team members as "operators in teams". It now counts the online ones, the number that the cap limits.

### Known gaps

- An edit of a post is not pushed live to a conversation that is open on a screen.
- The router never deletes operators. Every visit to the operator screen leaves one `web-` operator in the database, and the router publishes it again every 6 hours. In this 41-minute run the view grew to 225 operators, 25 of them `web-`.
- A conversation whose first events have left the topics (24 hours) does not show in the supervisor list.
- Tokens that the site rejects are not cached, so every connection with such a token calls the site API.
- There is no rate limit for new connections per address, and one address can take all 5 demo seats.
- The access level and the right to change stand settings are decided once per connection. A role taken away in the site counts only for the next connection.
- The orphan sweep assumes one gateway process. A second process would put the operators of the first one offline.
- The answer shape of the real site API is not verified live; this run used a local stub.
- The site cookie holds a token that lives a short time, and the site refreshes it when its own pages load. A person who opens the console with an old cookie gets the demo level until they open a site page. This is not checked on the real site yet (decision 0014, point 7A).
- The trend history lives in the memory of the gateway. A gateway restart loses it, and the strip starts empty.
- A team number that goes down acts slowly, because decision 0009 has no forced exit from a team. In the live check of the console, 24 online team operators stayed above a lowered cap of 10 for the 6 minutes that were watched.
- The CPU of PostgreSQL grows with the length of the queue: 2 to 5% of one core with an empty queue, up to about two cores with 1,000 to 2,800 conversations waiting on the laptop. The busiest statements are the operator pick (`PickOperator`) and the list of waiting conversations (`WaitingConversations`).
- After the first start, a new team or simulator value in the environment acts only after "Reset to defaults" in the console, because the stored set wins at a restart.
- A person with no conversation now gets the next conversation in their languages (1 to 3.4 s in the run after the change). A person who already holds one still loses to an idle simulated operator, because the priority only breaks a tie of the load (decision 0009). With the console capacity of 2 this is a short wait while the stream is busy, but it is not measured for a quiet hour.
- A language whose few operators are online but busy (German and Spanish with 4 simulated operators each) still waits for them, up to 96 s for the oldest one in the run after the change; the general queue is used only when no online operator has the language.
- A team that the cap keeps small no longer makes its company wait while the pool is free: the shared pool takes what the team cannot (the overflow). In the overflow check the team queues stayed empty while the pool had free English slots, where the cap check without the overflow had 303 waiting and an oldest wait of 980 s. The other direction is still open: a free slot of a team member serves only its company, also when the shared queue waits. How many member slots were free while the shared queue waited was not sampled (one snapshot right after the overflow check found 3 of 144). The decision lists "idle team members help the shared pool" as an option for later.
- In both laptop checks of the team rules, the demand passed the online capacity of the 200 simulated operators after about 15 minutes of an afternoon hour, and from then on the English queues grew. These checks show the routing rules, not the number of operators a real day needs.
- The overflow ends the strict rule that only the team serves a company with a team. A busy company with a team can now get more operators than its team size, from the shared pool, in the same queue order as everyone else. How much of the pool one busy company takes in a busy hour is not measured yet.
- The team cap counters grow fast when companies wait for room: `form` reached 2,295 in 21 minutes of the overflow check, because each company is counted again at every team check (every 10 s).
- The first state of a supervisor is sent without WebSocket compression: 801 KB for 2,496 conversations after 41 minutes.
- The Content-Security-Policy of the console allows inline `<style>` elements, because the dialog adds one to lock the page scroll.
- After a restart of the gateway, a person in the operator screen gets a new operator that is offline, and the conversations they held went back to the queue. Resume tokens live only in the memory of the gateway, and the console does not say what happened.
- The memory of the gateway over one day of the live stream is not measured yet.
- Where the gateway runs in production is not decided; it gets its own decision later.

These tables are filled only with real measurements, together with the conditions they were taken under.

## License

MIT, see [LICENSE](LICENSE).
