# threadroute

threadroute reads the public Bluesky post stream, turns it into conversations and routes them to simulated support agents. It is a learning project about high-load Go services and real-time interfaces. The operators are simulated: no real person answers anything. This is a personal, non-commercial project.

## Status

Stage 1 of 6 is implemented: ingest of the live stream into Kafka, and the recorder tool for load tests.

Planned stages, not done yet:

- Stage 2: `normalize` and `conv`. Raw source events become channel-agnostic events without duplicates, and conversations with live state in Redis.
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

  raw.bluesky  ---->  record  ---->  compressed segment files outside the repository
                        +-- :9465  /metrics  /healthz  /debug/pprof/
```

`ingest` reads the Bluesky Jetstream through the official SDK, which handles reconnects and failover between two hosts. For every batch it writes one Kafka record per event to `raw.bluesky` (`acks=all`, idempotent, zstd) and, only after Kafka has acknowledged all of them, the batch position to the compacted topic `ingest.cursor`. On start it reads that position and resumes from it, so a restart repeats a few seconds of events and never skips any. Delivery is at-least-once: duplicates are removed later in `normalize`. If Kafka is down for longer than the delivery timeout, ingest exits with an error and the container restart policy starts it again. `record` is a separate tool that copies the raw topic into files for load tests. The reasons for these choices are in [docs/decisions/](docs/decisions/).

## Run it locally

Prerequisites: Docker with Compose v2, GNU Make, and Go 1.27.4 for the tests (`GOTOOLCHAIN=go1.27.4` is set by the Makefile).

```sh
make up        # builds the images, starts Kafka and ingest
make logs      # follows the logs (JSON lines)
```

Ingest writes to `raw.bluesky` a few seconds after the start. Every published port listens on `127.0.0.1` only:

| Port | What |
|---|---|
| 9464 | ingest: `/metrics`, `/healthz`, `/debug/pprof/` |
| 9465 | record (only with the `record` profile): the same endpoints |
| 9094 | Kafka for tools on the host, for example `kcat -b localhost:9094 -L` |

Useful metrics on `http://127.0.0.1:9464/metrics`:

- `threadroute_source_events_total`: events received, by kind
- `threadroute_source_lag_seconds`: now minus the witnessed time of the last batch
- `threadroute_bus_produced_records_total`: records acknowledged by Kafka, by topic
- `process_resident_memory_bytes`: memory of the ingest process

To start the recorder too (it writes to `$HOME/threadroute-recordings` on the host):

```sh
docker compose -f deploy/compose.yaml --profile record up -d --build
```

On Linux, create the recordings directory first and make it writable for the container user (uid 65532).

Stop the stack with `make down`. To also remove the Kafka data volume:

```sh
docker compose -f deploy/compose.yaml --profile record down -v
```

Other targets: `make build`, `make test` (unit tests with the race detector, no Docker), `make test-integration` (a real Kafka broker through Testcontainers, needs Docker), `make lint`, `make vuln`.

## Configuration

Both commands read `THREADROUTE_*` environment variables. An unset or empty variable takes its default. `deploy/compose.yaml` sets the values for the containers and reads overrides from a `.env` file (`make up` passes the root `.env` if it exists). See [.env.example](.env.example). No secrets exist in stage 1.

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

## Data and privacy

The source data is public posts written by real people. The raw topic keeps them for about 24 hours, and recordings for load tests are private files outside this repository that are deleted after 14 days. Logs never contain post text, and tests, examples and documentation use only synthetic data. The details, including how deletions are handled, are in [decision 0004](docs/decisions/0004-data-handling-and-privacy.md).

## Decisions

Architecture decisions are recorded in [docs/decisions/](docs/decisions/). A decision is written and agreed before the code that depends on it.

## Results

| Measurement | Value | Conditions |
|---|---|---|
| Live post events per second, average | | |
| Live post events per second, peak | | |
| Raw topic size per day | | |
| Ingest memory | | |

This table is filled only with real measurements, together with the conditions they were taken under.

## License

MIT, see [LICENSE](LICENSE).
