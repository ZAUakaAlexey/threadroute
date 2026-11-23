# 0002. Official Bluesky Jetstream SDK behind a connector interface

Status: Accepted
Date: 2026-11-23

## Context

Bluesky publishes its network activity through Jetstream v2, a WebSocket service with JSON events, optional zstd compression with a shared dictionary, a resume cursor, and two public hosts. Version 2 was released in August 2026 and is still changing.

There is an official Go client, `github.com/bluesky-social/jetstream` (v0.4.6 at the time of writing, needs Go 1.26 or newer).

## Options

1. **Official SDK.** It already handles the subscription, zstd with the dictionary, reconnects, resume by cursor, deduplication of the overlap after a resume, and failover between hosts. Events arrive in batches through an iterator.
2. **Own WebSocket client.** Full control over the read loop and frame-level metrics, and a smaller dependency set. It repeats work the SDK already does and must follow every protocol change.

## What was checked

- The SDK API covers what ingest needs: kinds and collection filters, `WithFailoverHosts`, `WithCursorMode`, `WithLiveCursor`, batch size and delay, and `Batch.LastCursor`.
- The SDK has no metrics of its own. The connector counts events, bytes and reconnects itself.
- Building the SDK client package pulls in `sentry-go`, parts of `pebble`, `cockroachdb/errors` and `gogo/protobuf`. This makes the binary and the vulnerability-scan surface larger, but none of it runs in the live-tail path.
- Not checked yet: how the SDK behaves when the consumer is blocked for a long time (memory use and recovery).

## Decision

Use the official SDK. Wrap it in a small connector interface owned by this project, so the rest of the code does not import the SDK.

## Consequences

- Much less code to write and maintain for the source side.
- Protocol changes are followed by upgrading one dependency.
- The connector interface is the only place that changes if the SDK is replaced or another channel is added.
- The SDK is before version 1.0, so its API can change between releases. The version is pinned.
- Open check for stage 1: block publishing for one minute and watch memory and recovery. If memory grows without a limit, this decision is reviewed with the numbers.
