# Architecture decisions

Each file records one decision: the context, the options that were considered, the choice and what follows from it. A decision is written and agreed before the code that depends on it.

Status values: `Proposed` (under review), `Accepted`, `Superseded by NNNN`.

| No. | Decision | Status |
|---|---|---|
| [0001](0001-kafka-as-event-log.md) | Apache Kafka as the event log between services | Accepted |
| [0002](0002-official-bluesky-sdk.md) | Official Bluesky Jetstream SDK behind a connector interface | Accepted |
| [0003](0003-failover-cursor-dedup.md) | Two source hosts, time cursor in a compacted topic, deduplication in normalize | Accepted |
| [0004](0004-data-handling-and-privacy.md) | Data handling and privacy | Accepted |
| [0005](0005-code-structure.md) | Code structure and conventions | Accepted |
| [0006](0006-normalize-and-event-model.md) | The normalize service and the channel-agnostic event model | Proposed |
| [0007](0007-conversation-lifecycle.md) | Conversation lifecycle and state | Proposed |
| [0008](0008-company-admission.md) | Which companies are served | Proposed |
