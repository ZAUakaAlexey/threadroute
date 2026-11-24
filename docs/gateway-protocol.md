# Gateway protocol, version 1

This document describes how the console in `web/` talks to the `gateway` service. It follows decisions [0011](decisions/0011-gateway.md), [0012](decisions/0012-console-access.md) and [0013](decisions/0013-console-screens.md). The Go types are in `internal/gwapi`, and the examples in `internal/gwapi/testdata/` are checked by the Go tests. The console tests should load the same files, so that both sides stay in line.

If this document and the Go code disagree, the Go code and its tests win, and this document has a bug.

## Overview

| Path | Method | What |
|---|---|---|
| `/v1/ws` | GET, WebSocket upgrade | the live connection of the operator and supervisor screens |
| `/v1/public/snapshot` | GET | the JSON of the public page |

Default ports of the gateway: `:9472` for both paths, `:9471` for `/metrics`, `/healthz` and `/debug/pprof/` (like every other service). In production the site's web server forwards the two paths.

Nothing secret is ever put into a URL. The site token goes into the first WebSocket message.

## Common rules

- Every WebSocket message is one text frame with one JSON object. The field `type` names the message.
- Unknown fields are ignored by both sides, so fields can be added without a new protocol version.
- A server message with an unknown `type` is skipped by the client. A client message with an unknown `type` closes the connection (`4400`).
- Times are integers: unix microseconds in fields that end in `_us`. They are below 2^53, so JavaScript numbers hold them exactly.
- Fields that end in `_at_us` or `_due_us` are times on the machine clock of the server. Fields that end in `_source_us` or are named `source_us` are source times of posts. Other `_us` fields are durations in **model time**: machine time multiplied by `speed_factor`. The SLA targets are model time too.
- Languages are 2 or 3 lower case letters. `und` is the general queue.
- A request ID is chosen by the client: 1 to 64 characters from `A-Z a-z 0-9 . _ -`.
- Errors never contain post text or a token.

## Access levels

| Level | Who | What changes on the wire |
|---|---|---|
| public | anyone, no WebSocket | only `/v1/public/snapshot` |
| `demo` | anyone, with or without sign-in | no post text; every conversation ID, company, customer and message ID is a pseudonym |
| `full` | users the owner allowed in the site | text and real IDs |

The server decides the level from the token it checked with the site. The client never asks for a level.

**Pseudonyms** look like `ps-` followed by 12 characters from `a-z` and `2-7`, for example `ps-k2m3n4p5q6r7`. The same ID gives the same pseudonym everywhere on one day, so a company in a team row and in a conversation row has the same label. The key behind them changes every day; on that change the server sends a new state snapshot (see below).

**Operator IDs** are synthetic and are shown at every level. A human operator's ID starts with `web-`. A simulated operator's ID starts with `sim-op-`.

## Connection life cycle

```
client                                 server
  |  open WebSocket /v1/ws               |
  |  hello  ---------------------------> |   within 5 seconds
  |  <---------------------------  welcome
  |  <----------------------------  status
  |  <--------------  state (snapshot: true, seq 1)
  |  <--------------  state (snapshot: false), at most every 250 ms
  |  action / watch / unwatch  --------> |
  |  <------------------  result (same request_id)
  |  <------------------  messages / message_removed (watched conversations)
```

1. The client opens the WebSocket and sends `hello` as its first message.
2. If no valid `hello` arrives within **5 seconds**, the server closes with `4408`.
3. The server checks the token (if any) with the site, decides the level, and for the operator view creates an operator. Then it sends `welcome`, `status`, and a `state` snapshot.
4. From then on the server sends `state` updates, `status` changes, and the answers to requests.
5. A second `hello` on the same connection closes it with `4400`.

### Reconnecting

After a close, look at the close code (table below). For codes marked "reconnect", wait with a backoff (for example 1, 2, 4, 8 seconds, up to 30 seconds, with some random jitter) and open a new connection. A new connection always starts with a full snapshot, so nothing needs to be merged.

An operator whose connection closes stays online for **15 seconds** on the server, so a page reload does not lose their conversations. To get the same operator back, send the `resume_token` from the last `welcome` in the new `hello`. Keep the resume token in `sessionStorage` (per browser tab). It is not secret text, and it is the only thing besides settings that the console may store.

## Close codes

The close reason is always the name in the second column, never anything else.

| Code | Name (close reason) | Meaning | Reconnect | Client reaction |
|---|---|---|---|---|
| 1000 | `normal` | closed on purpose | no | do not reconnect |
| 1001 | `going_away` | the gateway restarts or shuts down | yes | reconnect with backoff |
| 1009 | `message_too_big` | a client message was over 4096 bytes | no | bug in the client, do not reconnect at once |
| 1011 | `internal` | unexpected server error | yes | reconnect with backoff |
| 1013 | `try_again_later` | the gateway cannot take the connection now | yes | reconnect with backoff |
| 4400 | `bad_message` | invalid JSON, unknown type, invalid fields, or the first message was not `hello` | no | bug in the client, do not reconnect at once |
| 4401 | `unauthorized` | the site rejected the token | no | drop the token; the user may reconnect without it (demo level) |
| 4408 | `hello_timeout` | no valid `hello` within 5 seconds | no | bug in the client, do not reconnect at once |
| 4409 | `replaced` | a newer connection took over this operator (another tab, or a reload) | no | tell the user the session moved |
| 4410 | `slow_client` | the client did not read its messages fast enough | yes | reconnect with backoff |
| 4426 | `unsupported_version` | `hello.version` is not the server's version | no | reload the page |
| 4429 | `too_many_connections` | this address holds too many connections | yes | reconnect with backoff |
| 4503 | `no_operator_seat` | the demo operator seats are taken (5 by default) | yes | reconnect with backoff, or open the supervisor view |
| 4504 | `auth_unavailable` | the site API did not answer, so the token could not be checked | yes | reconnect with backoff, or without the token |

The "Reconnect" column is `CloseCode.Reconnect` in the Go code.

A browser also reports two codes that the server never sends: `1005` (a close frame without a code) and `1006` (the connection dropped without a close frame, for example a network loss). The client treats both like `1001` and reconnects with backoff.

The gateway of version 1 never sends `4401` and `4504`: when the site API rejects a token, gives a broken answer, or does not answer in time, the connection goes on at the `demo` level (decision 0012, "the default for an unknown user is no text"). The codes stay defined, so a later gateway can use them without a new version. A client should still handle them as the table says.

## Versioning

There is one number, `version`, now `1`. The client sends it in `hello`; the server repeats its own in `welcome` and in the public snapshot.

- Adding a field or a message type does not change the version. Both sides ignore what they do not know.
- Removing or changing the meaning of a field, or a new rule that an old client would break, raises the version.
- A `hello` with another version (or without one) is answered with close code `4426` and no `welcome`. The close reason is `unsupported_version`, like every close reason. The console should then ask the user to reload the page, because a newer console was deployed.
- **Enum values.** Fields with a fixed set of values (`state`, `kind`, `presence`, `level`, `view`, `outcome`, `code`, `reason`, `direction`) only ever hold the values this document lists. A new value needs a new version. The console treats an unknown value as a broken frame: it closes the connection and reconnects. So the server never sends a value the version does not define.

## Client messages

### `hello`

```json
{"type": "hello", "version": 1, "view": "operator", "languages": ["en", "und"]}
```

| Field | Type | Rule |
|---|---|---|
| `version` | integer | must be `1` |
| `view` | `"operator"` or `"supervisor"` | the screen of this connection; it does not change later |
| `token` | string, optional | the site token of a signed-in user, at most 2048 bytes |
| `resume_token` | string, optional | from an earlier `welcome`; operator view only |
| `languages` | list of languages | operator view only, and required there: 1 to 32 values, no duplicates, any order |

For the operator view the gateway creates an operator with these languages and a capacity of 2. The new operator is **offline**; send the `online` action to start getting offers.

A browser that wants both screens opens two connections.

### `watch` and `unwatch`

```json
{"type": "watch", "request_id": "req-0001", "conversation_id": "ps-k2m3n4p5q6r7"}
{"type": "unwatch", "request_id": "req-0002", "conversation_id": "ps-k2m3n4p5q6r7"}
```

`watch` asks for the messages of one conversation. `conversation_id` is the `id` of a conversation as the server sent it.

- An operator connection may watch the conversations it holds: rows of `my_conversations` with state `assigned` (decision 0011, point 6). An offer shows no messages.
- A supervisor connection may watch any conversation in its `conversations`.
- At most **4** watched conversations per connection.
- Every `watch` and `unwatch` gets one `result`. On success, a `messages` snapshot follows at once. A `watch` of a conversation the connection already watches is answered `already_done` and also gets a new `messages` snapshot.
- A state snapshot ends every watch. Send `watch` again for a conversation you still show, using its `id` from the new snapshot.
- The server ends a watch by itself, without a message, when the connection may no longer watch the conversation: on an operator connection when the operator stops holding it (resolved, returned to the queue, conversation closed), on a supervisor connection when the gateway forgets the conversation. No more `messages` or `message_removed` arrive for it. Drop its messages when the row leaves `my_conversations` or `conversations`.
- An `unwatch` for a conversation that is not watched (never, or no longer, for example after the server ended the watch) is answered `already_done`, not with an error.

### `action`

```json
{"type": "action", "request_id": "req-accept", "action": "accept", "offer_id": "of-101"}
```

| `action` | Extra field | What it does |
|---|---|---|
| `online` | none | the operator gets offers |
| `offline` | none | the operator's offers end and their conversations return to the queue |
| `set_languages` | `languages` (as in `hello`) | replaces the operator's languages |
| `accept` | `offer_id` | accepts an offer |
| `refuse` | `offer_id` | refuses an offer |
| `reply` | `offer_id` | "mark replied": records a reply marker. Nothing is posted anywhere |
| `resolve` | `offer_id` | resolves the conversation and frees the slot |

Actions are allowed only on an operator connection. They act on the operator the server created for the connection; the client never names an operator.

For `reply` the request ID is also the reply ID. If the answer was lost, send the same request again with the **same** `request_id`: the server answers `already_done` and records no second reply. For other actions a repeat is also safe (`already_done`), but a new request ID is fine.

The `result` only says whether the action was applied. The new state (for example the conversation moving from `offered` to `assigned`) arrives in a later `state` message.

### Rate limit

Requests (`action`, `watch`, `unwatch`) of one connection are limited by a token bucket: 10 at once, refilled with 5 per second. A request over the limit gets a `result` with code `rate_limited`; the connection stays open.

## Server messages

### `welcome`

```json
{
  "type": "welcome", "version": 1, "level": "demo", "view": "operator",
  "operator_id": "web-test0001", "resume_token": "rt_test_0000000000000001",
  "server_time_us": 1767323100000001, "speed_factor": 1,
  "accept_timeout_us": 20000000, "assignment_target_us": 60000000, "first_response_target_us": 120000000,
  "tick_ms": 250,
  "limits": {"max_message_bytes": 4096, "actions_per_second": 5, "action_burst": 10, "max_watched": 4}
}
```

| Field | Meaning |
|---|---|
| `level` | `demo` or `full` |
| `view` | the view of the `hello` |
| `operator_id` | operator view only: the operator the server created. Show it; never send it |
| `resume_token` | operator view only: keep it for a reconnect of this tab |
| `server_time_us` | the server clock. Keep `offset = server_time_us - Date.now() * 1000` and use it for every timer |
| `speed_factor` | model time = machine time * `speed_factor`. 1 for live traffic, for example 50 in a load test |
| `accept_timeout_us`, `assignment_target_us`, `first_response_target_us` | model time |
| `tick_ms` | the shortest time between two `state` messages |
| `limits` | the limits of this connection |

### `status`

```json
{"type": "status", "stale": true, "reason": "source_unavailable", "lag_us": 12500000}
{"type": "status", "stale": false}
```

Sent right after `welcome` and again on every change. While `stale` is true, the screens show that the data is stale and stop the timers (decision 0013). `state` messages may still arrive.

| `reason` | Meaning |
|---|---|
| `catching_up` | the gateway started and is still reading the topics |
| `source_unavailable` | the gateway cannot read the topics |
| `lagging` | the gateway is behind the topics by more than its limit |

`lag_us` is optional with every reason: how long the gateway has been behind the end of the topics, in machine time, when it knows it. It is always set with `lagging`. With `source_unavailable` it says how long ago the gateway last saw the end of the topics.

Losing the WebSocket connection also means "stale" for the screens, without a message.

### `state`

The server keeps, for each connection, the set of keys that changed. At most every 250 ms it sends one `state` message with the **current state** of those keys. It never sends a list of steps, so a slow client simply gets fewer and larger updates.

```json
{
  "type": "state", "seq": 2, "snapshot": false, "server_time_us": 1767323100250001,
  "me": {"id": "web-test0001", "kind": "human", "languages": ["en", "und"], "capacity": 2, "presence": "online", "load": 1, "offered": 0},
  "my_conversations": {"set": [{"id": "ps-k2m3n4p5q6r7", "...": "..."}], "remove": ["ps-c2d3e4f5g6h7"]}
}
```

| Field | Meaning |
|---|---|
| `seq` | counts the `state` messages of the connection from 1 |
| `snapshot` | `true`: drop the whole view and build it again from this message |
| `server_time_us` | the server clock; the client may correct its offset with it |
| collections | see below |

**Collections.** Each collection is an object with two optional lists:

- `set`: objects to add or to replace, matched by their `id`.
- `remove`: IDs to drop.

A collection that is absent did not change. In a snapshot a collection that is absent is empty, and `remove` is never used.

**A snapshot in several frames.** A large first state (thousands of conversations on the supervisor view) may come in several `state` frames sent back to back: the first one has `snapshot: true` and every collection except part of `conversations` (or `my_conversations`), and the next ones have `snapshot: false` and only `set` more rows of that collection, each with the next `seq` (decision 0011). The client needs no extra logic: it applies them like any update.

**First snapshot and later updates.** The first `state` of a connection always has `seq: 1` and `snapshot: true`. The server sends another snapshot when it rebuilt its own view, and at the demo level when the pseudonyms change (once a day). The client needs no other logic to tell them apart: `snapshot: true` means "replace", `false` means "apply".

What each view gets:

| Field | Operator view | Supervisor view | Object |
|---|---|---|---|
| `me` | yes (always in a snapshot) | no | operator row, a single object |
| `my_conversations` | yes | no | operator conversation |
| `conversations` | no | yes | conversation row |
| `operators` | no | yes | operator row |
| `queues` | no | yes | queue row |
| `teams` | no | yes | team row |
| `totals` | no | yes (always in a snapshot) | totals, a single object |

#### Operator conversation (`my_conversations`)

An offer or an assignment of this connection's operator. It is removed when the offer or assignment ends (refused, expired, resolved, returned to the queue, or the conversation closed).

| Field | Type | Meaning |
|---|---|---|
| `id` | string | conversation ID: the conversation key at full level, a pseudonym at demo level |
| `company`, `customer` | string | IDs at full level, pseudonyms at demo level |
| `lang` | language | the queue language |
| `session` | integer | session number |
| `state` | `"offered"` or `"assigned"` | |
| `offer_id` | string | the offer, and after accept the assignment. Actions take it |
| `accept_by_us` | time | offered only: the accept deadline (countdown) |
| `accepted_at_us` | time | assigned only |
| `queued_at_us` | time | the waiting time runs from here |
| `first_response_due_us` | time, optional | when the "time to first response" target is missed; absent when met or missed already |
| `first_response_missed` | boolean | |
| `replied` | boolean | the operator marked a reply in this session |
| `priority` | integer | 0 or higher |
| `customer_messages`, `company_messages` | integer | messages of the session; removed messages still count |
| `last_message_source_us` | source time, optional | the newest message |

#### Conversation row (`conversations`)

Every open conversation the gateway knows. A closed one is removed.

| Field | Type | Meaning |
|---|---|---|
| `id`, `company`, `customer`, `lang`, `session` | | as above |
| `state` | `open`, `waiting`, `offered`, `assigned`, `resolved` | `open`: not in a queue yet |
| `team_queue` | boolean | waits in the team queue of its company |
| `priority` | integer | |
| `operator_id` | string, optional | for `offered` and `assigned` |
| `queued_at_us` | time, optional | for `waiting`, `offered` and `assigned` |
| `assignment_missed`, `first_response_missed` | boolean | missed SLA targets of the session |
| `customer_messages`, `company_messages` | integer | |
| `opened_source_us` | source time | |

#### Operator row (`operators` and `me`)

| Field | Type | Meaning |
|---|---|---|
| `id` | string | operator ID |
| `kind` | `"human"` or `"simulated"` | |
| `languages` | list, sorted | |
| `capacity` | integer | |
| `presence` | `"online"` or `"offline"` | |
| `load` | integer | live offers and assignments; can be above `capacity` after the capacity was lowered |
| `offered` | integer | the part of `load` that waits for an accept |
| `team` | string, optional | company of the dedicated team: ID at full level, pseudonym at demo level |

#### Queue row (`queues`)

One row per language. `id` is the language. Shared and team queues of the language are counted together.

| Field | Meaning |
|---|---|
| `waiting` | conversations in the queue without an offer |
| `team_waiting` | the part of `waiting` in team queues |
| `offered` | conversations with an offer that waits for an accept |
| `oldest_queued_at_us` | queue time of the one that has waited longest; absent when `waiting + offered` is 0 |
| `missed_assignment` | of `waiting + offered`, those that missed the "time to assignment" target |

#### Team row (`teams`)

`id` is the company: its ID at full level, its pseudonym at demo level. A released team is removed.

| Field | Meaning |
|---|---|
| `members` | operator IDs |
| `active_load` | conversations of the company that wait or are held |
| `formed_at_us` | when the team formed |

#### Totals (`totals`)

Also used by the public snapshot.

| Field | Meaning |
|---|---|
| `messages_per_second` | messages added to conversations, over the last 10 seconds |
| `conversations` | open conversations by state: `open`, `waiting`, `offered`, `assigned`, `resolved` |
| `companies_active` | companies with at least one open conversation |
| `operators_online`, `humans_online` | online operators, and the human ones among them |
| `slots_capacity`, `slots_used` | sum of capacity and of load of online operators |
| `missed_assignment`, `missed_first_response` | missed SLA targets in the last hour (machine time) |
| `wait_p50_us`, `wait_p90_us` | median and 90th percentile of the waiting time until accept, last hour, model time; 0 when there was no accept |

### `messages`

```json
{
  "type": "messages", "conversation_id": "ps-k2m3n4p5q6r7", "snapshot": true,
  "messages": [
    {"id": "ps-msgaaaa22222", "direction": "customer", "source_us": 1767323045123456, "length": 31, "edited": false}
  ]
}
```

The messages of a watched conversation.

- `snapshot: true`: the whole list, ordered by `source_us`. Replace what you hold for this conversation. A late message can sort before messages you already show, so never only append.
- `snapshot: false`: one or more messages to add or replace by `id` (a new message, or an edit). Sort them in by `source_us`.
- `messages` is never `null`. A conversation whose text is gone gives `[]`.

| Field | Type | Meaning |
|---|---|---|
| `id` | string | message ID at full level, pseudonym at demo level |
| `direction` | `"customer"` or `"company"` | the author is the conversation's customer or company |
| `source_us` | source time | |
| `length` | integer | length of the text in Unicode code points (`Array.from(text).length`) |
| `text` | string | **full level only**, always present there (it can be `""`) |
| `edited` | boolean | the author edited the post |

At the demo level the rows are the same, without `text`: show a placeholder with the length and the time.

Text lives only in memory. Never put it into `localStorage`, `sessionStorage`, IndexedDB, logs or error reports (decision 0013).

### `message_removed`

```json
{"type": "message_removed", "conversation_id": "ps-k2m3n4p5q6r7", "message_id": "ps-msgaaaa22222", "reason": "author_removed"}
```

The author deleted the post (`deleted`) or was removed (`author_removed`). Drop the text from memory at once and show a placeholder ("message removed"). The message will not be in a later `messages` snapshot.

### `result`

```json
{"type": "result", "request_id": "req-accept", "outcome": "done"}
{"type": "result", "request_id": "req-accept", "outcome": "rejected", "code": "offer_expired", "message": "accept: the offer is expired"}
```

Every request gets exactly one `result` with its `request_id`. Results can arrive in another order than the requests.

| `outcome` | Meaning |
|---|---|
| `done` | the request changed the state |
| `already_done` | the same request was applied before; nothing changed |
| `rejected` | not applied; `code` says why, `message` is for people |

Codes from the router's operator API, passed on as they are:

| Code | Meaning |
|---|---|
| `invalid_request` | a field is not valid |
| `operator_not_found` | the router does not know the operator |
| `offer_not_found` | the router does not know the offer for this operator |
| `offer_expired` | the accept timeout passed |
| `offer_refused` | the operator refused this offer before |
| `offer_accepted` | refuse after accept |
| `offer_ended` | the offer ended (conversation closed, operator offline) |
| `offer_not_accepted` | reply or resolve before accept |
| `assignment_ended` | the assignment ended before the call |
| `unavailable` | the router cannot answer now; retry later |
| `internal` | unexpected error |

Codes of the gateway:

| Code | Meaning |
|---|---|
| `not_allowed` | not allowed for this view or level, for example an action on a supervisor connection |
| `rate_limited` | over the request limit; retry later |
| `unknown_offer` | the offer is not a live offer or assignment of this operator in the gateway's view |
| `stale_connection` | see below |
| `unknown_conversation` | `watch` for a conversation that is not in this view, or that this connection may not watch |
| `too_many_watched` | `watch` with 4 conversations already watched |

Only `unavailable` and `rate_limited` mean "the same request may work later". The others need a change first (for example a new offer).

`stale_connection` has two causes, and the `message` names the one that applies:

- **The connection lost its operator** (`message`: "the operator moved to another connection"). Another connection took the operator over with the resume token; this connection is closed with `4409` right after. Nothing to do on this connection.
- **The gateway's data is stale** (`message`: "the gateway data is stale"). The gateway is catching up or cannot read the topics (`status.stale` is true), so it cannot check an offer action (`accept`, `refuse`, `reply`, `resolve`) against its view. The connection stays open. Wait for `status` with `stale: false` and send the request again, or reconnect. `online`, `offline` and `set_languages` are not checked against the view and work while the data is stale.

## Timers in the browser

The server sends deadlines and start times, never a message per second (decision 0013).

- `server_now_us = Date.now() * 1000 + offset`, with `offset` from `welcome.server_time_us` (and optionally updated from `state.server_time_us`).
- Accept countdown: `(accept_by_us - server_now_us)`, in machine time. Show it as model time by multiplying with `speed_factor`, like every other duration on the screens.
- Waiting time: `(server_now_us - queued_at_us) * speed_factor`.
- Time left to the first response target: `(first_response_due_us - server_now_us) * speed_factor`.
- While `status.stale` is true, or the connection is lost, stop the timers.

## Public snapshot

`GET /v1/public/snapshot` returns one JSON object. The gateway rebuilds it every second; the web server may cache it for one second; the page asks for it every two seconds.

```json
{
  "version": 1, "generated_at_us": 1767323100000001, "stale": false, "speed_factor": 1,
  "totals": {"...": "the totals object above"},
  "queues": [{"lang": "en", "waiting": 3, "offered": 2, "oldest_wait_us": 61000000, "missed_assignment": 1}]
}
```

It holds totals only: no conversation keys, no company or customer IDs or pseudonyms, no operator IDs, no text. `queues` is sorted by language and never `null`. `oldest_wait_us` is a duration in model time at `generated_at_us`, because the answer is cached.

A Go test compares the full field set of this object with an allow-list, so a field that could hold a key cannot be added by mistake.

## Limits

| Limit | Value |
|---|---|
| first message (`hello`) | within 5 seconds |
| client message size | 4096 bytes |
| site token | 2048 bytes |
| resume token | 256 characters |
| request ID | 1 to 64 characters `A-Z a-z 0-9 . _ -` |
| requests per connection | 10 at once, 5 per second |
| watched conversations per connection | 4 |
| languages of an operator | 32 |
| `state` messages | at most one per 250 ms |
| demo operators at the same time | 5 (then close code `4503`) |
| capacity of a human operator | 2 |
| operator stays online after a close | 15 seconds |

## Examples

Every file is listed with its purpose, level and view in `internal/gwapi/testdata/index.json`. The Go tests decode each file, encode it again and compare. Names start with the direction: `client_`, `server_` or `public_`.

| File | Message | Level, view | Purpose |
|---|---|---|---|
| `client_hello_operator_anonymous.json` | `hello` | | operator screen without sign-in |
| `client_hello_operator_resume.json` | `hello` | | operator screen after a reload, signed in, with a resume token |
| `client_hello_supervisor_signed_in.json` | `hello` | | supervisor screen with a site token |
| `client_watch.json` | `watch` | | start watching a conversation |
| `client_unwatch.json` | `unwatch` | | stop watching |
| `client_action_online.json` | `action` | | go online |
| `client_action_offline.json` | `action` | | go offline |
| `client_action_set_languages.json` | `action` | | set languages |
| `client_action_accept.json` | `action` | | accept an offer |
| `client_action_refuse.json` | `action` | | refuse an offer |
| `client_action_reply.json` | `action` | | mark replied |
| `client_action_resolve.json` | `action` | | resolve |
| `server_welcome_operator_demo.json` | `welcome` | demo, operator | answer to an anonymous operator |
| `server_welcome_supervisor_full.json` | `welcome` | full, supervisor | answer to an allowed user, load test speed 50 |
| `server_status_fresh.json` | `status` | demo, operator | data is current |
| `server_status_stale.json` | `status` | demo, supervisor | topics cannot be read |
| `server_state_operator_snapshot_demo.json` | `state` | demo, operator | first state: the operator, an assignment and an offer |
| `server_state_operator_update_demo.json` | `state` | demo, operator | a reply marked, the offer gone |
| `server_state_supervisor_snapshot_full.json` | `state` | full, supervisor | first state with every collection |
| `server_state_supervisor_update_demo.json` | `state` | demo, supervisor | an update with set and remove |
| `server_messages_snapshot_full.json` | `messages` | full, operator | messages with text, one of them edited |
| `server_messages_snapshot_demo.json` | `messages` | demo, operator | the same rows without text |
| `server_messages_snapshot_empty.json` | `messages` | demo, supervisor | a conversation whose text is gone |
| `server_messages_update_full.json` | `messages` | full, operator | one new message |
| `server_message_removed_full.json` | `message_removed` | full, operator | the author deleted a post |
| `server_message_removed_demo.json` | `message_removed` | demo, supervisor | the author was removed |
| `server_result_done.json` | `result` | demo, operator | done |
| `server_result_already_done.json` | `result` | demo, operator | already done |
| `server_result_rejected_router.json` | `result` | demo, operator | rejected by the router |
| `server_result_rejected_gateway.json` | `result` | demo, operator | rejected by the gateway (rate limit) |
| `public_snapshot.json` | public snapshot | | answer of `/v1/public/snapshot` |

All data in the examples is synthetic.
