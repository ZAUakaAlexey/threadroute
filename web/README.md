# threadroute console

The browser console of threadroute (decisions [0011](../docs/decisions/0011-gateway.md), [0012](../docs/decisions/0012-console-access.md) and [0013](../docs/decisions/0013-console-screens.md)): three screens on top of the `gateway` service.

- **Overview** (`#/`): totals only, from the public HTTP snapshot, asked for every two seconds. No keys and no text.
- **Operator** (`#/operator`): take a seat as a demo operator next to the simulated ones. Offers with a countdown, the conversations you hold, their messages, "mark replied" and "resolve". Nothing is posted to Bluesky.
- **Supervisor** (`#/supervisor`): flow totals, queues by language, operators, teams, and all open conversations with filters.

The console speaks the protocol in [docs/gateway-protocol.md](../docs/gateway-protocol.md). The tests read the protocol examples in `internal/gwapi/testdata/` directly, so the Go side and this side check the same files.

## Run it

Node 24 and pnpm 10 (the version is pinned in `package.json`).

```sh
pnpm install
pnpm dev            # fake gateway on :9472 and the app on http://127.0.0.1:5174/
```

`pnpm dev` starts the fake gateway and the Vite dev server together. Vite forwards `/v1` (WebSocket and HTTP) to the fake gateway. Ports: `FAKE_GATEWAY_PORT` (default 9472) and `WEB_PORT` (default 5174).

| Command | What |
|---|---|
| `pnpm dev` | fake gateway and dev server |
| `pnpm fake-gateway` | the fake gateway alone |
| `pnpm typecheck` | TypeScript, strict |
| `pnpm lint` | ESLint with the TypeScript, React hooks and jsx-a11y rules, no warnings allowed |
| `pnpm test` | unit and component tests (Vitest, Testing Library) |
| `pnpm build` | production build into `dist/` |
| `pnpm e2e` | Playwright end-to-end and accessibility tests against the fake gateway (builds first) |
| `pnpm e2e:install` | installs the Chromium of Playwright |

## Configuration

All settings are build-time variables. URLs may be relative: they are resolved against the page, so the built files work under any path of another site.

| Variable | Default | What |
|---|---|---|
| `CONSOLE_BASE` | `./` | Vite `base`: the path the files are served under, for example `/demos/threadroute/`. The default makes every asset URL relative. |
| `VITE_GATEWAY_WS_URL` | `v1/ws` | the WebSocket of the gateway. `http(s)` becomes `ws(s)`. |
| `VITE_PUBLIC_SNAPSHOT_URL` | `v1/public/snapshot` | the public snapshot |
| `VITE_SITE_TOKEN_KEY` | empty | the `localStorage` key under which the site keeps its sign-in token. Empty: the console never sends a token and every user gets the demo level. |
| `VITE_PRIVACY_URL` | decision 0004 on GitHub | the "How data is handled" link of the public page |
| `VITE_SOURCE_URL` | the repository on GitHub | the "Source code" link |

Example for a site that serves the console under `/demos/threadroute/` and forwards the gateway paths at its root:

```sh
CONSOLE_BASE=/demos/threadroute/ VITE_GATEWAY_WS_URL=/v1/ws VITE_PUBLIC_SNAPSHOT_URL=/v1/public/snapshot pnpm build
```

Screens use hash routes (`#/operator`), so the web server needs no rewrite rules.

## Layout

```
src/protocol/   types, run-time parser, message builders, close codes, WebSocket client (no React)
src/live/       Zustand store of the live view, session glue, shared clock tick, timers, hooks
src/screens/    operator, supervisor and public screens
src/ui/         shared components (panel, tables, tiles, message list, icons)
fake-gateway/   Node fake of the gateway for development and tests (not in the build)
e2e/            Playwright tests, with axe checks of every screen in light and dark
```

## How it works

- **Protocol.** Every server frame goes through `parseServerMessage`, which checks each field and keeps only the known ones. A frame that fails closes the connection and reconnects, so the store never holds a broken view. Unknown message types are skipped. At the demo level a frame with post text is refused.
- **Connection.** `GatewayClient` sends `hello` first, checks the version, follows the close codes (the reconnect rule of `CloseCode.Reconnect` in the Go code; 1005 and 1006 from the browser count as 1001; 4401 stops and the next connection goes without the site token), reconnects with backoff from 1 to 30 seconds with jitter, and sends the resume token from the last `welcome`. Each request has its own ID and gets exactly one result. A "reply" that was in flight during a reconnect is sent again with the same request ID.
- **Store.** One Zustand store per connection. A `state` snapshot replaces the view; an update sets and removes rows by key. Watched messages live only in memory and are dropped when a conversation leaves the view, on every snapshot, and on a lost connection. A large first state may come in several frames; the later ones are plain updates. The shown conversation stays selected over a snapshot and is watched again; it goes only when it leaves the view or the gateway answers `unknown_conversation`. The gateway may end a watch by itself, so an `unwatch` answered `already_done` is normal. `message_removed` replaces the message with a copy without text at once.
- **Timers.** One shared clock tick for the whole page (every 250 ms, only while a timer is shown). Timers use the server clock offset from `welcome` and `state`, and show model time (machine time times `speed_factor`). While the data is stale or the connection is lost, they stop.
- **Storage.** `localStorage` holds the chosen languages only, `sessionStorage` holds the resume token only. A test checks that no text reaches either.
- **Public page.** TanStack Query asks for the snapshot every two seconds. The page shows only what the parser knows, so a new field on the server cannot leak a key onto the page.

## Fake gateway

`fake-gateway/` serves the protocol over a synthetic world: 24 simulated operators, two dedicated teams, six language queues, and conversations between invented accounts (`did:plc:synthcompany0001` and so on) with invented sample text. It is deterministic for a seed (`FAKE_SEED`). Every frame it sends goes through the console's own parser first, and demo frames are checked for pseudonyms only, so the fake cannot drift from the protocol without a test failing.

- Scenario mode (default): conversations arrive, simulated operators accept and resolve, a human operator gets an offer every few seconds, and a cycle of two minutes plays a new message, a removed message, an operator going offline, and a stale spell.
- Manual mode (`FAKE_MANUAL=1`, used by the end-to-end tests): nothing happens until the control API asks: `POST /__control/{reset,offer,customer-message,remove-message,stale,operator-offline,rekey,snapshot,close}`. `snapshot` sends every connection a new snapshot, as the gateway does when it rebuilt its view.
- Levels: the token `synthetic.full.token` gets the full level, any other or none gets the demo level. Like the real gateway of version 1, the fake never closes with 4401 or 4504.
- `FAKE_SNAPSHOT_FRAME_ROWS` (default 500) is the most conversation rows in one frame of a supervisor snapshot. A larger snapshot comes in several frames: the first has `snapshot: true`, the rest are updates that add rows. The end-to-end tests use 3.
- Watches follow the real gateway: a repeated `watch` gets `already_done` and a new messages snapshot, an `unwatch` of a conversation that is not watched gets `already_done`, and a watch ends without a message when the operator stops holding the conversation. Only `accept`, `refuse`, `reply` and `resolve` fail with `stale_connection` while the data is stale, with the gateway's texts.
- `FAKE_SPEED_FACTOR` sets the speed factor. The end-to-end tests use 5, so an offer expires after 4 seconds.

## CI

`.github/workflows/web.yml` runs on changes under `web/`, `internal/gwapi/testdata/` and `docs/gateway-protocol.md`: install with the frozen lockfile, type check, lint, unit tests, build, and the Playwright tests with a cached browser.
