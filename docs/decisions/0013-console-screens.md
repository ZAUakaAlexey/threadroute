# 0013. Console: screens and the public page

Status: Proposed
Date: 2026-11-24

## Context

The console is a web application in `web/` in this repository: Vite, React, TypeScript, TanStack Query, Zustand, Tailwind and TanStack Virtual (agreed in planning). It talks to the gateway (decision 0011) with the access levels of decision 0012.

Decision 0004 says that the public page shows metadata only, and that post text is visible only after sign-in.

## Screens

### Operator

- A card for the current offer with a countdown to the accept deadline, and the buttons accept and refuse.
- A list of the conversations the operator holds: language, waiting time, time to the first response target, number of messages. The list is virtualized.
- A conversation view: the messages in order (text at the full level, a placeholder with length and time at the demo level), the timers, and the buttons "mark replied" and "resolve".
- A language picker and an online switch.

Nothing the operator does is posted to Bluesky. "Mark replied" records a marker only (decision 0010). Draft replies from an LLM come in stage 6.

### Supervisor

- Flow: events per second, open conversations, admitted companies.
- Queues by language: how many wait, how long the oldest has waited, missed targets.
- Operators: online, load against capacity, simulated or human. The table is virtualized.
- Teams: which companies have one and its size. Companies appear under a pseudonym at the demo level.
- A list of conversations with their state, filter by language and state.

### Public page

Totals only: flow, conversations by state, queues by language, operators online and their load, missed targets, typical waiting time. No conversation keys, no company keys, no text.

## Options and recommendations

### 1. How the public page gets its numbers

- **A. A WebSocket per visitor,** like the console.
- **B. A small JSON snapshot over HTTP** that the gateway rebuilds every second. The web server may cache it for a second, and the page asks for it every two seconds.
- **C. Server-sent events.**

Recommendation: **B**. The page shows totals that do not need less than a second of delay, a cached snapshot costs the same for one visitor and for a thousand, and an anonymous visitor holds no open connection. The live WebSocket path is shown in the console, which anyone can open as a demo operator (decision 0012).

### 2. Where the console lives on the site

- **A. A separate static application** built from `web/`, served under a path of the site by the web server, with a card on the site's demo page that links to it.
- **B. Pages inside the site's own application.** That application is in a private repository, so the console code would not be public.

Recommendation: **A**. The full-stack part of this project should be readable in one public repository.

### 3. How the browser keeps live state

- **A. One store for the WebSocket view** (Zustand): the gateway sends states by key, and the store replaces them. TanStack Query is used only for plain HTTP reads (the public snapshot, settings).
- **B. Everything through TanStack Query,** with WebSocket messages written into its cache.

Recommendation: **A**. The two kinds of data have different rules: a live view that the server pushes, and requests that the client makes.

### 4. Components

- **A. Tailwind with unstyled accessible primitives** (Radix) for dialogs, menus and tooltips.
- **B. A ready component kit.**

Recommendation: **A**. A few screens do not need a kit, and the primitives give keyboard and screen reader support that is easy to get wrong by hand.

## Details that follow

- **Timers run in the browser** from the deadline times in the events, corrected by the offset between the server clock and the browser clock that the gateway sends at connect. The gateway does not send a message per second per timer.
- **Stale data is shown as stale.** When the connection is lost or the gateway reports that it is behind, the screens say so and stop the timers.
- **Removed text.** When the gateway reports a removed message (decision 0011), the text is replaced on the screen at once and dropped from the store.
- **No text in the browser's storage.** Live state stays in memory. Only settings such as the chosen languages are stored.
- **Tests.** Unit tests with Vitest and Testing Library, and end-to-end tests with Playwright against a fake gateway that replays synthetic events. No real posts in fixtures (decision 0004).
- **Quality gates in CI:** type check, lint, tests, a build, and an accessibility check of each screen.
- **Speed factor.** The screens show model time, so a load test at 50 times the live speed is readable.

## Consequences

- A second language and toolchain in the repository, with its own CI job.
- The public page is cheap to serve and safe to cache, and it shows no personal data.
- The demo level needs placeholders that still make the screens understandable without text. This is designed with the first build.
