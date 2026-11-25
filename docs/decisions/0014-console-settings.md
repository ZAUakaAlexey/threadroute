# 0014. Settings changed from the console

Status: Accepted
Date: 2026-11-24

## Context

Every routing and simulator number is an environment variable that a service reads once at start. To try another value, the service must restart. A restart resets its counters and puts every simulated operator offline, so the effect of the change is mixed with the effect of the restart.

The first day of live traffic showed that these numbers decide how the stand behaves (decision 0009, the team cap and the overflow). The project owner wants to change them from the console and see what happens.

Facts that shape this decision:

- **The number of teams is a result, not a setting.** It follows from five numbers: the load that forms a team, the load that releases it, the release time, the team size bounds and the share of operators that teams may hold.
- **Capacity is the strongest lever.** In the evening of 2026-11-24 about 8,000 conversations waited, the operators resolved about 98 per minute, and the queue did not get shorter. While demand is above capacity, team rules change who waits, not how many wait.
- **Team rules act slowly.** Decision 0009 has no forced exit from a team. After the cap was turned on, the teams needed 84 minutes to shrink to it.
- **The supervisor screen shows only current values.** Without history a person cannot see the effect of a change.
- **The simulator already reads router settings** from `GET /v1/settings` at start, and the gateway reads the same endpoint.
- **The site keeps the sign-in in a cookie that page code cannot read.** Decision 0012 chose "the token goes in the first message" (point 5C). The console cannot do that with the real site, because it has no way to read the token.

Choices the project owner made on 2026-11-24, before the options below were written:

- Expose the five existing team numbers. No new hard limit on the number of teams.
- The first version covers simulator capacity, team rules and a strip of trends. Timers come later.
- The right to change settings comes from a role on the site, not from a local switch.

## Settings of the first version

| Setting | Service | Default | Bounds | Effect |
|---|---|---|---|---|
| Simulated operators | sim | 200 | 10 to 500 | within a minute |
| Capacity of an operator | sim | 4 | 1 to 20 | within a minute |
| Handling time, median | sim | 45 s | 5 s to 10 min | for conversations accepted after the change |
| Resolve time, median | sim | 3 min | 10 s to 30 min | for conversations replied to after the change |
| Share of missed offers | sim | 0.03 | 0 to 0.5 | at once |
| Load that forms a team | router | 20 | 2 to 500 | at the next team check (10 s) |
| Load that releases a team | router | 8 | 1 to the form load minus 1 | after the release time |
| Release time | router | 15 min | 1 min to 2 h | gradual |
| Team size, min and max | router | 2 and 20 | 1 to 50, min not above max | at the next team check (10 s) |
| Share of operators in teams | router | 0.5 | 0.05 to 1 | growth at once, shrinking gradual |

Not in the first version: the speed factor, worker counts and intervals (they can break the stand and do not change the picture), the accept timeout and the two targets (second version), shift lengths and the language spread of the simulator.

The whole set is checked together, with the same rules as at start: the form load is above the release load, the max size is not below the min size.

## Options and recommendations

### 1. Where the changed values live

- **A. In the memory of each service.** A restart brings the defaults back. Nothing records who changed what.
- **B. In the router's PostgreSQL:** one row with the current set and its version number, and a log table with the time, the old set and the new set of every change. The router reads the row at start.
- **C. In a compacted Kafka topic** that the router, the simulator and the gateway read. It fits the pipeline, but it adds a topic, and the change log needs a second place anyway.

Recommendation: **B**. The values survive a restart, and the log gives exact times for cutting a measurement into segments.

### 2. How the simulator learns its settings

- **A. The router keeps the simulator part too.** It checks the bounds and does not use the values. The simulator asks `GET /v1/settings` every few seconds and applies a set with a new version number. One store, one version, one write. At its start the simulator reports the values of its own environment. They become the defaults of its part, and the current values when the store has none yet. (The report was removed on 2026-11-25, see "The settings live only in the store" below.)
- **B. The simulator gets its own HTTP API,** and the gateway writes to two services. A change can then succeed in one and fail in the other.
- **C. A Kafka topic,** as in 1C.

Recommendation: **A**. The simulator already depends on this endpoint. The contract of the simulator part lives in the shared `opapi` package, so both sides check the same bounds.

### 3. How the simulator changes on the run

- **A. Step by step, without a restart.**
  - The language of an operator depends only on the language spread and the number of the operator. The active operators are always the numbers 1 to N. So every count gives the same operators the same languages, whatever changes and restarts came before, and the languages keep the proportions of the spread at every count.
  - Fewer operators: the operators with the highest numbers go offline through the normal call and leave the shift cycle. Their conversations return to the queue by the existing rule of decision 0010. They stay registered and offline.
  - More operators: operators that were put aside come back first, in the order of their numbers. Then new ones are created with the next numbers. They take their place in the shift cycle by the same rule as at start.
  - Another capacity: the simulator sends the operator again with the new value. An operator who holds more than the new capacity keeps the conversations and gets no new offers until the load is below it.
  - Other times and shares: they apply to steps planned after the change. A step that is already planned keeps its time.
- **B. The simulator restarts its engine inside the process.** Simple, but every operator goes offline and online, so every held conversation returns to the queue. The change is then mixed with that wave.

Recommendation: **A**. The cost: a run with changes cannot be repeated from the seed alone. The change log holds the times.

### 4. What the router does when a team number goes down

- **A. Keep the rule of decision 0009: no forced exit.** Teams above the new numbers stop growing and shrink as their members leave. The console marks such settings as "acts gradually".
- **B. Force the teams to the new numbers at once.** The effect is visible in seconds. It is a new routing rule: the router must choose who leaves and what happens to conversations in team queues.

Recommendation: **A** for the first version. If the wait is too long to be useful, B comes as its own change to decision 0009.

The team size is not part of this choice. The size check of decision 0009 already adds and removes members at every team check, so a new min or max size acts at the next check.

In both options the router swaps the whole set in one step, and each team check and each pick reads one consistent set.

### 5. Where the trend history lives

- **A. In the gateway's memory:** one point every 10 seconds for the last 60 minutes, sent with the first state and then point by point. The series: waiting conversations, the longest wait, resolved per minute, teams, operators in teams, operators online. Marks of setting changes come from the change log. A gateway restart loses the history. It holds only counts, so the demo level and the public page may show it.
- **B. In the browser only.** A newly opened page has an empty chart and no marks of earlier changes.
- **C. A Prometheus server on the stand** with a query path for the console. The right tool for long history, but a new service and a new public path.

Recommendation: **A**. About 360 points per series is small, and one hour is enough to see a change act.

### 6. Who may change settings

The site has several roles. A new account gets the ordinary one. One role is given only by an administrator and means read-only access on the site, so it fits invited guests.

- **A. One list.** Whoever has the full level (decision 0012) may also change settings.
- **B. Two lists in the gateway:** the roles that get the full level, and the roles that may change settings. Both are empty by default.

Recommendation: **B**. Seeing post text and changing the stand are different rights. With two lists the owner can give a guest role both, or only the first, without a code change.

Rules in both options:

- The gateway checks the right, the bounds and the version on every change. The console only hides the controls.
- Everyone sees the current values, read-only.
- A change carries the version it was based on. If someone else changed the set first, the change is refused and the console shows the new values.
- At most one change per 10 seconds for the whole stand.
- "Reset to defaults" is one change like any other.

### 7. How the site sign-in reaches the gateway

This replaces point 5C of decision 0012 for the site sign-in, because page code cannot read the site's token.

- **A. The gateway reads the site's sign-in cookie from the WebSocket opening request** and checks it with the site API as today. The name of the cookie is a setting of the gateway. It works when the console and the gateway are served under the site's domain. The site does not change. The origin check of decision 0011 stays required, because a cookie is sent without any action of the page.
- **B. The site gets a new endpoint that gives the page a short token for the console.** Clean, but it is a change and a deploy of the site.
- **C. The console gets a field to paste a token.** No site change, but the site token lives a short time and a person must get it by hand.

Recommendation: **A**.

Open point for A: the cookie holds a token that lives a short time, and the site refreshes it when its own pages load. A person who opens the console with an old token gets the demo level until they open a site page. This must be checked on the real site before the public demo.

On a laptop there is no site cookie at all, because the console runs on `127.0.0.1`. So option A alone gives the owner no way to change settings on the local stand. For the local stand one of these is needed:

- **L1. A gateway setting that names one role for every connection,** accepted only when every allowed origin (decision 0011) is a loopback address. The gateway refuses to start with it otherwise. A public stand must list its public origin, so it cannot run with this setting. The listen address is not a usable test: inside a container the gateway listens on every address, and the port is bound to the loopback address by the container settings.
- **L2. The paste field of option C,** shown only on a local address.

Recommendation: **L1**.

## Choices confirmed by the project owner

On 2026-11-24 the project owner chose the recommended option in every point:

- **Store:** the router's PostgreSQL, with a change log (1B).
- **Simulator settings:** kept by the router and read by the simulator from `GET /v1/settings` (2A).
- **Simulator changes:** step by step, without a restart (3A).
- **Team numbers that go down:** no forced exit, the console says "acts gradually" (4A). On 2026-11-25 one exception was added to decision 0009: a change that lowers the share of operators in teams asks for one trim at the next team check. The other team numbers keep 4A.
- **Trend history:** in the gateway's memory, one hour (5A).
- **Rights:** two lists of roles (6B). For now the owner gives the site's guest role both rights.
- **Site sign-in:** the cookie of the opening request (7A), and the role setting for a local stand, tied to loopback origins (L1).

Point 7A replaces "the first message after the connection opens" (point 5C of decision 0012) for the site sign-in. The `hello` message stays the first message of the protocol. It no longer has to carry a site token.

## Details that follow

- **Replaced on 2026-11-25, see "The settings live only in the store" below.** The environment gives the defaults, the store gives the current values. The team values of the router environment and the values the simulator reports are the defaults. While nothing is stored, the environment values are in effect. The first report of the simulator already stores the whole set, so in practice this happens at the first start. From then on the stored set wins, also over a new environment value at a restart: a new value in the environment only becomes the new default. "Reset to defaults" in the console brings the environment values back.
- **Removed on 2026-11-25, see "The settings live only in the store" below.** A router whose environment values are outside the bounds runs with them as before and does not offer the settings to the console.
- **The change log keeps the changed fields** with the old and the new value, the time and a short label of who changed them. The label is shown only to connections that may change settings.
- **Everyone sees the current values** in the settings panel of the supervisor screen. The trends are on the same screen. The help texts of both screens use the current values.

## Protocol changes

- `welcome` carries the current set, the defaults, the bounds, the version, the last changes and whether this connection may change settings.
- A new server message tells every connection about a new set.
- A new client message asks for a change with the whole set and the base version. The answer is "applied" or a list of fields with reasons.
- The help texts take the real values, so the words "by default" go away.

## Rules for measurements

- Every change is in the log table with its time.
- The router and the simulator export the current values as metrics, and the router counts the changes.
- Nothing of this is deployed to the measurement server before the current window ends.
- A live change is mixed with the time of day. The numbers for the README come from the replay of a recorded day (stage 5), not from the console.

## Change on 2026-11-25: the settings live only in the store

The first version had two sources for every changeable number: an environment variable and the stored set. From the first start on the stored set won, so a person who changed a `TEAM_` or `SIM_` value in `.env` and restarted the stand saw no change, no error and no log line.

The eleven numbers of "Settings of the first version" are now state of the stand, not configuration.

- **One source.** The current values are in the router's PostgreSQL and nowhere else. The six team variables of the router and the five variables of the simulator for these numbers are removed. Every other variable (ports, intervals, the speed factor) stays in the environment.
- **Defaults in the code.** The default of every number is in the shared `opapi` package, next to its bounds.
- **Seed.** At a start on a database without a stored set, the router stores the defaults as version 1. The change log gets an entry with the label "defaults". A database that already has a stored set keeps it. If its simulator part is missing, the router adds the defaults of that part.
- **Changes.** From the console, or with `PUT /v1/settings` of the operator API. Both are in the change log, and both follow the same checks.
- **"Reset to defaults"** brings back the defaults of the code.
- **The simulator only reads.** It no longer reports values at its start, and the call for that report is removed. It reads the set from `GET /v1/settings` at start and then every few seconds, as before.
- **A removed variable that is still set** is not read. The service logs a warning at start that names the variable and says where the value lives now.
- **A stored row that is not valid** (only a hand edit can make one) turns the settings off, as before. The router and the simulator then run with the defaults of the code.

What goes away with the second source: the question which source wins, the state "settings are off because the environment is outside the bounds", and the state "no simulator has reported yet".

The cost:

- A stand that should start with other values needs one call to the operator API after its first start, or the console.
- A wiped database volume brings the defaults back. A line in `.env` survived that.
- The values of a measurement run are no longer visible in `.env`. They are in the change log, and a measurement script must save the answer of `GET /v1/settings` at its start.

The project owner chose this on 2026-11-25. Options that were not chosen:

- **Keep the first version.** No work, but a changed variable has no effect and nothing says so.
- **A changed environment value wins at the next start.** Both sources stay, with a comparison at every start and a rule for values that do not fit together.
- **The environment gives the defaults, the store keeps only what a person changed.** Clear to say, but a variable has no effect for a field that was changed in the console.
- **The environment only seeds an empty database.** This is the first version under another name.
