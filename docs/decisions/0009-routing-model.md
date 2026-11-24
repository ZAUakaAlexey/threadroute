# 0009. Routing: queues, teams, assignment and SLA

Status: Accepted
Date: 2026-11-23

## Context

`conv` publishes conversation events (opened, message added, answered, closed) to the `conversations` topic (decision 0007). The `router` service decides which operator handles each conversation.

Agreed earlier:

- Operators are simulated. The busiest companies get a dedicated team, and all other companies share one pool of operators grouped by language.
- Assignment is pushed to an operator, who must accept within a time limit. If they do not, the conversation goes to the next operator.
- A conversation stays with its operator while it is open, and a reopened conversation prefers the same operator.
- Each conversation gets exactly one operator. The first implementation uses PostgreSQL with `SELECT ... FOR UPDATE SKIP LOCKED`.
- Routing data lives in PostgreSQL, through `pgx` and `sqlc`, with `goose` migrations.

Companies come and go within hours (decision 0008), so a dedicated team needs a lifecycle.

## Decision

### Operators, skills and capacity

- An operator has a set of languages, a capacity (how many conversations they handle at once), a presence state (online or offline) and, at most, one dedicated company.
- A conversation uses one slot of its operator from the moment the operator accepts it until the operator resolves it or the conversation closes.

### Queues

- A conversation that needs an operator waits in a queue. The queue of a conversation is its language, plus its company when that company has a dedicated team.
- Inside a queue the order is by priority, then by waiting time. Priority starts at normal and rises when an SLA target is missed.
- A conversation whose language is unknown (`und`) goes to a general queue. An operator serves it when their languages include `und`.
- Since 2026-11-24 the operators of the general queue also take a conversation whose language no online operator has. See "Changes after the first live run of the console".

### Dedicated teams

- A company gets a dedicated team when the number of its conversations that are open at the same time reaches a threshold.
- The team is a group of operators taken from the shared pool for the time the company needs them. Its size follows the load of the company: enough operators for its open conversations at their capacity, between a minimum and a maximum.
- Team members serve only their company. Conversations of that company are served only by the team.
- The team is released when the company has stayed below a lower threshold for a set time, or when its admission ends. Its operators finish the conversations they already hold and go back to the shared pool. Conversations of the company that are still waiting go to the shared queues.
- The two thresholds are different on purpose, so a company near the limit does not gain and lose its team again and again.
- Since 2026-11-24 all teams together hold at most a share of the online operators. See "A cap on the operators that teams hold".

### Assignment

1. When a conversation opens or reopens, the router puts it in its queue. A reopened conversation is offered first to its previous operator, if that operator is online and has a free slot.
2. For a waiting conversation the router picks an operator who is online, has the language, has a free slot and belongs to the right group (the team of the company, or the shared pool). Among those it picks the one with the lowest load. Since 2026-11-24 a priority of the operator breaks a tie (see "Changes after the first live run of the console").
3. The pick and the reservation of the slot happen in one database transaction with `FOR UPDATE SKIP LOCKED` on the conversation row and the operator row, so two router workers can never give one conversation to two operators or overfill one operator.
4. The operator gets an offer and must accept within the accept timeout. On accept the conversation is assigned. On timeout or refusal the slot is freed, the conversation returns to the queue with its waiting time kept, and that operator is skipped for this conversation.
5. The operator resolves the conversation when they are done. That frees the slot. A later customer message in the same session puts the conversation back in the queue, with the same operator preferred.
6. When `conv` closes the conversation, any offer or assignment ends and the slot is freed.

### SLA

Two targets are measured for each session:

- **Time to assignment:** from the moment the conversation enters the queue until an operator accepts it.
- **Time to first response:** from the first customer message until the operator's first reply.

When a target is missed, the conversation gets a higher priority and an event is published. The real response time of the thread author (decision 0007) is stored next to the simulated one for comparison.

### Clock

Queue waiting times, the accept timeout and the SLA targets run on the clock of the machine, multiplied by a speed factor from the configuration. The factor is 1 for live traffic. A load test that replays a recording at 50 times the live speed sets it to 50, so that operators and timers keep the same proportion to the traffic.

Conversation windows in `conv` run on source time (decision 0007). The router cannot do the same, because an operator acts in real time and a human in the console (stage 4) has no source time.

### Events and state

- The router reads `conversations` with a consumer group and is idempotent: it skips an event whose sequence number it has already applied for that conversation.
- It publishes routing events (queued, offered, accepted, offer expired, resolved, SLA missed, team formed, team released) to a topic `routing`, keyed by the conversation key, for the gateway and the simulator. They carry IDs, times and counters, and no post text.
- State changes in PostgreSQL and routing events use the outbox pattern: the event rows are written in the same database transaction as the state change and published to Kafka afterwards.

## Details fixed before implementation

These points follow from the decisions above and from decisions 0007 and 0008.

- **Which conversations count for a team.** The count is the active load of a company: its conversations that wait in a queue or are held by an operator. It is not the number of conversations that `conv` still has open, because a conversation stays open there for 30 minutes after the last message even when no operator is needed. A team forms at 20 active conversations, is released below 8 for 15 minutes, and its size is the active load divided by the capacity, rounded up, between 2 and 20. The size is checked again at a fixed interval.
- **Languages in a team.** Team members are taken from the shared pool for the languages of the company's active conversations, most needed language first. A conversation of a team company goes to the team only if a team member has its language. If no member has it, the conversation is served from the shared queue of that language like any other. It never waits for a team that cannot serve it.
- **Where the timers start.** The waiting time and both SLA targets start when the router puts the conversation in a queue, on the machine clock with the speed factor. They do not start at the source time of the customer message. A source time can be hours old after a catch-up, and every such conversation would count as late at once.
- **Catching up.** When the router starts, it reads the end offsets of `conversations` and first applies everything up to them to its state without making any offer. Conversations that were opened and closed in that backlog never reach an operator. When the backlog is done, the conversations that are still open enter their queues, and their waiting time starts at that moment. A new consumer group starts at the beginning of the topic.
- **Sequence numbers from `conv` can restart.** `conv` forgets a conversation 24 hours after it closed and then starts again at session 1, sequence 1 (decision 0007). The router skips an event whose session and sequence number it has already applied. The one exception: an "opened" event with session 1 and sequence 1 for a conversation that the router holds as closed is accepted and resets the stored numbers.
- **Order of routing events.** Each routing event about a conversation carries a sequence number of its own, per conversation, given in the same database transaction as the state change. Events about a team are keyed by the company and carry a sequence number per company. One publisher at a time sends the outbox to Kafka in the order of the outbox rows, so events of one key reach the topic in order. Consumers skip a sequence number they have already seen.
- **One speed factor.** The factor is a setting of the router only. The simulator reads it from the router's API at start, so the two cannot drift apart.
- **Queue language.** The language of a conversation in the router is the language of its session in the conversation events (decision 0007). It does not change inside a session.
- **Pending offers when a team changes.** An offer that is already out when a team forms or is released runs to its end: accept, refusal or timeout. An operator who joins or leaves a team keeps the conversations and offers they already have. Only conversations that wait without an offer move between the shared queue and the team queue.
- **What releases a team.** Only low load releases a team. `conv` publishes no event for the end of an admission, and it does not need one: an admission ends after 2 hours without a customer message (decision 0008), and by then the company has had no active conversation for much longer than the 15 minutes of the release rule.
- **More routing events than the list above.** The topic also carries "replied" (a reply marker of the operator), "ended" (the conversation was closed by `conv`, so an offer or assignment ended) and "team resized". The simulator and the gateway need them to follow a conversation and a team to the end.
- **Durations in routing events.** Points in time are machine clock times. Durations (waiting time, response time, the measured value of a missed target) are machine time multiplied by the speed factor, so they compare directly with the SLA targets.
- **Skipped operators.** An operator who let an offer time out or refused it is skipped for that conversation until the session ends. When every online operator who could take the conversation has been skipped, the skips of the session are cleared, so a conversation in a small queue does not wait forever.
- **Previous operator.** The previous operator gets the conversation only if they pass every rule: online, language, group and a free slot. Then they win over an operator with a lower load. The preference holds for one offer.
- **SLA per session.** Both targets are measured once per session, from the first time the conversation enters a queue. A customer message after a resolve gives a new waiting time but does not start the targets again. Each missed target raises the priority by one.
- **Capacity.** The load of an operator is counted from their live offers and assignments and is not stored. When the capacity of an operator is lowered below their load, they keep what they hold and get nothing new until the load is below the new capacity.
- **One consumer process.** The router reads `conversations` as one process with several assignment workers inside. The database operations are safe for several processes, but the catch-up phase assumes one. The outbox is published under a database lock, so only one process publishes at a time.
- **A conversation that never closes.** If `conv` loses its state, the "closed" event of a conversation never arrives. The slot is still freed when the operator resolves the conversation, so no operator stays blocked. The row of such a conversation stays in the database. Cleaning these rows up is not decided yet.

## Changes after the first live run of the console (2026-11-24)

The first run with a browser (41 minutes, README, stage 4) showed two problems in the rules above. The project owner chose a change for each on 2026-11-24.

### A language that no online operator has

Conversations in Danish, Dutch, Polish, Russian and other small languages waited for minutes and missed their targets, because no operator had the language. Such a conversation waited until `conv` closed it.

- **Rule.** When no online operator of the shared pool has the language of a waiting conversation, operators who serve the general queue (`und`) may take it. When at least one online operator of the shared pool has the language, the conversation waits for those operators as before, also when all of them are busy.
- **Checked at every pick.** The rule is not a move between queues. The conversation keeps its language and its place by priority and waiting time, and the router checks the rule each time it looks for an operator. So nothing has to move when the last operator of a language goes offline or the first one comes online.
- **Teams.** Nothing changes for a team queue. A conversation of a team company whose language no team member has is already served from the shared pool, and this rule then applies to it there.
- **Skips** work as before. "Every online operator who could take the conversation" now includes the operators of the general queue when the rule applies.
- **Counted.** The router counts the offers made under this rule, so the share of conversations that nobody with the language served is visible.

Options that were not chosen: use the general queue only after the assignment target has passed (60 seconds of waiting that nobody can use), and no change (an honest picture of a missing skill, but a conversation that can never be served).

### An operator priority

A person in the console waited 374 to 469 seconds for a first offer in the first minutes after a cold start, and 2 to 10 seconds later in the run. The cause: among operators with the same lowest load the router took the lowest operator ID, and the IDs of simulated operators sort before the IDs of people. While any simulated operator with the language was idle, a person got nothing.

- **Rule.** An operator has a priority, a small whole number, 0 by default. Among the operators that pass every rule, the order is: the previous operator of the conversation, then the lowest load, then the highest priority, then the operator ID.
- **Who sets it.** The caller of the operator API (decision 0010). The gateway gives the operators of people priority 1. The simulator sends none. The router still does not know whether an operator is a person.
- **What it changes.** A person with no conversation gets the next conversation in their languages. A person who already holds one still loses to an idle simulated operator, so the simulated numbers change very little.

Options that were not chosen: priority before load (a person gets offers while they have a free slot, which moves further away from "lowest load"), and no change.

## Change after the first day of live traffic (2026-11-24)

### A cap on the operators that teams hold

About five hours into the first 24-hour run, at daytime traffic, the number of teams grew from 11 to 60 in 90 minutes. The teams held 174 of the 180 online operators, 6 were left in the shared pool, and about 3,850 conversations waited, the oldest for more than an hour. At the same time about a fifth of all slots was free, because a team member serves only their company.

The cause is a loop in the rules above. A waiting conversation counts for the active load of its company. When the shared queues grow, more companies reach the threshold and get a team, each team takes operators out of the shared pool, and the shared queues grow faster.

- **Rule.** All teams together hold at most a share of the online operators. The start value is one half. The router checks the cap when an operator would join a team: a new team, or a team that grows.
- **At the cap.** A team that should grow gets only as many operators as the cap leaves. A new team forms only if the cap leaves room for the minimum team size. Otherwise the company stays in the shared queues, and the router tries again at the next check.
- **Order.** When the cap does not leave enough for every company, the company with the highest active load comes first.
- **No forced exit.** The cap never takes an operator out of a team. When operators of the shared pool go offline and the share of the teams rises above the cap, the teams only stop growing. They get smaller by the normal size check and the release rule.
- **Off switch.** A share of 1 turns the cap off.
- **Counted.** The router shows the number of online operators in teams, the current cap, and how often the cap stopped a team from forming or growing.

The project owner chose this option on 2026-11-24. Options that were not chosen:

- **Idle team members help the shared pool.** It removes the free slots, but a team can then be busy when its company needs it, and it adds more rules and more edge cases. It can be added later if free team slots stay a problem.
- **No new team while the shared queue is late.** It works against the loop directly, but teams then depend on the state of the queues, which is harder to explain and to test.
- **More simulated operators and no rule change.** It helps for this traffic, and the loop comes back at the next peak.

The rule for a language that no online operator has (above) removes a part of what fed the loop: conversations that nobody could serve stayed in a queue and counted for the load of their company. How much of the loop is left without the cap is not measured. The recorded stream of that day can be replayed against the new rules in the load tests.

## Choices confirmed by the project owner

- **When a slot is freed:** when the operator resolves the conversation, not when the conversation closes by inactivity. The alternative keeps every conversation with its operator for at least 30 minutes and needs several times more operators.
- **Team rules:** by the number of open conversations of a company, with separate thresholds to form and to release a team. Alternatives were a fixed number of top companies, and a team that lasts as long as the admission.
- **Clock:** the machine clock with a speed factor. Alternatives were source time, which does not work for a person in the console, and the machine clock without a factor, which makes load tests at 50 times the live speed meaningless.
- **Routing events:** a Kafka topic with an outbox in PostgreSQL. Alternatives were HTTP polling and PostgreSQL notifications.

## Choices and their starting values

These are configuration values. The numbers are starting points and are tuned with the load tests.

| Setting | Start value | Note |
|---|---|---|
| Operator capacity | 4 conversations | a usual value for chat support |
| Accept timeout | 20 seconds | |
| Time to assignment target | 60 seconds | |
| Time to first response target | 120 seconds | |
| Team threshold, form | 20 open conversations of one company | |
| Team threshold, release | below 8 for 15 minutes | |
| Team size | open conversations divided by capacity, rounded up, between 2 and 20 | |
| Share of online operators that teams may hold | one half | added on 2026-11-24 |

## Alternatives considered

- **One shared pool with a share limit per company.** It solves the noisy neighbour problem with one scheme. It was not chosen in the planning discussion in favour of teams plus a pool.
- **Pull model, operators take conversations themselves.** Simpler, but it does not show assignment logic, and it was not chosen in the planning discussion.
- **Assignment with a Lua script in Redis.** Faster on paper. It is planned as a later comparison against the PostgreSQL version, with numbers.

## Consequences

- The router is the first service that needs PostgreSQL.
- The team rules add state and moving parts: forming, resizing and releasing a team must be safe when several things change at once. They are covered by tests on a real database.
- The speed factor is a second notion of time next to source time. Load-test setups must set it to match the replay speed.
- Without a share limit, a company that is busy but below the team threshold competes with everyone else in the shared queues.
- PostgreSQL is one more process on the server. Its memory use is measured before deployment, next to Kafka and Redis.
