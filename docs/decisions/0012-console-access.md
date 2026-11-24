# 0012. Console access: sign-in, roles and the human operator

Status: Proposed
Date: 2026-11-24

## Context

Decision 0010 says that the operator API gets authentication "with the site's tokens" when the gateway arrives. The console runs under the project owner's site, which already has accounts.

Facts about the site that matter here:

- Its tokens are signed with a shared secret. Whoever holds that secret can check tokens and can also create them, for any user and any role.
- Anyone can create an account. So "signed in" does not mean "trusted".
- The web server writes request URLs to its log.

Facts about the pipeline:

- The simulator owns the operator IDs it creates, and puts them offline when it starts and stops.
- Conversation keys are author IDs of real people. Message text is real post text.

## Options and recommendations

### 1. How the gateway checks a site token

- **A. The gateway gets the site's signing secret.** Simple and needs no network call. The cost: a leak from this project would let an attacker create site tokens, including admin ones.
- **B. The gateway asks the site API who the caller is,** with the caller's token, once per connection, and keeps the answer for a short time. No secret is shared. A sign-in to the console needs the site API to be up.
- **C. The site issues a separate short token for the console,** signed with a key pair, and the gateway holds only the public key. The cleanest separation. It needs a new endpoint in the site's API.
- **D. The console has its own accounts.** No link to the site, and a second place with passwords.

Recommendation: **B** now. It needs no change in the site and shares no secret. C is the better design if the console stays for long, and can replace B without a change in the browser.

### 2. Who sees post text and author IDs

- **A. Every signed-in user.** With open sign-up this makes the text public in practice.
- **B. Only users on a list** that the project owner keeps in the gateway configuration (site user IDs), plus the owner. Everyone else gets data without text and without author IDs.
- **C. Nobody.** The console never shows text.

Recommendation: **B**. The owner can add a reviewer to the list for a demo. The default for an unknown user is no text.

### 3. Who can try the operator role

- **A. Only users on the list from point 2.**
- **B. Any signed-in user, without text.** The screen shows language, counters and timers, and the buttons work.
- **C. Anyone, without signing in and without text,** with a limit on how many such operators exist at the same time and a rate limit per address.

Recommendation: **C**. A person who opens the demo should be able to accept and resolve a conversation in a minute, without creating an account. Their actions touch only simulated routing. The limit (5 at the same time) keeps a crowd from changing the numbers.

### 4. Who the human operator is in the router

- **A. They take the seat of a simulated operator.** The simulator must give up that ID and take it back later, which needs new logic in the simulator.
- **B. They are an operator of their own.** The gateway creates an operator ID on the server from the checked identity (or from the anonymous session), puts it online while the connection is open, and offline when it closes. The simulator is not touched.

Recommendation: **B**. Rules that follow:

- The operator ID is made by the gateway. The browser never sends one.
- A closed connection puts the operator offline after a short grace time (15 seconds), so that a page reload does not drop their conversations. Offline returns the conversations to the queue (decision 0009).
- A human operator chooses languages in the console and has a capacity of 2.
- How long a human waits for a first offer is measured in the first run. In the stage 3 run the router made about 28 offers per minute for about 180 operators. If the wait is longer than a minute, a rule that prefers human operators is proposed as a change to decision 0009.

### 5. How the token reaches the gateway

- **A. In the WebSocket URL.** It then lands in the web server log.
- **B. In a cookie.** It needs the site to set that cookie for the console path.
- **C. In the first message after the connection opens.** The gateway closes a connection that does not send it within 5 seconds.
- **D. A one-time ticket:** the browser first calls an HTTP endpoint with the token in a header, gets a ticket that works once and for 30 seconds, and opens the WebSocket with the ticket in the URL.

Recommendation: **C**. Nothing secret is in a URL, and no extra endpoint is needed.

## Access levels that result

| Level | Who | Sees | Can do |
|---|---|---|---|
| Public | anyone | totals only, no keys (decision 0013) | nothing |
| Demo operator | anyone, limited number | their offers and conversations without text and author IDs, the supervisor screen without them | accept, refuse, mark a reply, resolve |
| Full | users on the owner's list | the same with text and author IDs | the same |

## Details that follow

- **Pseudonyms.** Where author IDs are hidden, the screen shows a short label made from a keyed hash of the ID. The key changes every day and never leaves the gateway, so the label is stable for a day and cannot be turned back into the ID.
- **Operator kind.** The router does not need to know whether an operator is a person. The gateway marks human operators in its own view by their ID prefix.
- **Limits.** Connections per address, actions per second per operator, and message size are limited in the gateway.
- **The operator API of the router stays closed to the internet.** Only the gateway calls it.

## Consequences

- The gateway depends on the site's API for sign-in (option 1B).
- The owner keeps a list of user IDs in the configuration. It is not in the repository.
- Anonymous demo operators are a way for strangers to cause load. The limits above bound it, and the load test of stage 5 includes it.
