// Every help hint and screen description of the console, in one place, so
// they can be read and edited together. The screens refer to a text by its
// ID; help-texts.test.ts checks that every ID used exists here and that no
// text here is unused.
//
// Writing rules: simple English, one to three short sentences, concrete,
// plain ASCII (no dashes, arrows or emoji), and only behaviour the system
// has. The meaning of each number is taken from docs/gateway-protocol.md and
// from the gateway code that computes it (internal/gateway/view_read.go).
//
// Values that the gateway sends in its welcome message are placeholders in
// braces. They are filled from the live settings, or with the fallback
// words below while the settings are not known (the public page never has
// them, so its texts use no placeholders).

const TEXTS = {
  // Screen descriptions, shown under the screen title.
  'public.description':
    'threadroute reads the public Bluesky post stream, groups posts into customer and company conversations, and routes them to simulated operators by language, with timers for every service target. This page shows totals only: no post text, no account names and no keys.',
  'supervisor.description':
    'A live view of the whole contact centre: the traffic, the queues, the operators, the dedicated teams and every open conversation. The conversations come from real public Bluesky posts, without their text at the demo level. The operators are simulated, except visitors who took a seat on the operator screen.',
  'operator.description':
    'This is a practice desk. You get real conversations from public Bluesky posts, without their text at the demo level, and the other operators around you are simulated. Nothing you do here is posted to Bluesky: your actions only change the simulated routing.',
  'operator.conversation.note': '"Mark replied" only records a marker. Nothing is posted to Bluesky.',

  // Header of the live screens.
  'live.level.demo':
    'Demo level: you see no post text and no account IDs. Accounts, conversations and messages appear under pseudonyms that start with ps- and change every day. Operator IDs are made up by the project and are shown as they are.',
  'live.level.full':
    'Full level: you see post text and real account IDs, because the site allows your account to. The text stays in the memory of this page only.',
  'live.connection':
    'Live: the data is current, and the gateway sends changes as they happen. Stale: the gateway reports that its data is behind, so the numbers may be old and the timers stop. Connecting or Offline: the page has no connection to the gateway; the banner below says what happens next.',
  'live.speed':
    'The router runs its clocks {speedFactor} times faster than real time, as in a load test. Every time on this screen is model time: real time multiplied by {speedFactor}.',
  'live.banner.stale':
    'The gateway builds this view from the event topics of the pipeline. It is stale while the gateway catches up after a start, cannot read the topics, or is too far behind them. The numbers may be old, and timers stop until the data is current.',
  'live.banner.reconnecting':
    'The page lost its connection to the gateway and tries again by itself, with pauses that grow up to 30 seconds. The numbers stay as they were at the loss, and timers stop.',
  'live.messages.demo':
    'At the demo level you see who wrote each message, when, and how long it is, but not its text. New messages appear here while this conversation is open.',
  'live.messages.full':
    'You see the text of the posts. It stays in the memory of this page only and is dropped at once when the author deletes the post.',

  // Conversation states, on the state bar of the public page and the supervisor screen.
  'state.open': 'Open: the conversation exists, but the router has not put it in a queue in this session yet.',
  'state.waiting': 'Waiting: the conversation is in a queue, and no operator has an offer for it yet.',
  'state.offered':
    'Offered: an operator got an offer and has {acceptTimeout} to accept it. Without an accept, the conversation goes back to its queue and keeps its waiting time.',
  'state.assigned': 'Assigned: an operator accepted the conversation and holds it until they resolve it or the conversation closes.',
  'state.resolved':
    'Resolved: the operator finished and freed the slot, but the conversation is still open. A new customer message puts it back in a queue.',

  // Supervisor screen: totals.
  'supervisor.totals.messages':
    'New messages per second in the tracked conversations, from customers and companies together, over the last 10 seconds. Only accounts that get many replies are tracked, so this is a small part of the Bluesky stream.',
  'supervisor.totals.open':
    'All open conversations, in every state, resolved ones included. A conversation closes after some time without a new message (30 minutes by default). Waiting: conversations in a queue that have no offer yet.',
  'supervisor.totals.companies':
    'Companies with at least one open conversation. A company is the Bluesky account that started a thread; the people who reply to it are its customers.',
  'supervisor.totals.operators':
    'Operators who are online now. Most are simulated by the project; the human ones are visitors who took a seat on the operator screen.',
  'supervisor.totals.slots':
    'A slot is room for one conversation. The first number counts the offers and conversations that online operators hold now; the second is the sum of their capacities. An offer that waits for an accept also takes a slot.',
  'supervisor.totals.wait':
    'Time from entering the queue until an operator accepted, over all accepts of the last hour; conversations that still wait are not in it. Half of them waited less than the median, and 9 of 10 less than the 90th percentile. 0 means no accept in the last hour.',
  'supervisor.missed':
    'Targets of the router that were missed in the last hour. Assignment: an operator accepts within {assignmentTarget} after the conversation entered the queue. First response: the operator marks a reply within {firstResponseTarget} of the same moment.',

  // Supervisor screen: queues by language.
  'supervisor.panel.queues':
    'Conversations that need an operator, counted per language. The shared queue and the team queues of a language are counted together. Most busy queues come first.',
  'supervisor.queues.language':
    'The language of the post that opened the conversation, or und (general queue) when the post has no language tag. Operators of the general queue also take languages that no online operator of the shared pool has; such a conversation still counts in its own row.',
  'supervisor.queues.waiting':
    'Conversations in this queue that no operator has an offer for yet. (N team): how many of them wait in the queue of a dedicated team.',
  'supervisor.queues.offered':
    'Conversations with an offer that waits for the operator to accept. The operator has {acceptTimeout}; then the conversation goes back to waiting.',
  'supervisor.queues.oldest':
    'How long the conversation that waits longest has been in this queue, waiting or offered. A conversation that comes back after an offer ran out keeps its queue time. None: the queue is empty.',
  'supervisor.queues.missed':
    'Conversations in this queue now that already missed the assignment target of {assignmentTarget}. Each missed target raises the priority of a conversation by one.',

  // Supervisor screen: dedicated teams.
  'supervisor.panel.teams':
    'A dedicated team is a group of operators taken from the shared pool to serve one busy company only. By default a team forms at 20 active conversations of the company, is released after 15 minutes below 8, and has 2 to 20 operators, sized by the load.',
  'supervisor.teams.pool':
    'By default, operators join teams only while all teams together hold less than half of the online operators, so the shared pool keeps the rest. When no team member with the language has a free slot, an operator of the shared pool may take the conversation. Active conversations: those of the company that wait or that an operator holds, counted when the team last changed.',

  // Supervisor screen: operators.
  'supervisor.panel.operators':
    'Every operator the gateway knows, online ones first. Simulated operators have IDs that start with sim-op- and work in shifts; human operators have IDs that start with web-.',
  'supervisor.operators.kind':
    'Simulated: an operator played by the simulator of the project. Human: a visitor who took a seat on the operator screen. With the same load, a human usually gets the next offer first.',
  'supervisor.operators.presence':
    'Online operators get offers. When an operator goes offline, the router puts their conversations back in the queue.',
  'supervisor.operators.load':
    'Offers and conversations the operator holds now, against their capacity. "+N offered" is the part of the load that waits for an accept, not extra load.',
  'supervisor.operators.languages':
    'The queues this operator serves. und is the general queue: posts without a language tag, and languages that no online operator of the shared pool has.',
  'supervisor.operators.team':
    'The company of the dedicated team this operator is in, as a pseudonym at the demo level. Empty: the operator is in the shared pool.',

  // Supervisor screen: conversations.
  'supervisor.panel.conversations':
    'Every open conversation that the gateway knows. A conversation is one company, the Bluesky account that started a thread, and one customer, a person who replied in it.',
  'supervisor.conversations.filters':
    'Show only one language or one state. The count next to the title says how many conversations match, of all open ones.',
  'supervisor.conversations.customer':
    'The customer, and under it the company the customer writes to. At the demo level both are pseudonyms that change every day.',
  'supervisor.conversations.lang': 'The queue language of the conversation. und is the general queue.',
  'supervisor.conversations.state':
    'Where the conversation is in routing: open, waiting, offered, assigned or resolved; the state bar at the top explains each one. "(team)": it waits in the queue of the dedicated team of its company.',
  'supervisor.conversations.operator': 'The operator who has the offer or holds the conversation. Empty when no operator has it.',
  'supervisor.conversations.waiting':
    'Time since the conversation entered the queue, while it waits or is offered. A dash in the other states.',
  'supervisor.conversations.messages': 'Messages of this session, from the customer and the company together. Removed messages still count.',
  'supervisor.conversations.targets':
    'On time: no target missed in this session so far. Missed assign: no accept within {assignmentTarget} after it entered the queue. Missed reply: no reply marker within {firstResponseTarget} of the same moment.',

  // Operator screen.
  'operator.panel.you':
    'Your own operator in the router; the gateway made its ID for this browser tab. When you close the page, you stay online for 15 seconds, so a reload keeps your conversations. After that you go offline and your conversations go back to the queue.',
  'operator.online':
    'Online: the router sends you offers from the queues of your languages. Offline: the offers stop, and the conversations you hold go back to the queue.',
  'operator.slots':
    'A slot is room for one conversation, and offers take a slot too. You can hold {capacity} conversations at once; with every slot in use you get no new offer.',
  'operator.languages':
    'The queues you serve. und is the general queue: posts without a language tag, and languages that no online operator of the shared pool has. When you and a simulated operator hold the same number of conversations, you usually get the offer first.',
  'operator.panel.offers':
    'An offer is a conversation the router picked for you. Accept it within {acceptTimeout}, or it goes back to its queue with its waiting time kept, and the router does not offer it to you again for now. Refuse does the same at once.',
  'operator.offer.priority': 'The priority rises by one for each target this conversation missed. Conversations with a higher priority are offered first.',
  'operator.panel.held':
    'Conversations you accepted. Each one takes a slot until you resolve it or the conversation closes. The badge counts down to the first response target, or says Replied or Reply late.',
  'operator.panel.conversation':
    'The conversation you chose, with its timers and messages. A session is one open period of this customer and company; when the customer writes again after it closed, a new session starts.',
  'operator.waiting':
    'Time since the conversation entered the queue for this visit. The clock keeps running after you accept.',
  'operator.first-response':
    'Time left to mark a reply: the target is {firstResponseTarget} from the moment the conversation first entered the queue in this session, so it started before your accept. Reply late: the target passed.',
  'operator.messages':
    'Messages in this session: from the customer / from the company. The company is the Bluesky account that started the thread, so its number changes only when that account really replies, not when you mark a reply.',
  'operator.actions':
    'Mark replied records that you answered and stops the first response clock. Resolve ends your work and frees the slot. If the customer writes again in this session, the conversation goes back to the queue and comes to you first while you are online and have a free slot.',

  // Public page.
  'public.freshness':
    'The page asks for new totals every 2 seconds. Stale: the last request failed, or the gateway reports that its data is behind, so the numbers show the last known state.',
  'public.totals.messages':
    'New messages per second in the tracked conversations, from customers and companies together, over the last 10 seconds. Only accounts that get many replies are tracked, so this is a small part of the Bluesky stream.',
  'public.totals.open':
    'All open conversations, in every state. Companies: the Bluesky accounts with at least one open conversation; the people who reply to them are their customers.',
  'public.totals.operators':
    'Operators who are online now. Most are simulated by the project; the people are visitors who took a seat on the operator screen.',
  'public.totals.slots':
    'A slot is room for one conversation. The first number counts the offers and conversations that online operators hold now; the second is the sum of their capacities.',
  'public.totals.wait':
    'Median time from entering the queue until an operator accepted, over all accepts of the last hour. 9 of 10 accepts came within the second number. 0 means no accept in the last hour.',
  'public.totals.missed':
    'Service targets missed in the last hour: no operator accepted in time, or no reply was marked in time. The time limits are settings of the router.',
  'public.panel.queues':
    'Conversations that need an operator, counted per language. The general queue (und) holds posts without a language tag; its operators also take languages that no online operator of the shared pool has.',
  'public.queues.waiting': 'Conversations in this queue that no operator has an offer for yet.',
  'public.queues.offered': 'Conversations with an offer that waits for the operator to accept. Without an accept, they go back to waiting.',
  'public.queues.oldest':
    'How long the conversation that waits longest has been in this queue, waiting or offered, at the time of the last update. A dash: no conversation in the queue.',
  'public.queues.missed': 'Conversations in this queue now that already missed the target for an accept. Each miss raises their priority.',
} as const satisfies Record<string, string>;

export type HelpId = keyof typeof TEXTS;

/** Live settings that some texts name. Missing values get the fallback words. */
export interface HelpValues {
  readonly acceptTimeout?: string;
  readonly assignmentTarget?: string;
  readonly firstResponseTarget?: string;
  readonly speedFactor?: string;
  readonly capacity?: string;
}

const FALLBACKS: Readonly<Record<keyof HelpValues, string>> = {
  acceptTimeout: 'a short time set by the router',
  assignmentTarget: 'the time set by the router',
  firstResponseTarget: 'the time set by the router',
  speedFactor: 'several',
  capacity: 'a fixed number of',
};

export const HELP_PLACEHOLDERS = Object.keys(FALLBACKS) as readonly (keyof HelpValues)[];

export const HELP_TEXTS: Readonly<Record<HelpId, string>> = TEXTS;

/** The text of a hint or description, with the live values filled in. */
export function helpText(id: HelpId, values: HelpValues = {}): string {
  return TEXTS[id].replace(/\{(\w+)\}/g, (match, name: string) => {
    if (!(name in FALLBACKS)) return match;
    const key = name as keyof HelpValues;
    return values[key] ?? FALLBACKS[key];
  });
}

/** "20 seconds", "2 minutes", for a duration in microseconds of model time. */
export function helpDuration(us: number): string {
  const seconds = Math.round(us / 1_000_000);
  if (seconds >= 600 && seconds % 60 === 0) return `${seconds / 60} minutes`;
  return seconds === 1 ? '1 second' : `${seconds} seconds`;
}
