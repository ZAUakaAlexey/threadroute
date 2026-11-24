// The synthetic world of the fake gateway: conversations, queues, simulated
// and human operators, teams, and a small scenario. It works in machine
// time (microseconds); durations on the wire are model time, as in the
// real gateway (machine time * speedFactor).

import type {
  ConversationRow,
  ConversationState,
  OperatorConversation,
  OperatorRow,
  Presence,
  PublicSnapshot,
  QueueRow,
  StandSettings,
  TrendPoint,
  RemoveReason,
  StaleReason,
  TeamRow,
  Totals,
} from '../src/protocol/types.ts';
import { createRng, type Rng } from './rng.ts';
import { EXTRA_LANGUAGES, LANGUAGES, companyId, conversationKey, customerId, pickLanguage, sampleText } from './synthetic.ts';

export interface WorldOptions {
  readonly seed: number;
  readonly speedFactor: number;
  /** Manual worlds change only when the control API asks (for e2e tests). */
  readonly isManual: boolean;
  readonly acceptTimeoutUs?: number;
  readonly assignmentTargetUs?: number;
  readonly firstResponseTargetUs?: number;
}

export interface WorldMessage {
  readonly id: string;
  readonly direction: 'customer' | 'company';
  readonly sourceUs: number;
  readonly text: string;
  readonly edited: boolean;
}

export interface Conversation {
  readonly key: string;
  readonly company: string;
  readonly customer: string;
  lang: string;
  session: number;
  state: ConversationState;
  teamQueue: boolean;
  priority: number;
  operatorId: string | undefined;
  offerId: string | undefined;
  acceptByUs: number | undefined;
  acceptedAtUs: number | undefined;
  queuedAtUs: number | undefined;
  firstResponseDueUs: number | undefined;
  firstResponseMissed: boolean;
  assignmentMissed: boolean;
  replied: boolean;
  readonly replyIds: Set<string>;
  customerMessages: number;
  companyMessages: number;
  readonly openedSourceUs: number;
  readonly messages: WorldMessage[];
  readonly refusedBy: Set<string>;
  simAcceptAtUs: number | undefined;
  simResolveAtUs: number | undefined;
  closeAtUs: number | undefined;
}

export interface Operator {
  readonly id: string;
  readonly kind: 'human' | 'simulated';
  languages: string[];
  capacity: number;
  presence: Presence;
  team: string | undefined;
}

export interface Team {
  readonly company: string;
  readonly members: string[];
  readonly formedAtUs: number;
}

export interface StaleState {
  readonly stale: boolean;
  readonly reason?: StaleReason;
  readonly lagUs?: number;
}

export type WorldEvent =
  | { readonly kind: 'message_added'; readonly key: string; readonly message: WorldMessage }
  | { readonly kind: 'message_removed'; readonly key: string; readonly messageId: string; readonly reason: RemoveReason }
  | { readonly kind: 'status' }
  | { readonly kind: 'rekey' };

export type ActionOutcome = { readonly outcome: 'done' | 'already_done' } | { readonly outcome: 'rejected'; readonly code: string; readonly message: string };

const DONE: ActionOutcome = { outcome: 'done' };
const HOUR_US = 3_600_000_000;
const SIM_OPERATORS = 24;
const COMPANIES = 9;
const SCENARIO_CYCLE_US = 120_000_000;
/** Model seconds of the trend model per trend point. */
const FLOW_STEP_S = 20;

export class World {
  readonly speedFactor: number;
  readonly acceptTimeoutUs: number;
  readonly assignmentTargetUs: number;
  readonly firstResponseTargetUs: number;
  readonly conversations = new Map<string, Conversation>();
  readonly operators = new Map<string, Operator>();
  readonly teams = new Map<string, Team>();
  status: StaleState = { stale: false };
  dayKey: string;
  private readonly isManual: boolean;
  private readonly rng: Rng;
  private readonly listeners = new Set<(event: WorldEvent) => void>();
  private readonly startedAtUs: number;
  private lastScenarioStep = -1;
  private counter = 0;
  private humanCounter = 0;
  private readonly addedAtUs: number[] = [];
  private readonly missedAssignmentAtUs: number[] = [];
  private readonly missedFirstResponseAtUs: number[] = [];
  private readonly waits: { readonly atUs: number; readonly waitUs: number }[] = [];
  private nextHumanOfferAtUs = 0;
  private readonly extraLanguages: string[] = [];
  /** Simulated operators put aside because the stand settings ask for fewer. */
  private readonly parked = new Set<string>();
  /**
   * Set when a change moved the share of operators in teams: teams then
   * shrink to it, or grow to it, one member per trend point.
   */
  private teamShare: { readonly share: number; readonly maxSize: number } | null = null;
  private readonly flow = { waiting: 0, served: 0, step: 0 };

  constructor(options: WorldOptions, nowUs: number) {
    this.speedFactor = options.speedFactor;
    this.acceptTimeoutUs = options.acceptTimeoutUs ?? 20_000_000;
    this.assignmentTargetUs = options.assignmentTargetUs ?? 60_000_000;
    this.firstResponseTargetUs = options.firstResponseTargetUs ?? 120_000_000;
    this.isManual = options.isManual;
    this.rng = createRng(options.seed);
    this.dayKey = `synthetic-day-key-${options.seed}-0`;
    this.startedAtUs = nowUs;
    this.populate(nowUs);
  }

  on(listener: (event: WorldEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private emit(event: WorldEvent): void {
    for (const listener of this.listeners) listener(event);
  }

  private nextId(prefix: string): string {
    this.counter += 1;
    return `${prefix}-${this.counter.toString().padStart(5, '0')}`;
  }

  /** Machine time of a model duration. */
  private machine(modelUs: number): number {
    return modelUs / this.speedFactor;
  }

  // ------------------------------------------------------------ setup

  private populate(nowUs: number): void {
    for (let i = 1; i <= SIM_OPERATORS; i += 1) {
      const langs = new Set<string>(['und', pickLanguage(this.rng), pickLanguage(this.rng)]);
      if (i % 3 === 0) langs.add('en');
      this.operators.set(`sim-op-${i.toString().padStart(4, '0')}`, {
        id: `sim-op-${i.toString().padStart(4, '0')}`,
        kind: 'simulated',
        languages: [...langs].sort(),
        capacity: this.rng.int(3, 4),
        presence: i % 8 === 0 ? 'offline' : 'online',
        team: undefined,
      });
    }
    for (const n of [1, 2]) {
      const company = companyId(n);
      const members = [`sim-op-000${n * 2 - 1}`, `sim-op-000${n * 2}`];
      this.teams.set(company, { company, members, formedAtUs: nowUs - 600_000_000 * n });
      for (const id of members) {
        const op = this.operators.get(id);
        if (op !== undefined) op.team = company;
      }
    }
    for (let i = 0; i < 36; i += 1) this.addConversation(nowUs - this.rng.int(3, 40) * 1_000_000, nowUs);
    for (const c of this.conversations.values()) {
      if (c.state === 'waiting' && this.rng.chance(0.6)) this.offerToSim(c, nowUs, true);
    }
  }

  /**
   * Adds dedicated teams for companies that open no conversations
   * (synthcompany1001 and up), so the routing of the world stays the same.
   * Members are simulated operators without a team; the sizes go 1, 2, 3,
   * or all have the given size.
   * No random numbers: the rest of a seeded world does not change.
   */
  addTeams(count: number, nowUs: number, size?: number): void {
    const free = [...this.operators.values()].filter((o) => o.kind === 'simulated' && o.team === undefined).map((o) => o.id);
    const start = [...this.teams.keys()].filter((c) => c >= companyId(1001)).length + 1001;
    for (let i = 0; i < count; i += 1) {
      const company = companyId(start + i);
      // With a size, every new team has that many members (at most the free operators).
      const length = size === undefined ? (i % 3) + 1 : Math.min(size, free.length);
      const members = Array.from({ length }, (_, k) => free[(i * 3 + k) % free.length] ?? 'sim-op-0024');
      this.teams.set(company, { company, members, formedAtUs: nowUs - 60_000_000 * (i + 1) });
    }
  }

  addConversation(openedUs: number, nowUs: number, lang = pickLanguage(this.rng)): Conversation {
    const companyN = this.rng.int(1, COMPANIES);
    const company = companyId(companyN);
    const customer = customerId(this.rng.int(1, 999_999));
    const key = conversationKey(company, customer);
    const first: WorldMessage = { id: this.nextId('synth-msg'), direction: 'customer', sourceUs: openedUs, text: sampleText(this.rng, 'customer'), edited: false };
    const conv: Conversation = {
      key,
      company,
      customer,
      lang,
      session: 1,
      state: 'waiting',
      teamQueue: this.teams.has(company),
      priority: 0,
      operatorId: undefined,
      offerId: undefined,
      acceptByUs: undefined,
      acceptedAtUs: undefined,
      queuedAtUs: Math.min(nowUs, openedUs + 2_000_000),
      firstResponseDueUs: undefined,
      firstResponseMissed: false,
      assignmentMissed: false,
      replied: false,
      replyIds: new Set(),
      customerMessages: 1,
      companyMessages: 0,
      openedSourceUs: openedUs,
      messages: [first],
      refusedBy: new Set(),
      simAcceptAtUs: undefined,
      simResolveAtUs: undefined,
      closeAtUs: undefined,
    };
    this.conversations.set(key, conv);
    this.addedAtUs.push(nowUs);
    return conv;
  }

  // ------------------------------------------------------------ operators

  createHuman(languages: readonly string[]): Operator {
    this.humanCounter += 1;
    const op: Operator = {
      id: `web-synth${this.humanCounter.toString().padStart(4, '0')}`,
      kind: 'human',
      languages: [...languages].sort(),
      capacity: 2,
      presence: 'offline',
      team: undefined,
    };
    this.operators.set(op.id, op);
    return op;
  }

  removeHuman(id: string, nowUs: number): void {
    this.setPresence(id, 'offline', nowUs);
    this.operators.delete(id);
  }

  humanCount(): number {
    return [...this.operators.values()].filter((o) => o.kind === 'human').length;
  }

  setPresence(id: string, presence: Presence, nowUs: number): void {
    const op = this.operators.get(id);
    if (op === undefined) return;
    op.presence = presence;
    if (presence === 'offline') {
      for (const c of this.conversations.values()) {
        if (c.operatorId === id) this.returnToQueue(c);
      }
    } else if (op.kind === 'human') {
      this.nextHumanOfferAtUs = nowUs + 1_500_000;
    }
  }

  load(id: string): { readonly load: number; readonly offered: number } {
    let load = 0;
    let offered = 0;
    for (const c of this.conversations.values()) {
      if (c.operatorId !== id) continue;
      if (c.state === 'offered') offered += 1;
      if (c.state === 'offered' || c.state === 'assigned') load += 1;
    }
    return { load, offered };
  }

  // ------------------------------------------------------------ routing

  private offer(c: Conversation, op: Operator, nowUs: number): void {
    c.state = 'offered';
    c.operatorId = op.id;
    c.offerId = this.nextId('of-synth');
    c.acceptByUs = Math.round(nowUs + this.machine(this.acceptTimeoutUs));
  }

  private offerToSim(c: Conversation, nowUs: number, isInitial = false): boolean {
    const candidates = [...this.operators.values()].filter(
      (o) =>
        o.kind === 'simulated' &&
        o.presence === 'online' &&
        o.languages.includes(c.lang) &&
        !c.refusedBy.has(o.id) &&
        (c.teamQueue ? o.team === c.company : o.team === undefined) &&
        this.load(o.id).load < o.capacity,
    );
    if (candidates.length === 0) return false;
    candidates.sort((a, b) => this.load(a.id).load - this.load(b.id).load || a.id.localeCompare(b.id));
    const op = candidates[0];
    if (op === undefined) return false;
    this.offer(c, op, nowUs);
    if (isInitial && this.rng.chance(0.6)) {
      this.accept(c, nowUs);
      return true;
    }
    // About 5 percent of offers are left to time out.
    c.simAcceptAtUs = this.rng.chance(0.05) ? undefined : nowUs + this.rng.int(800, 3500) * 1000;
    return true;
  }

  /** Offers a waiting conversation (or a new one) to an online human operator. */
  offerToHuman(op: Operator, nowUs: number): Conversation | undefined {
    if (op.presence !== 'online' || this.load(op.id).load >= op.capacity) return undefined;
    const waiting = [...this.conversations.values()]
      .filter((c) => c.state === 'waiting' && op.languages.includes(c.lang) && !c.refusedBy.has(op.id) && !c.teamQueue)
      .sort((a, b) => (a.queuedAtUs ?? 0) - (b.queuedAtUs ?? 0));
    const lang = op.languages.includes('en') ? 'en' : (op.languages[0] ?? 'und');
    const conv = waiting[0] ?? this.addConversation(nowUs - this.rng.int(3, 20) * 1_000_000, nowUs, lang);
    if (conv.teamQueue) conv.teamQueue = false;
    this.offer(conv, op, nowUs);
    return conv;
  }

  private accept(c: Conversation, nowUs: number): void {
    c.state = 'assigned';
    c.acceptedAtUs = nowUs;
    c.acceptByUs = undefined;
    c.simAcceptAtUs = undefined;
    c.firstResponseDueUs = Math.round((c.queuedAtUs ?? nowUs) + this.machine(this.firstResponseTargetUs));
    this.waits.push({ atUs: nowUs, waitUs: Math.round((nowUs - (c.queuedAtUs ?? nowUs)) * this.speedFactor) });
    const op = c.operatorId === undefined ? undefined : this.operators.get(c.operatorId);
    if (op?.kind === 'simulated') c.simResolveAtUs = nowUs + this.rng.int(8, 40) * 1_000_000;
  }

  private returnToQueue(c: Conversation): void {
    if (c.operatorId !== undefined && c.state === 'offered') c.refusedBy.add(c.operatorId);
    c.state = 'waiting';
    c.operatorId = undefined;
    c.offerId = undefined;
    c.acceptByUs = undefined;
    c.acceptedAtUs = undefined;
    c.simAcceptAtUs = undefined;
    c.simResolveAtUs = undefined;
  }

  private resolve(c: Conversation, nowUs: number): void {
    c.state = 'resolved';
    c.operatorId = undefined;
    c.offerId = undefined;
    c.acceptedAtUs = undefined;
    c.firstResponseDueUs = undefined;
    c.queuedAtUs = undefined;
    c.simResolveAtUs = undefined;
    c.closeAtUs = nowUs + 6_000_000;
  }

  // ------------------------------------------------------------ actions

  action(operatorId: string, action: string, offerId: string | undefined, requestId: string, nowUs: number): ActionOutcome {
    const op = this.operators.get(operatorId);
    if (op === undefined) return reject('operator_not_found', 'the operator is not known');
    if (action === 'online' || action === 'offline') {
      if (op.presence === action) return { outcome: 'already_done' };
      this.setPresence(op.id, action, nowUs);
      return DONE;
    }
    const c = [...this.conversations.values()].find((x) => x.offerId === offerId && x.operatorId === op.id);
    if (c === undefined) return reject('unknown_offer', 'the offer is not live for this operator');
    switch (action) {
      case 'accept':
        if (c.state === 'assigned') return { outcome: 'already_done' };
        if ((c.acceptByUs ?? 0) <= nowUs) return reject('offer_expired', 'accept: the offer is expired');
        this.accept(c, nowUs);
        return DONE;
      case 'refuse':
        if (c.state === 'assigned') return reject('offer_accepted', 'refuse: the offer was accepted');
        this.returnToQueue(c);
        return DONE;
      case 'reply':
        if (c.state !== 'assigned') return reject('offer_not_accepted', 'reply: the offer is not accepted');
        if (c.replyIds.has(requestId)) return { outcome: 'already_done' };
        c.replyIds.add(requestId);
        c.replied = true;
        c.firstResponseDueUs = undefined;
        return DONE;
      case 'resolve':
        if (c.state !== 'assigned') return reject('offer_not_accepted', 'resolve: the offer is not accepted');
        this.resolve(c, nowUs);
        return DONE;
      default:
        return reject('invalid_request', 'unknown action');
    }
  }

  setLanguages(operatorId: string, languages: readonly string[]): void {
    const op = this.operators.get(operatorId);
    if (op !== undefined) op.languages = [...languages].sort();
  }

  // ------------------------------------------------------------ scenario steps

  addCustomerMessage(c: Conversation, nowUs: number): WorldMessage {
    const message: WorldMessage = { id: this.nextId('synth-msg'), direction: 'customer', sourceUs: nowUs, text: sampleText(this.rng, 'customer'), edited: false };
    c.messages.push(message);
    c.customerMessages += 1;
    this.addedAtUs.push(nowUs);
    this.emit({ kind: 'message_added', key: c.key, message });
    return message;
  }

  removeMessage(c: Conversation, messageId: string, reason: RemoveReason): boolean {
    const index = c.messages.findIndex((m) => m.id === messageId);
    if (index < 0) return false;
    c.messages.splice(index, 1);
    this.emit({ kind: 'message_removed', key: c.key, messageId, reason });
    return true;
  }

  setStale(status: StaleState): void {
    this.status = status;
    this.emit({ kind: 'status' });
  }

  rekey(): void {
    const [base, n] = [this.dayKey.slice(0, this.dayKey.lastIndexOf('-')), Number(this.dayKey.split('-').at(-1))];
    this.dayKey = `${base}-${n + 1}`;
    this.emit({ kind: 'rekey' });
  }

  heldByHumans(): Conversation[] {
    return [...this.conversations.values()].filter(
      (c) => c.state === 'assigned' && c.operatorId !== undefined && this.operators.get(c.operatorId)?.kind === 'human',
    );
  }

  // ------------------------------------------------------------ time

  step(nowUs: number): void {
    for (const c of this.conversations.values()) this.stepConversation(c, nowUs);
    if (this.isManual) return;
    // The scenario runs while stale too: it is what ends the stale spell.
    this.stepScenario(nowUs);
    if (this.status.stale) return;
    this.stepArrivals(nowUs);
    this.stepHumans(nowUs);
  }

  private stepConversation(c: Conversation, nowUs: number): void {
    if (c.state === 'offered' && (c.acceptByUs ?? 0) <= nowUs) this.returnToQueue(c);
    const isQueued = c.state === 'waiting' || c.state === 'offered';
    if (isQueued && !c.assignmentMissed && (nowUs - (c.queuedAtUs ?? nowUs)) * this.speedFactor > this.assignmentTargetUs) {
      c.assignmentMissed = true;
      c.priority += 1;
      this.missedAssignmentAtUs.push(nowUs);
    }
    if (c.state === 'assigned' && c.firstResponseDueUs !== undefined && c.firstResponseDueUs <= nowUs && !c.replied) {
      c.firstResponseMissed = true;
      c.firstResponseDueUs = undefined;
      this.missedFirstResponseAtUs.push(nowUs);
    }
    if (c.closeAtUs !== undefined && c.closeAtUs <= nowUs) {
      this.conversations.delete(c.key);
      return;
    }
    if (this.isManual || this.status.stale) return;
    if (c.state === 'offered' && c.simAcceptAtUs !== undefined && c.simAcceptAtUs <= nowUs) this.accept(c, nowUs);
    if (c.state === 'assigned' && c.simResolveAtUs !== undefined && c.simResolveAtUs <= nowUs) this.resolve(c, nowUs);
    if (c.state === 'waiting' && this.rng.chance(0.25)) this.offerToSim(c, nowUs);
  }

  private stepArrivals(nowUs: number): void {
    if (this.conversations.size < 60 && this.rng.chance(0.35)) this.addConversation(nowUs - this.rng.int(1, 4) * 1_000_000, nowUs);
    if (this.rng.chance(0.08)) {
      const assigned = [...this.conversations.values()].filter((c) => c.state === 'assigned');
      if (assigned.length > 0) this.addCustomerMessage(this.rng.pick(assigned), nowUs);
    }
  }

  private stepHumans(nowUs: number): void {
    if (nowUs < this.nextHumanOfferAtUs) return;
    for (const op of this.operators.values()) {
      if (op.kind === 'human') this.offerToHuman(op, nowUs);
    }
    this.nextHumanOfferAtUs = nowUs + 9_000_000;
  }

  /** The repeating scenario of the dev mode: a message, a removal, an operator away, a stale spell. */
  private stepScenario(nowUs: number): void {
    const step = Math.floor(((nowUs - this.startedAtUs) % SCENARIO_CYCLE_US) / 5_000_000);
    if (step === this.lastScenarioStep) return;
    this.lastScenarioStep = step;
    const held = this.heldByHumans();
    const target = held[0];
    switch (step) {
      case 4:
        if (target !== undefined) this.addCustomerMessage(target, nowUs);
        break;
      case 7: {
        const message = target?.messages.at(-1);
        if (target !== undefined && message !== undefined) this.removeMessage(target, message.id, 'deleted');
        break;
      }
      case 10:
        this.setPresence('sim-op-0005', 'offline', nowUs);
        break;
      case 15:
        this.setPresence('sim-op-0005', 'online', nowUs);
        break;
      case 18:
        this.setStale({ stale: true, reason: 'source_unavailable', lagUs: 4_000_000 });
        break;
      case 19:
        this.setStale({ stale: false });
        break;
    }
  }

  // ------------------------------------------------------------ stand settings

  simCount(): number {
    return [...this.operators.values()].filter((o) => o.kind === 'simulated' && !this.parked.has(o.id)).length;
  }

  /**
   * Applies stand settings to the world, step by step like the simulator
   * of decision 0014: fewer operators go offline from the highest number,
   * more come back or are created; a new capacity applies to every
   * simulated operator; a lower team share makes teams shrink one member per
   * trend point. No random numbers, so a seeded world stays the same.
   */
  applyStand(settings: StandSettings, previous: StandSettings, nowUs: number): void {
    const sim = settings.sim;
    if (sim !== undefined) {
      this.setSimCount(sim.operators, sim.capacity, nowUs);
      if (sim.capacity !== previous.sim?.capacity) {
        for (const op of this.operators.values()) if (op.kind === 'simulated') op.capacity = sim.capacity;
      }
    }
    const { max_share: share, max_size: maxSize } = settings.teams;
    if (share !== previous.teams.max_share) this.teamShare = { share, maxSize };
    // Team sizes act at the next team check of the router: at once here.
    for (const team of [...this.teams.values()]) {
      while (team.members.length > maxSize) {
        const op = this.operators.get(team.members.at(-1) ?? '');
        if (op === undefined) team.members.pop();
        else this.leaveTeam(op);
      }
    }
  }

  /**
   * The active simulated operators stay numbers 1 to N: the highest numbers
   * are put aside first, and the lowest put-aside numbers come back first.
   * A new operator gets its language from its number only.
   */
  private setSimCount(target: number, capacity: number, nowUs: number): void {
    const sims = [...this.operators.values()].filter((o) => o.kind === 'simulated').sort((a, b) => a.id.localeCompare(b.id));
    const active = sims.filter((o) => !this.parked.has(o.id));
    for (const op of active.slice(target).reverse()) {
      this.setPresence(op.id, 'offline', nowUs);
      this.leaveTeam(op);
      this.parked.add(op.id);
    }
    let missing = target - active.length;
    for (const op of sims.filter((o) => this.parked.has(o.id))) {
      if (missing <= 0) break;
      this.parked.delete(op.id);
      this.setPresence(op.id, 'online', nowUs);
      missing -= 1;
    }
    for (let n = sims.length + 1; missing > 0; n += 1, missing -= 1) {
      const id = `sim-op-${n.toString().padStart(4, '0')}`;
      const langs = new Set(['und', LANGUAGES[n % LANGUAGES.length] ?? 'en']);
      this.operators.set(id, { id, kind: 'simulated', languages: [...langs].sort(), capacity, presence: 'online', team: undefined });
    }
  }

  private leaveTeam(op: Operator): void {
    if (op.team === undefined) return;
    const team = this.teams.get(op.team);
    op.team = undefined;
    if (team === undefined) return;
    team.members.splice(team.members.indexOf(op.id), 1);
    if (team.members.length === 0) this.teams.delete(team.company);
  }

  /**
   * One step toward the team share: a member leaves the largest team while
   * the online team members are more than the share allows, or a free
   * simulated operator joins the smallest team while they are fewer. The
   * share is of the online operators, as in the router.
   */
  private moveTeamsToShare(): void {
    if (this.teamShare === null) return;
    const ops = [...this.operators.values()];
    const limit = Math.floor(this.teamShare.share * ops.filter((o) => o.presence === 'online').length);
    const teams = [...this.teams.values()].sort((a, b) => b.members.length - a.members.length || a.company.localeCompare(b.company));
    const inTeams = this.onlineInTeams();
    if (inTeams > limit) {
      const members = teams[0]?.members ?? [];
      const op = this.operators.get([...members].reverse().find((id) => this.operators.get(id)?.presence === 'online') ?? members.at(-1) ?? '');
      if (op !== undefined) this.leaveTeam(op);
      else teams[0]?.members.pop();
      return;
    }
    const smallest = teams.filter((t) => t.members.length < (this.teamShare?.maxSize ?? 0)).at(-1);
    const free = ops.find((o) => o.kind === 'simulated' && o.presence === 'online' && o.team === undefined && !this.parked.has(o.id));
    if (inTeams >= limit || smallest === undefined || free === undefined) {
      this.teamShare = null;
      return;
    }
    smallest.members.push(free.id);
    free.team = smallest.company;
  }

  /**
   * Online operators who are members of a team, each counted once (the
   * "teams" control can put one operator in more than one team). This is
   * the number the team share limits, so it is never above the online
   * operators.
   */
  private onlineInTeams(): number {
    const members = new Set([...this.teams.values()].flatMap((t) => t.members));
    return [...members].filter((id) => this.operators.get(id)?.presence === 'online').length;
  }

  /**
   * One trend point. Teams and operators are the world's own counts; the
   * flow numbers come from a small queue model driven by the stand
   * settings, so a change of operators or capacity shows on the strip even
   * in the manual mode of the tests, where no conversation moves. Each
   * point moves the model by FLOW_STEP_S seconds of model time.
   */
  trendPoint(settings: StandSettings, nowUs: number): TrendPoint {
    this.moveTeamsToShare();
    const point = this.flowPoint(settings, this.flow.step, this.flow);
    this.flow.step += 1;
    return { ...point, sampled_at_us: Math.round(nowUs), ...this.teamCounts() };
  }

  /** History for a new gateway: count points of the same model, ending before nowUs. */
  trendHistory(settings: StandSettings, count: number, intervalMs: number, nowUs: number): TrendPoint[] {
    const state = { waiting: 0, served: 0 };
    const counts = this.teamCounts();
    const points: TrendPoint[] = [];
    for (let i = -count; i < 0; i += 1) {
      points.push({ ...this.flowPoint(settings, i, state), sampled_at_us: Math.round(nowUs + i * intervalMs * 1000), ...counts });
    }
    this.flow.waiting = state.waiting;
    this.flow.served = state.served;
    this.flow.step = 0;
    return points;
  }

  private teamCounts(): Pick<TrendPoint, 'teams' | 'operators_in_teams' | 'operators_online'> {
    return {
      teams: this.teams.size,
      operators_in_teams: this.onlineInTeams(),
      operators_online: [...this.operators.values()].filter((o) => o.presence === 'online').length,
    };
  }

  private flowPoint(
    settings: StandSettings,
    step: number,
    state: { waiting: number; served: number },
  ): Omit<TrendPoint, 'sampled_at_us' | 'teams' | 'operators_in_teams' | 'operators_online'> {
    const sim = settings.sim;
    const online = [...this.operators.values()].filter((o) => o.kind === 'simulated' && o.presence === 'online').length;
    const capacity = sim?.capacity ?? 4;
    const holdS = sim === undefined ? 225 : (sim.handling_median_us + sim.resolve_median_us) / 1_000_000;
    // Conversations the operators can finish per minute, and a demand that
    // waves around the capacity of the starting world (21 online, capacity 4).
    const servePerMin = (online * capacity * 60) / holdS;
    const demandPerMin = 23 * (1 + 0.25 * Math.sin((2 * Math.PI * step) / 90));
    const dtMin = FLOW_STEP_S / 60;
    const served = Math.min(servePerMin * dtMin, state.waiting + demandPerMin * dtMin);
    state.waiting = Math.max(0, state.waiting + demandPerMin * dtMin - served);
    state.served = served / dtMin;
    const resolved = Math.round(state.served);
    return {
      waiting: Math.round(state.waiting),
      longest_wait_us: resolved === 0 ? 0 : Math.round(((state.waiting / Math.max(1, state.served)) * 60 + 5) * 1_000_000),
      resolved_last_minute: resolved,
    };
  }

  // ------------------------------------------------------------ views

  operatorRow(op: Operator): OperatorRow {
    const { load, offered } = this.load(op.id);
    return {
      id: op.id,
      kind: op.kind,
      languages: op.languages,
      capacity: op.capacity,
      presence: op.presence,
      load,
      offered,
      ...(op.team === undefined ? {} : { team: op.team }),
    };
  }

  operatorConversation(c: Conversation): OperatorConversation {
    return {
      id: c.key,
      company: c.company,
      customer: c.customer,
      lang: c.lang,
      session: c.session,
      state: c.state === 'offered' ? 'offered' : 'assigned',
      offer_id: c.offerId ?? 'of-none',
      ...(c.state === 'offered' ? { accept_by_us: c.acceptByUs ?? 1 } : { accepted_at_us: c.acceptedAtUs ?? 1 }),
      queued_at_us: c.queuedAtUs ?? c.openedSourceUs,
      ...(c.firstResponseDueUs === undefined ? {} : { first_response_due_us: c.firstResponseDueUs }),
      first_response_missed: c.firstResponseMissed,
      replied: c.replied,
      priority: c.priority,
      customer_messages: c.customerMessages,
      company_messages: c.companyMessages,
      ...(c.messages.length === 0 ? {} : { last_message_source_us: c.messages.at(-1)?.sourceUs ?? c.openedSourceUs }),
    };
  }

  conversationRow(c: Conversation): ConversationRow {
    const isHeld = c.state === 'offered' || c.state === 'assigned';
    const isQueued = isHeld || c.state === 'waiting';
    return {
      id: c.key,
      company: c.company,
      customer: c.customer,
      lang: c.lang,
      session: c.session,
      state: c.state,
      team_queue: c.teamQueue,
      priority: c.priority,
      ...(isHeld && c.operatorId !== undefined ? { operator_id: c.operatorId } : {}),
      ...(isQueued && c.queuedAtUs !== undefined ? { queued_at_us: c.queuedAtUs } : {}),
      assignment_missed: c.assignmentMissed,
      first_response_missed: c.firstResponseMissed,
      customer_messages: c.customerMessages,
      company_messages: c.companyMessages,
      opened_source_us: c.openedSourceUs,
    };
  }

  /**
   * Adds empty queues for more languages, for the layout of the queues
   * panel. Conversations keep their languages, so the rest of a seeded world
   * does not change. A reset forgets them with the world.
   */
  addLanguages(count: number): void {
    const next = EXTRA_LANGUAGES.filter((lang) => !this.extraLanguages.includes(lang)).slice(0, count);
    this.extraLanguages.push(...next);
  }

  queueRows(): QueueRow[] {
    return [...LANGUAGES, ...this.extraLanguages].map((lang) => {
      const queued = [...this.conversations.values()].filter((c) => c.lang === lang && (c.state === 'waiting' || c.state === 'offered'));
      const waiting = queued.filter((c) => c.state === 'waiting');
      const oldest = Math.min(...queued.map((c) => c.queuedAtUs ?? Number.MAX_SAFE_INTEGER));
      return {
        id: lang,
        waiting: waiting.length,
        team_waiting: waiting.filter((c) => c.teamQueue).length,
        offered: queued.length - waiting.length,
        ...(queued.length > 0 ? { oldest_queued_at_us: oldest } : {}),
        missed_assignment: queued.filter((c) => c.assignmentMissed).length,
      };
    });
  }

  teamRows(): TeamRow[] {
    return [...this.teams.values()].map((t) => ({
      id: t.company,
      members: t.members,
      active_load: [...this.conversations.values()].filter((c) => c.company === t.company && c.state !== 'resolved').length,
      formed_at_us: t.formedAtUs,
    }));
  }

  totals(nowUs: number): Totals {
    const counts = { open: 0, waiting: 0, offered: 0, assigned: 0, resolved: 0 };
    for (const c of this.conversations.values()) counts[c.state] += 1;
    const online = [...this.operators.values()].filter((o) => o.presence === 'online');
    const sinceHour = nowUs - HOUR_US;
    const waits = this.waits.filter((w) => w.atUs > sinceHour).map((w) => w.waitUs).sort((a, b) => a - b);
    return {
      messages_per_second: Math.round((this.addedAtUs.filter((t) => t > nowUs - 10_000_000).length / 10) * 100) / 100,
      conversations: counts,
      companies_active: new Set([...this.conversations.values()].map((c) => c.company)).size,
      operators_online: online.length,
      humans_online: online.filter((o) => o.kind === 'human').length,
      slots_capacity: online.reduce((sum, o) => sum + o.capacity, 0),
      slots_used: online.reduce((sum, o) => sum + this.load(o.id).load, 0),
      missed_assignment: this.missedAssignmentAtUs.filter((t) => t > sinceHour).length,
      missed_first_response: this.missedFirstResponseAtUs.filter((t) => t > sinceHour).length,
      wait_p50_us: quantile(waits, 0.5),
      wait_p90_us: quantile(waits, 0.9),
    };
  }

  publicSnapshot(nowUs: number): PublicSnapshot {
    return {
      version: 1,
      generated_at_us: nowUs,
      stale: this.status.stale,
      speed_factor: this.speedFactor,
      totals: this.totals(nowUs),
      queues: this.queueRows()
        .map((q) => ({
          lang: q.id,
          waiting: q.waiting,
          offered: q.offered,
          oldest_wait_us: q.oldest_queued_at_us === undefined ? 0 : Math.round((nowUs - q.oldest_queued_at_us) * this.speedFactor),
          missed_assignment: q.missed_assignment,
        }))
        .sort((a, b) => a.lang.localeCompare(b.lang)),
    };
  }
}

function quantile(sorted: readonly number[], q: number): number {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? 0;
}

function reject(code: string, message: string): ActionOutcome {
  return { outcome: 'rejected', code, message };
}
