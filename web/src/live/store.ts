// The live view that the gateway pushes (decision 0013, point 3A). One
// store per connection. State messages replace keys; nothing is merged.
// Post text lives only here, in memory: no middleware persists this store.

import { createStore, type StoreApi } from 'zustand/vanilla';

import type { ConnectionState } from '../protocol/client.ts';
import type {
  Changes,
  ConversationRow,
  Direction,
  MessageRemoved,
  Messages,
  OperatorConversation,
  OperatorRow,
  QueueRow,
  RemoveReason,
  ServerMessage,
  State,
  Status,
  TeamRow,
  Totals,
  Welcome,
} from '../protocol/types.ts';

export interface ChatMessage {
  readonly id: string;
  readonly direction: Direction;
  readonly sourceUs: number;
  readonly length: number;
  readonly edited: boolean;
  /** Full level only, and never after a removal. */
  readonly text?: string;
  readonly removedReason?: RemoveReason;
}

export type NoticeTone = 'info' | 'error';

export interface Notice {
  readonly id: number;
  readonly tone: NoticeTone;
  readonly text: string;
}

export interface LiveData {
  readonly connection: ConnectionState;
  readonly welcome: Welcome | null;
  /** server clock minus browser clock, in microseconds. */
  readonly clockOffsetUs: number;
  readonly status: Status | null;
  /** Server time at which the timers stopped, or null while they run. */
  readonly frozenAtUs: number | null;
  readonly seq: number;
  readonly hasSnapshot: boolean;
  readonly me: OperatorRow | null;
  readonly myConversations: ReadonlyMap<string, OperatorConversation>;
  readonly conversations: ReadonlyMap<string, ConversationRow>;
  readonly operators: ReadonlyMap<string, OperatorRow>;
  readonly queues: ReadonlyMap<string, QueueRow>;
  readonly teams: ReadonlyMap<string, TeamRow>;
  readonly totals: Totals | null;
  /** Conversations whose messages are shown. Messages of others are dropped. */
  readonly watched: ReadonlySet<string>;
  readonly messages: ReadonlyMap<string, readonly ChatMessage[]>;
  readonly selectedId: string | null;
  /** Keys of requests in flight, for example "accept:of-101". */
  readonly busy: ReadonlySet<string>;
  readonly notice: Notice | null;
}

export interface LiveActions {
  readonly applyMessage: (message: ServerMessage, nowMs?: number) => void;
  readonly setConnection: (connection: ConnectionState, nowMs?: number) => void;
  readonly select: (id: string | null) => void;
  readonly setWatched: (id: string, isWatched: boolean) => void;
  readonly setBusy: (key: string, isBusy: boolean) => void;
  readonly showNotice: (tone: NoticeTone, text: string) => void;
  readonly clearNotice: () => void;
}

export type LiveState = LiveData & LiveActions;
export type LiveStore = StoreApi<LiveState>;

const EMPTY_MAP: ReadonlyMap<string, never> = new Map<string, never>();
const EMPTY_SET: ReadonlySet<string> = new Set<string>();

export function initialLiveData(): LiveData {
  return {
    connection: { phase: 'idle', attempt: 0 },
    welcome: null,
    clockOffsetUs: 0,
    status: null,
    frozenAtUs: null,
    seq: 0,
    hasSnapshot: false,
    me: null,
    myConversations: EMPTY_MAP,
    conversations: EMPTY_MAP,
    operators: EMPTY_MAP,
    queues: EMPTY_MAP,
    teams: EMPTY_MAP,
    totals: null,
    watched: EMPTY_SET,
    messages: EMPTY_MAP,
    selectedId: null,
    busy: EMPTY_SET,
    notice: null,
  };
}

export function createLiveStore(): LiveStore {
  let noticeCounter = 0;
  return createStore<LiveState>()((set) => ({
    ...initialLiveData(),
    applyMessage: (message, nowMs = Date.now()) => set((s) => withFreeze(s, reduceMessage(s, message, nowMs), nowMs)),
    setConnection: (connection, nowMs = Date.now()) =>
      set((s) => withFreeze(s, { ...s, ...connectionChange(s, connection) }, nowMs)),
    select: (id) => set((s) => ({ selectedId: id, messages: id === null ? keepOnly(s.messages, s.watched) : s.messages })),
    setWatched: (id, isWatched) =>
      set((s) => {
        const watched = toggled(s.watched, id, isWatched);
        return { watched, messages: keepOnly(s.messages, watched) };
      }),
    setBusy: (key, isBusy) => set((s) => ({ busy: toggled(s.busy, key, isBusy) })),
    showNotice: (tone, text) => {
      noticeCounter += 1;
      set({ notice: { id: noticeCounter, tone, text } });
    },
    clearNotice: () => set({ notice: null }),
  }));
}

/** True while the data is current: connected and not stale. */
export function isLive(s: Pick<LiveData, 'connection' | 'status'>): boolean {
  return s.connection.phase === 'open' && s.status?.stale !== true;
}

export function serverNowUs(s: Pick<LiveData, 'clockOffsetUs'>, nowMs: number): number {
  return nowMs * 1000 + s.clockOffsetUs;
}

// ---------------------------------------------------------------- reducers

function reduceMessage(s: LiveState, message: ServerMessage, nowMs: number): LiveState {
  switch (message.type) {
    case 'welcome':
      return { ...s, welcome: message, clockOffsetUs: message.server_time_us - nowMs * 1000 };
    case 'status':
      return { ...s, status: message };
    case 'state':
      return reduceState(s, message, nowMs);
    case 'messages':
      return reduceMessages(s, message);
    case 'message_removed':
      return reduceRemoved(s, message);
    case 'result':
      return s;
  }
}

function connectionChange(s: LiveState, connection: ConnectionState): Partial<LiveData> {
  if (connection.phase === 'open' || connection.phase === s.connection.phase) return { connection };
  // A lost connection ends every watch; a new one starts with a snapshot.
  return { connection, watched: EMPTY_SET, messages: EMPTY_MAP, busy: EMPTY_SET };
}

function reduceState(s: LiveState, m: State, nowMs: number): LiveState {
  // A state is built before it is sent, so every estimate is a lower bound
  // of the true offset. The largest one is the closest.
  const clockOffsetUs = Math.max(s.clockOffsetUs, m.server_time_us - nowMs * 1000);
  if (m.snapshot) {
    const next: LiveState = {
      ...s,
      clockOffsetUs,
      seq: m.seq,
      hasSnapshot: true,
      me: m.me ?? null,
      myConversations: applyChanges(EMPTY_MAP, m.my_conversations),
      conversations: applyChanges(EMPTY_MAP, m.conversations),
      operators: applyChanges(EMPTY_MAP, m.operators),
      queues: applyChanges(EMPTY_MAP, m.queues),
      teams: applyChanges(EMPTY_MAP, m.teams),
      totals: m.totals ?? null,
      // A snapshot ends every watch (docs/gateway-protocol.md, "watch").
      watched: EMPTY_SET,
      messages: EMPTY_MAP,
    };
    return { ...next, selectedId: isInView(next, s.selectedId) ? s.selectedId : null };
  }
  const next: LiveState = {
    ...s,
    clockOffsetUs,
    seq: m.seq,
    me: m.me ?? s.me,
    myConversations: applyChanges(s.myConversations, m.my_conversations),
    conversations: applyChanges(s.conversations, m.conversations),
    operators: applyChanges(s.operators, m.operators),
    queues: applyChanges(s.queues, m.queues),
    teams: applyChanges(s.teams, m.teams),
    totals: m.totals ?? s.totals,
  };
  if (isInView(next, s.selectedId)) return next;
  return { ...next, selectedId: null, messages: keepOnly(next.messages, next.watched, s.selectedId) };
}

function reduceMessages(s: LiveState, m: Messages): LiveState {
  if (!s.watched.has(m.conversation_id)) return s;
  const incoming = m.messages.map(toChat);
  const base = m.snapshot ? [] : (s.messages.get(m.conversation_id) ?? []);
  const byId = new Map(base.map((x) => [x.id, x]));
  for (const x of incoming) byId.set(x.id, x);
  const list = [...byId.values()].sort((a, b) => a.sourceUs - b.sourceUs || a.id.localeCompare(b.id));
  return { ...s, messages: withEntry(s.messages, m.conversation_id, list) };
}

function reduceRemoved(s: LiveState, m: MessageRemoved): LiveState {
  const list = s.messages.get(m.conversation_id);
  if (!list?.some((x) => x.id === m.message_id)) return s;
  const next = list.map((x) => (x.id === m.message_id ? removedChat(x, m.reason) : x));
  return { ...s, messages: withEntry(s.messages, m.conversation_id, next) };
}

/** Sets frozenAtUs when the timers stop, and clears it when they run again. */
function withFreeze(prev: LiveState, next: LiveState, nowMs: number): LiveState {
  const wasLive = isLive(prev) && prev.frozenAtUs === null;
  const isNowLive = isLive(next);
  if (isNowLive) return next.frozenAtUs === null ? next : { ...next, frozenAtUs: null };
  if (!wasLive && next.frozenAtUs !== null) return next;
  return { ...next, frozenAtUs: serverNowUs(prev, nowMs) };
}

// ---------------------------------------------------------------- helpers

function toChat(m: Messages['messages'][number]): ChatMessage {
  const base = { id: m.id, direction: m.direction, sourceUs: m.source_us, length: m.length, edited: m.edited };
  return m.text === undefined ? base : { ...base, text: m.text };
}

/** A copy without the text: the text must leave memory (decision 0013). */
function removedChat(x: ChatMessage, reason: RemoveReason): ChatMessage {
  return { id: x.id, direction: x.direction, sourceUs: x.sourceUs, length: x.length, edited: x.edited, removedReason: reason };
}

function applyChanges<T extends { readonly id: string }>(
  base: ReadonlyMap<string, T>,
  changes: Changes<T> | undefined,
): ReadonlyMap<string, T> {
  if (changes === undefined) return base;
  const next = new Map(base);
  for (const id of changes.remove ?? []) next.delete(id);
  for (const row of changes.set ?? []) next.set(row.id, row);
  return next;
}

function isInView(s: LiveData, id: string | null): boolean {
  if (id === null) return false;
  if (s.welcome?.view === 'supervisor') return s.conversations.has(id);
  return s.myConversations.get(id)?.state === 'assigned';
}

function withEntry<V>(map: ReadonlyMap<string, V>, key: string, value: V): ReadonlyMap<string, V> {
  const next = new Map(map);
  next.set(key, value);
  return next;
}

function toggled(set: ReadonlySet<string>, key: string, isOn: boolean): ReadonlySet<string> {
  if (set.has(key) === isOn) return set;
  const next = new Set(set);
  if (isOn) next.add(key);
  else next.delete(key);
  return next;
}

/** Drops messages of conversations that are not watched, and of `drop`. */
function keepOnly(
  messages: ReadonlyMap<string, readonly ChatMessage[]>,
  watched: ReadonlySet<string>,
  drop: string | null = null,
): ReadonlyMap<string, readonly ChatMessage[]> {
  const keys = [...messages.keys()].filter((k) => !watched.has(k) || k === drop);
  if (keys.length === 0) return messages;
  const next = new Map(messages);
  for (const k of keys) next.delete(k);
  return next;
}
