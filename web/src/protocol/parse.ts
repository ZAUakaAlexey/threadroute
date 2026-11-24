// Run-time validation of every message of the protocol. A parser returns a
// new object with the known fields only, so an unknown field (allowed by the
// protocol) never reaches the store.

import {
  ACTIONS,
  CONVERSATION_STATES,
  DIRECTIONS,
  HELD_STATES,
  KNOWN_RESULT_CODES,
  LEVELS,
  OPERATOR_KINDS,
  OUTCOMES,
  PRESENCES,
  REMOVE_REASONS,
  STALE_REASONS,
  VIEWS,
  type Action,
  type Changes,
  type ClientMessage,
  type ConversationRow,
  type Hello,
  type Level,
  type Limits,
  type MessageRemoved,
  type Messages,
  type OperatorConversation,
  type OperatorRow,
  type PublicQueue,
  type PublicSnapshot,
  type QueueRow,
  type Result,
  type ServerMessage,
  type State,
  type StateCounts,
  type Status,
  type TeamRow,
  type TextMessage,
  type Totals,
  type Unwatch,
  type Watch,
  type Welcome,
} from './types.ts';
import {
  ProtocolError,
  asRecord,
  compact,
  idItem,
  langItem,
  readArray,
  readBool,
  readEnum,
  readInt,
  readLang,
  readNumber,
  readObject,
  readOptArray,
  readOptEnum,
  readOptInt,
  readOptObject,
  readOptRoutingId,
  readOptString,
  readOptText,
  readRoutingId,
  readString,
  routingIdItem,
  type Rec,
} from './read.ts';

export { ProtocolError } from './read.ts';

const MAX_TOKEN_BYTES = 2048;
const MAX_RESUME_TOKEN = 256;
const MAX_LANGUAGES = 32;

export interface ParseOptions {
  /**
   * The level of the connection, once the welcome arrived. At the demo
   * level a message with text is refused, so text that the server should
   * not have sent never reaches the store.
   */
  readonly level?: Level;
}

export type ServerParse =
  | { readonly kind: 'message'; readonly message: ServerMessage }
  | { readonly kind: 'unknown'; readonly type: string };

/** Parses one server frame. Unknown types are reported, not thrown. */
export function parseServerMessage(frame: string, options: ParseOptions = {}): ServerParse {
  const o = asRecord(parseJson(frame), '');
  const type = o.type;
  if (typeof type !== 'string' || type === '') throw new ProtocolError('type', 'is not set');
  const reader = SERVER_READERS[type];
  if (reader === undefined) return { kind: 'unknown', type };
  return { kind: 'message', message: reader(o, options) };
}

export function parseClientMessage(frame: string): ClientMessage {
  const o = asRecord(parseJson(frame), '');
  switch (o.type) {
    case 'hello':
      return readHello(o);
    case 'watch':
      return readWatchLike(o, 'watch');
    case 'unwatch':
      return readWatchLike(o, 'unwatch');
    case 'action':
      return readAction(o);
    default:
      throw new ProtocolError('type', 'is not a known client type');
  }
}

export function parsePublicSnapshot(json: unknown): PublicSnapshot {
  const o = asRecord(json, '');
  const queues = readArray(o, 'queues', '', (v, p) => readPublicQueue(asRecord(v, p), p));
  return {
    version: readInt(o, 'version', '', 1),
    generated_at_us: readInt(o, 'generated_at_us', '', 1),
    stale: readBool(o, 'stale', ''),
    speed_factor: readNumber(o, 'speed_factor', '', true),
    totals: readObject(o, 'totals', '', readTotals),
    queues,
  };
}

function parseJson(frame: string): unknown {
  try {
    return JSON.parse(frame) as unknown;
  } catch {
    throw new ProtocolError('', 'is not valid JSON');
  }
}

// ---------------------------------------------------------------- client

function readLanguages(o: Rec, key: string, path: string): string[] {
  const langs = readArray(o, key, path, langItem);
  if (langs.length === 0 || langs.length > MAX_LANGUAGES) {
    throw new ProtocolError(key, `needs 1 to ${MAX_LANGUAGES} values`);
  }
  if (new Set(langs).size !== langs.length) throw new ProtocolError(key, 'has a value twice');
  return langs;
}

function readHello(o: Rec): Hello {
  const view = readEnum(o, 'view', '', VIEWS);
  const languages = o.languages === undefined ? undefined : readLanguages(o, 'languages', '');
  const resume = readOptString(o, 'resume_token', '', MAX_RESUME_TOKEN);
  if (view === 'operator' && languages === undefined) throw new ProtocolError('languages', 'is not set');
  if (view === 'supervisor' && (languages !== undefined || resume !== undefined)) {
    throw new ProtocolError('languages', 'must be empty for the supervisor view');
  }
  return compact<Hello>({
    type: 'hello',
    version: readInt(o, 'version', '', 1),
    view,
    token: readOptString(o, 'token', '', MAX_TOKEN_BYTES),
    resume_token: resume,
    languages,
  });
}

function readWatchLike(o: Rec, type: 'watch' | 'unwatch'): Watch | Unwatch {
  return {
    type,
    request_id: readRoutingId(o, 'request_id', ''),
    conversation_id: readString(o, 'conversation_id', ''),
  };
}

function readAction(o: Rec): Action {
  const action = readEnum(o, 'action', '', ACTIONS);
  const needsOffer = action === 'accept' || action === 'refuse' || action === 'reply' || action === 'resolve';
  const offerId = readOptRoutingId(o, 'offer_id', '');
  if (needsOffer && offerId === undefined) throw new ProtocolError('offer_id', 'is not set');
  if (!needsOffer && offerId !== undefined) throw new ProtocolError('offer_id', 'must be empty');
  const languages = o.languages === undefined ? undefined : readLanguages(o, 'languages', '');
  if (action === 'set_languages' && languages === undefined) throw new ProtocolError('languages', 'is not set');
  if (action !== 'set_languages' && languages !== undefined) throw new ProtocolError('languages', 'must be empty');
  return compact<Action>({
    type: 'action',
    request_id: readRoutingId(o, 'request_id', ''),
    action,
    offer_id: offerId,
    languages,
  });
}

// ---------------------------------------------------------------- server

type ServerReader = (o: Rec, options: ParseOptions) => ServerMessage;

const SERVER_READERS: Readonly<Record<string, ServerReader>> = {
  welcome: readWelcome,
  status: readStatus,
  state: readState,
  messages: readMessages,
  message_removed: readMessageRemoved,
  result: readResult,
};

function readLimits(o: Rec, path: string): Limits {
  return {
    max_message_bytes: readInt(o, 'max_message_bytes', path, 1),
    actions_per_second: readInt(o, 'actions_per_second', path, 1),
    action_burst: readInt(o, 'action_burst', path, 1),
    max_watched: readInt(o, 'max_watched', path, 1),
  };
}

function readWelcome(o: Rec): Welcome {
  const view = readEnum(o, 'view', '', VIEWS);
  const operatorId = readOptRoutingId(o, 'operator_id', '');
  const resumeToken = readOptString(o, 'resume_token', '', MAX_RESUME_TOKEN);
  if (view === 'operator' && (operatorId === undefined || resumeToken === undefined)) {
    throw new ProtocolError('operator_id', 'and resume_token are required for the operator view');
  }
  return compact<Welcome>({
    type: 'welcome',
    version: readInt(o, 'version', '', 1),
    level: readEnum(o, 'level', '', LEVELS),
    view,
    operator_id: operatorId,
    resume_token: resumeToken,
    server_time_us: readInt(o, 'server_time_us', '', 1),
    speed_factor: readNumber(o, 'speed_factor', '', true),
    accept_timeout_us: readInt(o, 'accept_timeout_us', '', 1),
    assignment_target_us: readInt(o, 'assignment_target_us', '', 1),
    first_response_target_us: readInt(o, 'first_response_target_us', '', 1),
    tick_ms: readInt(o, 'tick_ms', '', 1),
    limits: readObject(o, 'limits', '', readLimits),
  });
}

function readStatus(o: Rec): Status {
  const stale = readBool(o, 'stale', '');
  const reason = readOptEnum(o, 'reason', '', STALE_REASONS);
  if (stale && reason === undefined) throw new ProtocolError('reason', 'is not set');
  return compact<Status>({ type: 'status', stale, reason, lag_us: readOptInt(o, 'lag_us', '') });
}

function readOperatorRow(o: Rec, path: string): OperatorRow {
  const load = readInt(o, 'load', path);
  const offered = readInt(o, 'offered', path);
  if (offered > load) throw new ProtocolError(`${path}.offered`, 'is more than load');
  return compact<OperatorRow>({
    id: readRoutingId(o, 'id', path),
    kind: readEnum(o, 'kind', path, OPERATOR_KINDS),
    languages: readArray(o, 'languages', path, langItem),
    capacity: readInt(o, 'capacity', path, 1),
    presence: readEnum(o, 'presence', path, PRESENCES),
    load,
    offered,
    team: readOptString(o, 'team', path),
  });
}

function readOperatorConversation(o: Rec, path: string): OperatorConversation {
  const state = readEnum(o, 'state', path, HELD_STATES);
  const acceptBy = readOptInt(o, 'accept_by_us', path, 1);
  const acceptedAt = readOptInt(o, 'accepted_at_us', path, 1);
  if (state === 'offered' && acceptBy === undefined) throw new ProtocolError(`${path}.accept_by_us`, 'is not set');
  if (state === 'assigned' && acceptedAt === undefined) {
    throw new ProtocolError(`${path}.accepted_at_us`, 'is not set');
  }
  return compact<OperatorConversation>({
    id: readString(o, 'id', path),
    company: readString(o, 'company', path),
    customer: readString(o, 'customer', path),
    lang: readLang(o, 'lang', path),
    session: readInt(o, 'session', path, 1),
    state,
    offer_id: readRoutingId(o, 'offer_id', path),
    accept_by_us: acceptBy,
    accepted_at_us: acceptedAt,
    queued_at_us: readInt(o, 'queued_at_us', path, 1),
    first_response_due_us: readOptInt(o, 'first_response_due_us', path, 1),
    first_response_missed: readBool(o, 'first_response_missed', path),
    replied: readBool(o, 'replied', path),
    priority: readInt(o, 'priority', path),
    customer_messages: readInt(o, 'customer_messages', path),
    company_messages: readInt(o, 'company_messages', path),
    last_message_source_us: readOptInt(o, 'last_message_source_us', path, 1),
  });
}

function readConversationRow(o: Rec, path: string): ConversationRow {
  const state = readEnum(o, 'state', path, CONVERSATION_STATES);
  const operatorId = readOptRoutingId(o, 'operator_id', path);
  const isHeld = state === 'offered' || state === 'assigned';
  if (isHeld && operatorId === undefined) throw new ProtocolError(`${path}.operator_id`, 'is not set');
  return compact<ConversationRow>({
    id: readString(o, 'id', path),
    company: readString(o, 'company', path),
    customer: readString(o, 'customer', path),
    lang: readLang(o, 'lang', path),
    session: readInt(o, 'session', path, 1),
    state,
    team_queue: readBool(o, 'team_queue', path),
    priority: readInt(o, 'priority', path),
    operator_id: operatorId,
    queued_at_us: readOptInt(o, 'queued_at_us', path, 1),
    assignment_missed: readBool(o, 'assignment_missed', path),
    first_response_missed: readBool(o, 'first_response_missed', path),
    customer_messages: readInt(o, 'customer_messages', path),
    company_messages: readInt(o, 'company_messages', path),
    opened_source_us: readInt(o, 'opened_source_us', path, 1),
  });
}

function readQueueRow(o: Rec, path: string): QueueRow {
  return compact<QueueRow>({
    id: readLang(o, 'id', path),
    waiting: readInt(o, 'waiting', path),
    team_waiting: readInt(o, 'team_waiting', path),
    offered: readInt(o, 'offered', path),
    oldest_queued_at_us: readOptInt(o, 'oldest_queued_at_us', path, 1),
    missed_assignment: readInt(o, 'missed_assignment', path),
  });
}

function readTeamRow(o: Rec, path: string): TeamRow {
  return {
    id: readString(o, 'id', path),
    members: readArray(o, 'members', path, routingIdItem),
    active_load: readInt(o, 'active_load', path),
    formed_at_us: readInt(o, 'formed_at_us', path, 1),
  };
}

function readStateCounts(o: Rec, path: string): StateCounts {
  return {
    open: readInt(o, 'open', path),
    waiting: readInt(o, 'waiting', path),
    offered: readInt(o, 'offered', path),
    assigned: readInt(o, 'assigned', path),
    resolved: readInt(o, 'resolved', path),
  };
}

function readTotals(o: Rec, path: string): Totals {
  return {
    messages_per_second: readNumber(o, 'messages_per_second', path),
    conversations: readObject(o, 'conversations', path, readStateCounts),
    companies_active: readInt(o, 'companies_active', path),
    operators_online: readInt(o, 'operators_online', path),
    humans_online: readInt(o, 'humans_online', path),
    slots_capacity: readInt(o, 'slots_capacity', path),
    slots_used: readInt(o, 'slots_used', path),
    missed_assignment: readInt(o, 'missed_assignment', path),
    missed_first_response: readInt(o, 'missed_first_response', path),
    wait_p50_us: readInt(o, 'wait_p50_us', path),
    wait_p90_us: readInt(o, 'wait_p90_us', path),
  };
}

function readChanges<T extends { readonly id: string }>(
  read: (o: Rec, path: string) => T,
  isSnapshot: boolean,
): (o: Rec, path: string) => Changes<T> {
  return (o, path) => {
    const set = readOptArray(o, 'set', path, (v, p) => read(asRecord(v, p), p));
    const remove = readOptArray(o, 'remove', path, idItem);
    if (isSnapshot && remove !== undefined && remove.length > 0) {
      throw new ProtocolError(`${path}.remove`, 'must be empty in a snapshot');
    }
    return compact<Changes<T>>({ set, remove });
  };
}

function readState(o: Rec): State {
  const seq = readInt(o, 'seq', '', 1);
  const snapshot = readBool(o, 'snapshot', '');
  if (seq === 1 && !snapshot) throw new ProtocolError('snapshot', 'must be true for seq 1');
  return compact<State>({
    type: 'state',
    seq,
    snapshot,
    server_time_us: readInt(o, 'server_time_us', '', 1),
    me: readOptObject(o, 'me', '', readOperatorRow),
    my_conversations: readOptObject(o, 'my_conversations', '', readChanges(readOperatorConversation, snapshot)),
    conversations: readOptObject(o, 'conversations', '', readChanges(readConversationRow, snapshot)),
    operators: readOptObject(o, 'operators', '', readChanges(readOperatorRow, snapshot)),
    queues: readOptObject(o, 'queues', '', readChanges(readQueueRow, snapshot)),
    teams: readOptObject(o, 'teams', '', readChanges(readTeamRow, snapshot)),
    totals: readOptObject(o, 'totals', '', readTotals),
  });
}

function readTextMessage(level: Level | undefined): (v: unknown, path: string) => TextMessage {
  return (v, path) => {
    const o = asRecord(v, path);
    const text = readOptText(o, 'text', path);
    if (text !== undefined && level === 'demo') {
      throw new ProtocolError(`${path}.text`, 'must be empty at the demo level');
    }
    return compact<TextMessage>({
      id: readString(o, 'id', path),
      direction: readEnum(o, 'direction', path, DIRECTIONS),
      source_us: readInt(o, 'source_us', path, 1),
      length: readInt(o, 'length', path),
      text,
      edited: readBool(o, 'edited', path),
    });
  };
}

function readMessages(o: Rec, options: ParseOptions): Messages {
  return {
    type: 'messages',
    conversation_id: readString(o, 'conversation_id', ''),
    snapshot: readBool(o, 'snapshot', ''),
    messages: readArray(o, 'messages', '', readTextMessage(options.level)),
  };
}

function readMessageRemoved(o: Rec): MessageRemoved {
  return {
    type: 'message_removed',
    conversation_id: readString(o, 'conversation_id', ''),
    message_id: readString(o, 'message_id', ''),
    reason: readEnum(o, 'reason', '', REMOVE_REASONS),
  };
}

function readResult(o: Rec): Result {
  const outcome = readEnum(o, 'outcome', '', OUTCOMES);
  // `code` is an enum like the others: an unknown code is a broken frame
  // (docs/gateway-protocol.md, "Versioning").
  const code = readOptEnum(o, 'code', '', KNOWN_RESULT_CODES);
  if (outcome === 'rejected' && code === undefined) throw new ProtocolError('code', 'is not set');
  return compact<Result>({
    type: 'result',
    request_id: readRoutingId(o, 'request_id', ''),
    outcome,
    code,
    message: readOptText(o, 'message', ''),
  });
}

function readPublicQueue(o: Rec, path: string): PublicQueue {
  return {
    lang: readLang(o, 'lang', path),
    waiting: readInt(o, 'waiting', path),
    offered: readInt(o, 'offered', path),
    oldest_wait_us: readInt(o, 'oldest_wait_us', path),
    missed_assignment: readInt(o, 'missed_assignment', path),
  };
}
