// Types of the gateway protocol, version 1 (docs/gateway-protocol.md).
// The Go types in internal/gwapi are the source of truth; the fixtures in
// internal/gwapi/testdata are checked against these types by the tests.
//
// Time fields: "_at_us" and "_due_us" are server machine times in unix
// microseconds, "_source_us" are source times of posts, and every other
// "_us" field is a duration in model time (machine time * speed_factor).

export const PROTOCOL_VERSION = 1;

export const VIEWS = ['operator', 'supervisor'] as const;
export type View = (typeof VIEWS)[number];

export const LEVELS = ['demo', 'full'] as const;
export type Level = (typeof LEVELS)[number];

export const OPERATOR_KINDS = ['human', 'simulated'] as const;
export type OperatorKind = (typeof OPERATOR_KINDS)[number];

export const PRESENCES = ['online', 'offline'] as const;
export type Presence = (typeof PRESENCES)[number];

export const CONVERSATION_STATES = ['open', 'waiting', 'offered', 'assigned', 'resolved'] as const;
export type ConversationState = (typeof CONVERSATION_STATES)[number];

export const HELD_STATES = ['offered', 'assigned'] as const;
export type HeldState = (typeof HELD_STATES)[number];

export const DIRECTIONS = ['customer', 'company'] as const;
export type Direction = (typeof DIRECTIONS)[number];

export const REMOVE_REASONS = ['deleted', 'author_removed'] as const;
export type RemoveReason = (typeof REMOVE_REASONS)[number];

export const STALE_REASONS = ['catching_up', 'source_unavailable', 'lagging'] as const;
export type StaleReason = (typeof STALE_REASONS)[number];

export const OUTCOMES = ['done', 'already_done', 'rejected'] as const;
export type Outcome = (typeof OUTCOMES)[number];

export const ACTIONS = ['online', 'offline', 'set_languages', 'accept', 'refuse', 'reply', 'resolve'] as const;
export type ActionName = (typeof ACTIONS)[number];
export type OfferActionName = Extract<ActionName, 'accept' | 'refuse' | 'reply' | 'resolve'>;

/** Codes of a rejected request that protocol version 1 defines (gwapi.Codes). */
export const KNOWN_RESULT_CODES = [
  'invalid_request',
  'operator_not_found',
  'offer_not_found',
  'offer_expired',
  'offer_refused',
  'offer_accepted',
  'offer_ended',
  'offer_not_accepted',
  'assignment_ended',
  'unavailable',
  'internal',
  'not_allowed',
  'rate_limited',
  'unknown_offer',
  'stale_connection',
  'unknown_conversation',
  'too_many_watched',
] as const;
export type KnownResultCode = (typeof KNOWN_RESULT_CODES)[number];

// ---------------------------------------------------------------- client

export interface Hello {
  readonly type: 'hello';
  readonly version: number;
  readonly view: View;
  readonly token?: string;
  readonly resume_token?: string;
  readonly languages?: readonly string[];
}

export interface Watch {
  readonly type: 'watch';
  readonly request_id: string;
  readonly conversation_id: string;
}

export interface Unwatch {
  readonly type: 'unwatch';
  readonly request_id: string;
  readonly conversation_id: string;
}

export interface Action {
  readonly type: 'action';
  readonly request_id: string;
  readonly action: ActionName;
  readonly offer_id?: string;
  readonly languages?: readonly string[];
}

export type ClientMessage = Hello | Watch | Unwatch | Action;
/** Client messages that get exactly one result. */
export type ClientRequest = Watch | Unwatch | Action;

// ---------------------------------------------------------------- server

export interface Limits {
  readonly max_message_bytes: number;
  readonly actions_per_second: number;
  readonly action_burst: number;
  readonly max_watched: number;
}

export interface Welcome {
  readonly type: 'welcome';
  readonly version: number;
  readonly level: Level;
  readonly view: View;
  readonly operator_id?: string;
  readonly resume_token?: string;
  readonly server_time_us: number;
  readonly speed_factor: number;
  readonly accept_timeout_us: number;
  readonly assignment_target_us: number;
  readonly first_response_target_us: number;
  readonly tick_ms: number;
  readonly limits: Limits;
}

export interface Status {
  readonly type: 'status';
  readonly stale: boolean;
  readonly reason?: StaleReason;
  readonly lag_us?: number;
}

export interface OperatorRow {
  readonly id: string;
  readonly kind: OperatorKind;
  readonly languages: readonly string[];
  readonly capacity: number;
  readonly presence: Presence;
  readonly load: number;
  readonly offered: number;
  readonly team?: string;
}

export interface OperatorConversation {
  readonly id: string;
  readonly company: string;
  readonly customer: string;
  readonly lang: string;
  readonly session: number;
  readonly state: HeldState;
  readonly offer_id: string;
  readonly accept_by_us?: number;
  readonly accepted_at_us?: number;
  readonly queued_at_us: number;
  readonly first_response_due_us?: number;
  readonly first_response_missed: boolean;
  readonly replied: boolean;
  readonly priority: number;
  readonly customer_messages: number;
  readonly company_messages: number;
  readonly last_message_source_us?: number;
}

export interface ConversationRow {
  readonly id: string;
  readonly company: string;
  readonly customer: string;
  readonly lang: string;
  readonly session: number;
  readonly state: ConversationState;
  readonly team_queue: boolean;
  readonly priority: number;
  readonly operator_id?: string;
  readonly queued_at_us?: number;
  readonly assignment_missed: boolean;
  readonly first_response_missed: boolean;
  readonly customer_messages: number;
  readonly company_messages: number;
  readonly opened_source_us: number;
}

export interface QueueRow {
  readonly id: string;
  readonly waiting: number;
  readonly team_waiting: number;
  readonly offered: number;
  readonly oldest_queued_at_us?: number;
  readonly missed_assignment: number;
}

export interface TeamRow {
  readonly id: string;
  readonly members: readonly string[];
  readonly active_load: number;
  readonly formed_at_us: number;
}

export interface StateCounts {
  readonly open: number;
  readonly waiting: number;
  readonly offered: number;
  readonly assigned: number;
  readonly resolved: number;
}

export interface Totals {
  readonly messages_per_second: number;
  readonly conversations: StateCounts;
  readonly companies_active: number;
  readonly operators_online: number;
  readonly humans_online: number;
  readonly slots_capacity: number;
  readonly slots_used: number;
  readonly missed_assignment: number;
  readonly missed_first_response: number;
  readonly wait_p50_us: number;
  readonly wait_p90_us: number;
}

export interface Changes<T> {
  readonly set?: readonly T[];
  readonly remove?: readonly string[];
}

export interface State {
  readonly type: 'state';
  readonly seq: number;
  readonly snapshot: boolean;
  readonly server_time_us: number;
  readonly me?: OperatorRow;
  readonly my_conversations?: Changes<OperatorConversation>;
  readonly conversations?: Changes<ConversationRow>;
  readonly operators?: Changes<OperatorRow>;
  readonly queues?: Changes<QueueRow>;
  readonly teams?: Changes<TeamRow>;
  readonly totals?: Totals;
}

export interface TextMessage {
  readonly id: string;
  readonly direction: Direction;
  readonly source_us: number;
  readonly length: number;
  /** Full level only. Lives in memory only (decision 0013). */
  readonly text?: string;
  readonly edited: boolean;
}

export interface Messages {
  readonly type: 'messages';
  readonly conversation_id: string;
  readonly snapshot: boolean;
  readonly messages: readonly TextMessage[];
}

export interface MessageRemoved {
  readonly type: 'message_removed';
  readonly conversation_id: string;
  readonly message_id: string;
  readonly reason: RemoveReason;
}

export interface Result {
  readonly type: 'result';
  readonly request_id: string;
  readonly outcome: Outcome;
  /**
   * A KnownResultCode from the gateway (the parser refuses any other), or a
   * local code of the client such as "connection_lost".
   */
  readonly code?: string;
  readonly message?: string;
}

export type ServerMessage = Welcome | Status | State | Messages | MessageRemoved | Result;
export type ServerType = ServerMessage['type'];

// ---------------------------------------------------------------- public

export interface PublicQueue {
  readonly lang: string;
  readonly waiting: number;
  readonly offered: number;
  readonly oldest_wait_us: number;
  readonly missed_assignment: number;
}

export interface PublicSnapshot {
  readonly version: number;
  readonly generated_at_us: number;
  readonly stale: boolean;
  readonly speed_factor: number;
  readonly totals: Totals;
  readonly queues: readonly PublicQueue[];
}
