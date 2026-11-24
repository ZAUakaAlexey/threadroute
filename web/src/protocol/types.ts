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
  // Answers to change_settings only (docs/gateway-protocol.md, "Versioning",
  // the exception for answers to a new request).
  'settings_conflict',
  'settings_too_soon',
  'settings_off',
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

export interface ChangeSettings {
  readonly type: 'change_settings';
  readonly request_id: string;
  readonly base_version: number;
  /** The whole new set. */
  readonly settings: StandSettings;
}

export type ClientMessage = Hello | Watch | Unwatch | Action | ChangeSettings;
/** Client messages that get exactly one result. */
export type ClientRequest = Watch | Unwatch | Action | ChangeSettings;

// ---------------------------------------------------------------- stand settings
// Decision 0014. The Go types are opapi.StandSettings, opapi.StandBounds and
// gwapi.Settings. Durations ("_us") are whole microseconds of model time.

export interface TeamSettings {
  readonly form_load: number;
  readonly release_load: number;
  readonly release_after_us: number;
  readonly min_size: number;
  readonly max_size: number;
  readonly max_share: number;
}

export interface SimSettings {
  readonly operators: number;
  readonly capacity: number;
  readonly handling_median_us: number;
  readonly resolve_median_us: number;
  readonly missed_offer_share: number;
}

export interface StandSettings {
  readonly teams: TeamSettings;
  /** Absent until a simulator reported its values. */
  readonly sim?: SimSettings;
}

export const TEAM_FIELDS = ['form_load', 'release_load', 'release_after_us', 'min_size', 'max_size', 'max_share'] as const;
export const SIM_FIELDS = ['operators', 'capacity', 'handling_median_us', 'resolve_median_us', 'missed_offer_share'] as const;
export type TeamField = (typeof TEAM_FIELDS)[number];
export type SimField = (typeof SIM_FIELDS)[number];
/** A field name of the set, as the protocol writes it: "teams.form_load". */
export type SettingsField = `teams.${TeamField}` | `sim.${SimField}`;
export const SETTINGS_FIELDS: readonly SettingsField[] = [
  ...TEAM_FIELDS.map((f) => `teams.${f}` as const),
  ...SIM_FIELDS.map((f) => `sim.${f}` as const),
];

export interface Bound {
  readonly min: number;
  readonly max: number;
  readonly integer: boolean;
  /** A field of the same set that the value must be below. */
  readonly below?: string;
  /** A field of the same set that the value must not be above. */
  readonly not_above?: string;
}

export interface StandBounds {
  readonly teams: Readonly<Record<TeamField, Bound>>;
  readonly sim: Readonly<Record<SimField, Bound>>;
}

export interface FieldChange {
  readonly field: string;
  /** Absent when the set had no value before (the first report of a simulator). */
  readonly old?: number;
  readonly new: number;
}

export interface SettingsChange {
  readonly changed_at_us: number;
  readonly version: number;
  /** Present only when the connection may change settings. */
  readonly by?: string;
  readonly fields: readonly FieldChange[];
}

/** The settings block of welcome and of the settings message. */
export interface SettingsBlock {
  readonly version: number;
  readonly current: StandSettings;
  readonly defaults: StandSettings;
  readonly bounds: StandBounds;
  /** Newest first. */
  readonly changes: readonly SettingsChange[];
  readonly may_change: boolean;
}

export interface TrendPoint {
  readonly sampled_at_us: number;
  readonly waiting: number;
  readonly longest_wait_us: number;
  readonly resolved_last_minute: number;
  readonly teams: number;
  readonly operators_in_teams: number;
  readonly operators_online: number;
}

export interface Trend {
  readonly interval_ms: number;
  /** Oldest first. */
  readonly points: readonly TrendPoint[];
}

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
  /** Absent from a gateway without stand settings, or an older one. */
  readonly settings?: SettingsBlock;
  /** Supervisor view only, and absent from an older gateway. */
  readonly trend?: Trend;
}

export interface SettingsMessage {
  readonly type: 'settings';
  readonly settings: SettingsBlock;
}

export interface TrendMessage {
  readonly type: 'trend';
  readonly point: TrendPoint;
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
  /** change_settings with invalid_request: the fields and why. */
  readonly fields?: readonly FieldReason[];
  /** change_settings with settings_too_soon: machine time to wait. */
  readonly retry_after_ms?: number;
}

export interface FieldReason {
  /** A field name of the set, or "sim" for the sim part as a whole. */
  readonly field: string;
  readonly reason: string;
}

export type ServerMessage = Welcome | Status | State | Messages | MessageRemoved | Result | SettingsMessage | TrendMessage;
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
