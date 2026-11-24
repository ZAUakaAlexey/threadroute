// The state of the settings form: what the user typed, the version the
// edits were made on, the answer of the gateway, and the wait after a
// "too soon". A reducer and pure functions, so the rules are tested without
// React. The panel keeps this state with useReducer.
//
// Only edited fields are kept. Every other field shows the live current
// value, so a change by someone else updates them at once. The set that is
// sent is the live current set with the edited fields laid over it.

import type { Bound, Result, SettingsBlock, SettingsField, StandSettings } from '../../../protocol/types.ts';
import { resultMessage } from '../../../live/result-messages.ts';
import { ALL_META, boundOf, fieldMeta, formatSetting, parseInput, toInput, valueOf, type FieldMeta } from './fields.ts';

export type Answer =
  | { readonly kind: 'applied'; readonly version: number }
  | { readonly kind: 'unchanged' }
  | { readonly kind: 'conflict' }
  | { readonly kind: 'invalid' }
  | { readonly kind: 'too_soon' }
  | { readonly kind: 'not_allowed' }
  | { readonly kind: 'off' }
  | { readonly kind: 'other'; readonly text: string };

export interface FormState {
  /** The text of each edited field. */
  readonly drafts: Readonly<Partial<Record<SettingsField, string>>>;
  /**
   * Exact values behind texts that the form filled itself ("Reset to
   * defaults"): the text of a duration is rounded, the value is not.
   */
  readonly exact: Readonly<Partial<Record<SettingsField, number>>>;
  /** The version of the set the edits were made on; null without edits. */
  readonly baseVersion: number | null;
  /** Reasons from the gateway, by field name, or "sim" for the sim part. */
  readonly serverErrors: Readonly<Record<string, string>>;
  readonly isPending: boolean;
  readonly answer: Answer | null;
  /** Wall clock time from which a change may be sent again, after "too soon". */
  readonly retryAtMs: number | null;
  /** The wait that the gateway asked for (retry_after_ms), after "too soon". */
  readonly retryWaitMs: number | null;
}

export type FormAction =
  /** isClean: no field differs from the live set now, so the edit starts on the live version. */
  | { readonly type: 'edit'; readonly field: SettingsField; readonly text: string; readonly version: number; readonly isClean: boolean }
  | { readonly type: 'discard' }
  | { readonly type: 'reset_to_defaults'; readonly block: SettingsBlock }
  | { readonly type: 'keep_edits'; readonly version: number }
  | { readonly type: 'submit' }
  | { readonly type: 'answered'; readonly result: Result; readonly version: number; readonly nowMs: number }
  | { readonly type: 'retry_ready' };

export const INITIAL_FORM: FormState = { drafts: {}, exact: {}, baseVersion: null, serverErrors: {}, isPending: false, answer: null, retryAtMs: null, retryWaitMs: null };

export function formReducer(state: FormState, action: FormAction): FormState {
  switch (action.type) {
    case 'edit': {
      // The reasons of the gateway were about the set it got. Any edit makes
      // a new set, so they go; the checks of the console stay.
      const exact = Object.fromEntries(Object.entries(state.exact).filter(([f]) => f !== action.field));
      return {
        ...state,
        drafts: { ...state.drafts, [action.field]: action.text },
        exact,
        baseVersion: action.isClean ? action.version : (state.baseVersion ?? action.version),
        serverErrors: {},
        answer: null,
      };
    }
    case 'discard':
      return { ...state, drafts: {}, exact: {}, baseVersion: null, serverErrors: {}, answer: null };
    case 'reset_to_defaults': {
      const drafts: Partial<Record<SettingsField, string>> = {};
      const exact: Partial<Record<SettingsField, number>> = {};
      for (const m of fieldsOf(action.block)) {
        const value = valueOf(action.block.defaults, m.field);
        if (value === undefined) continue;
        drafts[m.field] = toInput(m.field, boundOf(action.block.bounds, m.field), value);
        exact[m.field] = value;
      }
      return { ...state, drafts, exact, baseVersion: action.block.version, serverErrors: {}, answer: null };
    }
    case 'keep_edits':
      return { ...state, baseVersion: action.version, answer: null };
    case 'submit':
      return { ...state, isPending: true, answer: null, serverErrors: {} };
    case 'answered':
      return answered({ ...state, isPending: false }, action.result, action.version, action.nowMs);
    case 'retry_ready':
      return { ...state, retryAtMs: null, retryWaitMs: null, answer: state.answer?.kind === 'too_soon' ? null : state.answer };
  }
}

function answered(state: FormState, result: Result, version: number, nowMs: number): FormState {
  if (result.outcome === 'done') return { ...state, drafts: {}, exact: {}, baseVersion: null, answer: { kind: 'applied', version } };
  if (result.outcome === 'already_done') return { ...state, drafts: {}, exact: {}, baseVersion: null, answer: { kind: 'unchanged' } };
  switch (result.code) {
    case 'settings_conflict':
      // The settings message with the new set came first. The edits stay,
      // on their old base, so the panel shows them as outdated.
      return { ...state, answer: { kind: 'conflict' } };
    case 'invalid_request': {
      const serverErrors: Record<string, string> = {};
      for (const f of result.fields ?? []) serverErrors[f.field] = serverErrors[f.field] === undefined ? f.reason : `${serverErrors[f.field] ?? ''} ${f.reason}`;
      return { ...state, serverErrors, answer: { kind: 'invalid' } };
    }
    case 'settings_too_soon': {
      const waitMs = result.retry_after_ms ?? 1000;
      return { ...state, retryAtMs: nowMs + waitMs, retryWaitMs: waitMs, answer: { kind: 'too_soon' } };
    }
    case 'not_allowed':
      return { ...state, answer: { kind: 'not_allowed' } };
    case 'settings_off':
      return { ...state, answer: { kind: 'off' } };
    default:
      return { ...state, answer: { kind: 'other', text: resultMessage(result) } };
  }
}

// ---------------------------------------------------------------- derived

export interface FieldView {
  readonly meta: FieldMeta;
  readonly current: number;
  readonly defaultValue: number | undefined;
  /** The text of the input: the draft, or the current value. */
  readonly text: string;
  readonly isEdited: boolean;
  /** The value of the text, when it is valid on its own. */
  readonly value: number | undefined;
  readonly error: string | null;
}

export interface FormView {
  readonly fields: readonly FieldView[];
  readonly hasSim: boolean;
  readonly simError: string | null;
  /** The set to send: the live current set with the edits laid over it. */
  readonly next: StandSettings | null;
  readonly editedCount: number;
  readonly isValid: boolean;
  /** Someone changed the set after the edits started. */
  readonly isOutdated: boolean;
  readonly isWaiting: boolean;
  readonly canApply: boolean;
  readonly canReset: boolean;
}

/** The fields of the block: the teams part always, the sim part when a simulator reported. */
export function fieldsOf(block: SettingsBlock): readonly FieldMeta[] {
  return ALL_META.filter((m) => valueOf(block.current, m.field) !== undefined);
}

export function deriveForm(state: FormState, block: SettingsBlock): FormView {
  const metas = fieldsOf(block);
  // First each field on its own, then the rules between fields.
  const own = metas.map((meta) => {
    const bound = boundOf(block.bounds, meta.field);
    const current = valueOf(block.current, meta.field) ?? 0;
    const draft = state.drafts[meta.field];
    const text = draft ?? toInput(meta.field, bound, current);
    const exact = state.exact[meta.field];
    const parsed =
      draft === undefined ? ({ ok: true, value: current } as const) : exact !== undefined ? ({ ok: true, value: exact } as const) : parseInput(meta.field, bound, draft);
    return { meta, bound, current, text, parsed };
  });
  const values = new Map<string, number>();
  for (const f of own) if (f.parsed.ok) values.set(f.meta.field, f.parsed.value);

  const fields: FieldView[] = own.map(({ meta, bound, current, text, parsed }) => {
    const value = parsed.ok ? parsed.value : undefined;
    const isEdited = state.drafts[meta.field] !== undefined && value !== current;
    const error = parsed.ok ? (ruleError(bound, parsed.value, values, block) ?? state.serverErrors[meta.field] ?? null) : parsed.error;
    return { meta, current, defaultValue: valueOf(block.defaults, meta.field), text, isEdited, value, error };
  });

  const isValid = fields.every((f) => f.error === null);
  const editedCount = fields.filter((f) => f.isEdited).length;
  const isOutdated = !state.isPending && editedCount > 0 && state.baseVersion !== null && state.baseVersion !== block.version;
  // The panel sends retry_ready when the wait is over.
  const isWaiting = state.retryAtMs !== null;
  const next = isValid ? overlay(block.current, values) : null;
  return {
    fields,
    hasSim: block.current.sim !== undefined,
    simError: state.serverErrors.sim ?? null,
    next,
    editedCount,
    isValid,
    isOutdated,
    isWaiting,
    canApply: block.may_change && editedCount > 0 && isValid && !isOutdated && !state.isPending && !isWaiting,
    canReset: block.may_change && !state.isPending && fields.some((f) => f.defaultValue !== undefined && f.defaultValue !== (f.value ?? f.current)),
  };
}

/** The rule between two fields that the value breaks, if any (bounds "below" and "not_above"). */
function ruleError(bound: Bound, value: number, values: ReadonlyMap<string, number>, block: SettingsBlock): string | null {
  if (bound.below !== undefined) {
    const other = values.get(bound.below);
    if (other !== undefined && !(value < other)) return `Must be below the ${nounOf(bound.below)} (${formatOther(bound.below, other, block)}).`;
  }
  if (bound.not_above !== undefined) {
    const other = values.get(bound.not_above);
    if (other !== undefined && value > other) return `Must not be above the ${nounOf(bound.not_above)} (${formatOther(bound.not_above, other, block)}).`;
  }
  return null;
}

function nounOf(field: string): string {
  return fieldMeta(field)?.noun ?? field;
}

function formatOther(field: string, value: number, block: SettingsBlock): string {
  const meta = fieldMeta(field);
  return meta === undefined ? String(value) : formatSetting(field, value, boundOf(block.bounds, meta.field));
}

function overlay(current: StandSettings, values: ReadonlyMap<string, number>): StandSettings {
  const pick = <T extends object>(part: T, prefix: string): T =>
    Object.fromEntries(Object.entries(part).map(([k, v]) => [k, values.get(`${prefix}.${k}`) ?? (v as number)])) as T;
  const teams = pick(current.teams, 'teams');
  return current.sim === undefined ? { teams } : { teams, sim: pick(current.sim, 'sim') };
}

/**
 * The whole seconds a person still has to wait after "too soon": 0 when
 * the wait is over, else at least 1 and never more than the wait the
 * gateway asked for (10000 ms gives 10). The clock of the screen ticks
 * every 250 ms, so the time left it sees can be a little longer than the
 * wait itself; the upper limit hides that.
 */
export function retrySecondsLeft(retryAtMs: number, waitMs: number, nowMs: number): number {
  const leftMs = retryAtMs - nowMs;
  if (leftMs <= 0) return 0;
  return Math.max(1, Math.min(Math.ceil(waitMs / 1000), Math.ceil(leftMs / 1000)));
}

/** The sentence for an answer of the gateway. */
export function answerText(answer: Answer, secondsLeft: number): string {
  switch (answer.kind) {
    case 'applied':
      return `Applied. The stand now runs version ${answer.version} of the settings.`;
    case 'unchanged':
      return 'Nothing to change: the stand already uses these values.';
    case 'conflict':
      return 'Someone else changed the settings first. The panel shows their values now; your edits are kept and not applied.';
    case 'invalid':
      return 'The gateway did not accept some values. See the notes next to the fields.';
    case 'too_soon':
      return secondsLeft > 0
        ? `The stand takes one change at a time, and the last one was a moment ago. You can apply again in ${secondsLeft} ${secondsLeft === 1 ? 'second' : 'seconds'}.`
        : 'You can apply again now.';
    case 'not_allowed':
      return 'This connection may not change settings. Changing them needs a site account with access.';
    case 'off':
      return 'This stand does not offer settings at the moment. Nothing was changed.';
    case 'other':
      return answer.text;
  }
}

export function isErrorAnswer(answer: Answer): boolean {
  return answer.kind !== 'applied' && answer.kind !== 'unchanged';
}
