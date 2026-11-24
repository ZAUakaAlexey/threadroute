// What the console knows about each stand setting: its label, its help
// text, the words for its unit and how fast it acts (decision 0014, the
// table "Settings of the first version"). Every number (bounds, defaults,
// current values) comes from the gateway's settings block, never from here.

import type { HelpId } from '../../../lib/help-texts.ts';
import type { Bound, SettingsBlock, SettingsField, SimField, StandSettings, TeamField } from '../../../protocol/types.ts';

/**
 * "minute": acts within a minute. "gradual": acts step by step, or only on
 * work that starts after the change. "up_minute": within a minute when
 * raised, gradually when lowered (the team share).
 */
export type Speed = 'minute' | 'gradual' | 'up_minute';
export type FieldKind = 'count' | 'duration' | 'share';

export interface FieldMeta {
  readonly field: SettingsField;
  readonly label: string;
  /** The label in the middle of a sentence. */
  readonly noun: string;
  readonly hint: HelpId;
  readonly speed: Speed;
  /** The unit of a count, after the number. */
  readonly countUnit?: string;
}

export const SIM_META: readonly FieldMeta[] = [
  { field: 'sim.operators', label: 'Simulated operators', noun: 'simulated operators', hint: 'supervisor.settings.sim-operators', speed: 'minute', countUnit: 'operators' },
  { field: 'sim.capacity', label: 'Capacity of an operator', noun: 'capacity of an operator', hint: 'supervisor.settings.sim-capacity', speed: 'minute', countUnit: 'conversations' },
  { field: 'sim.handling_median_us', label: 'Handling time, median', noun: 'handling time', hint: 'supervisor.settings.sim-handling', speed: 'gradual' },
  { field: 'sim.resolve_median_us', label: 'Resolve time, median', noun: 'resolve time', hint: 'supervisor.settings.sim-resolve', speed: 'gradual' },
  { field: 'sim.missed_offer_share', label: 'Share of missed offers', noun: 'share of missed offers', hint: 'supervisor.settings.sim-missed', speed: 'minute' },
];

export const TEAM_META: readonly FieldMeta[] = [
  { field: 'teams.form_load', label: 'Load that forms a team', noun: 'load that forms a team', hint: 'supervisor.settings.teams-form-load', speed: 'minute', countUnit: 'conversations' },
  { field: 'teams.release_load', label: 'Load that releases a team', noun: 'load that releases a team', hint: 'supervisor.settings.teams-release-load', speed: 'gradual', countUnit: 'conversations' },
  { field: 'teams.release_after_us', label: 'Release time', noun: 'release time', hint: 'supervisor.settings.teams-release-after', speed: 'gradual' },
  { field: 'teams.min_size', label: 'Team size, min', noun: 'min team size', hint: 'supervisor.settings.teams-min-size', speed: 'minute', countUnit: 'operators' },
  { field: 'teams.max_size', label: 'Team size, max', noun: 'max team size', hint: 'supervisor.settings.teams-max-size', speed: 'minute', countUnit: 'operators' },
  { field: 'teams.max_share', label: 'Share of operators in teams', noun: 'share of operators in teams', hint: 'supervisor.settings.teams-max-share', speed: 'up_minute' },
];

export const ALL_META: readonly FieldMeta[] = [...SIM_META, ...TEAM_META];
const META_BY_FIELD = new Map<string, FieldMeta>(ALL_META.map((m) => [m.field, m]));

export function fieldMeta(field: string): FieldMeta | undefined {
  return META_BY_FIELD.get(field);
}

export const SPEED_TEXT: Readonly<Record<Speed, string>> = {
  minute: 'acts within a minute',
  gradual: 'acts gradually',
  up_minute: 'acts within a minute when raised, gradually when lowered',
};

// ---------------------------------------------------------------- values

const US_PER_SECOND = 1_000_000;
const US_PER_MINUTE = 60 * US_PER_SECOND;

export function boundOf(bounds: SettingsBlock['bounds'], field: SettingsField): Bound {
  const [part, name] = field.split('.') as ['teams' | 'sim', string];
  return part === 'teams' ? bounds.teams[name as TeamField] : bounds.sim[name as SimField];
}

export function valueOf(set: StandSettings, field: SettingsField): number | undefined {
  const [part, name] = field.split('.') as ['teams' | 'sim', string];
  if (part === 'teams') return set.teams[name as TeamField];
  return set.sim?.[name as SimField];
}

export function kindOf(field: SettingsField, bound: Bound): FieldKind {
  if (field.endsWith('_us')) return 'duration';
  return bound.integer ? 'count' : 'share';
}

/** The unit a duration is typed in: minutes when the bound starts at a minute or more, else seconds. */
export function durationUnit(bound: Bound): { readonly us: number; readonly label: 's' | 'min'; readonly word: string } {
  return bound.min >= US_PER_MINUTE ? { us: US_PER_MINUTE, label: 'min', word: 'minutes' } : { us: US_PER_SECOND, label: 's', word: 'seconds' };
}

/** The unit after the input. */
export function inputUnit(meta: FieldMeta, bound: Bound): string {
  const kind = kindOf(meta.field, bound);
  if (kind === 'duration') return durationUnit(bound).label;
  if (kind === 'share') return '%';
  return meta.countUnit ?? '';
}

function trimNumber(n: number, digits: number): string {
  return Number(n.toFixed(digits)).toString();
}

/** A wire value as the text of the input. */
export function toInput(field: SettingsField, bound: Bound, value: number): string {
  switch (kindOf(field, bound)) {
    case 'duration':
      return trimNumber(value / durationUnit(bound).us, 3);
    case 'share':
      return trimNumber(value * 100, 4);
    case 'count':
      return String(value);
  }
}

/** A wire value for people: "45 s", "15 min", "1 h 30 min", "50%", "200". */
export function formatSetting(field: string, value: number, bound?: Bound): string {
  const kind: FieldKind = field.endsWith('_us') ? 'duration' : bound === undefined ? (Number.isInteger(value) && !field.endsWith('share') ? 'count' : 'share') : kindOf(field as SettingsField, bound);
  if (kind === 'share') return `${trimNumber(value * 100, 2)}%`;
  if (kind === 'count') return new Intl.NumberFormat('en').format(value);
  return formatSettingDuration(value);
}

export function formatSettingDuration(us: number): string {
  const seconds = us / US_PER_SECOND;
  if (seconds < 90) return `${trimNumber(seconds, 2)} s`;
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const rest = Math.round(seconds % 60);
  const parts = [hours > 0 ? `${hours} h` : '', minutes > 0 ? `${minutes} min` : '', rest > 0 ? `${rest} s` : ''].filter((p) => p !== '');
  return parts.join(' ');
}

const NUMBER_RE = /^\d+(\.\d+)?$/;

export type Parsed = { readonly ok: true; readonly value: number } | { readonly ok: false; readonly error: string };

/** The text of an input as a wire value, with the checks of its own bound. */
export function parseInput(field: SettingsField, bound: Bound, text: string): Parsed {
  const s = text.trim();
  if (s === '') return { ok: false, error: 'Enter a value.' };
  if (!NUMBER_RE.test(s)) return { ok: false, error: 'Enter a number with digits and an optional decimal point.' };
  const n = Number(s);
  const kind = kindOf(field, bound);
  let value: number;
  if (kind === 'count') {
    if (!Number.isInteger(n)) return { ok: false, error: 'Enter a whole number.' };
    value = n;
  } else if (kind === 'duration') {
    // Durations are whole microseconds on the wire.
    value = Math.round(n * durationUnit(bound).us);
  } else {
    value = n / 100;
  }
  if (!(value >= bound.min && value <= bound.max)) {
    return { ok: false, error: `Use a value from ${formatSetting(field, bound.min, bound)} to ${formatSetting(field, bound.max, bound)}.` };
  }
  return { ok: true, value };
}
