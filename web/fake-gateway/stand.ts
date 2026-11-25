// Stand settings and the trend history of the fake gateway (decision 0014).
// The bounds and the starting values come from an example of the Go side,
// so no bound is written down in web/. The checks follow
// opapi.checkValues: each value against its bound, then the rules between
// two fields, with the same reason texts.

import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

import { parseServerMessage } from '../src/protocol/parse.ts';
import {
  SIM_FIELDS,
  TEAM_FIELDS,
  type Bound,
  type FieldChange,
  type FieldReason,
  type SettingsBlock,
  type SettingsChange,
  type SimSettings,
  type StandBounds,
  type StandSettings,
  type TrendPoint,
} from '../src/protocol/types.ts';

/** How the fake answers. "old": like a gateway before decision 0014. "no_sim": an older router that sends no sim part. */
export type StandMode = 'normal' | 'old' | 'no_sim';
/** An answer that the next change_settings gets, whatever it asks. */
export type ForcedAnswer = 'conflict' | 'too_soon' | 'off' | 'unavailable' | 'not_allowed';
export const FORCED_ANSWERS: readonly ForcedAnswer[] = ['conflict', 'too_soon', 'off', 'unavailable', 'not_allowed'];

const MAX_CHANGES = 20;
/** One change per this many milliseconds for the whole stand (opapi.SettingsChangeInterval). */
export const CHANGE_INTERVAL_MS = 10_000;
/** The trend window of docs/gateway-protocol.md. */
const TREND_WINDOW_MS = 3_600_000;

const EXAMPLE = 'server_welcome_supervisor_settings_full_may_change.json';
const TESTDATA = resolve(import.meta.dirname, '../../internal/gwapi/testdata');

/** The settings block of a Go example: the bounds and the default values. */
function readExampleBlock(): SettingsBlock {
  const path = [resolve(TESTDATA, 'settings', EXAMPLE), resolve(TESTDATA, EXAMPLE)].find((p) => existsSync(p));
  if (path === undefined) throw new Error(`fake gateway: no ${EXAMPLE} in internal/gwapi/testdata`);
  const parsed = parseServerMessage(readFileSync(path, 'utf8'));
  if (parsed.kind !== 'message' || parsed.message.type !== 'welcome' || parsed.message.settings === undefined) {
    throw new Error(`fake gateway: ${EXAMPLE} has no settings block`);
  }
  return parsed.message.settings;
}

export interface StandOptions {
  /** Simulated operators of the world: the simulator reports them as its default. */
  readonly simOperators: number;
  readonly mode?: StandMode;
}

export class Stand {
  readonly bounds: StandBounds;
  readonly defaults: StandSettings;
  current: StandSettings;
  version: number;
  changes: SettingsChange[] = [];
  mode: StandMode;
  /** The right of new connections. */
  mayChange = true;
  forced: ForcedAnswer | null = null;
  lastChangeAtMs = -Infinity;
  trend: TrendPoint[] = [];

  constructor(options: StandOptions, nowUs: number) {
    const example = readExampleBlock();
    this.bounds = example.bounds;
    this.mode = options.mode ?? 'normal';
    const sim: SimSettings | undefined = example.defaults.sim === undefined ? undefined : { ...example.defaults.sim, operators: options.simOperators };
    if (this.mode === 'no_sim' || sim === undefined) {
      this.defaults = { teams: example.defaults.teams };
      this.current = this.defaults;
      this.version = 0;
      return;
    }
    this.defaults = { teams: example.defaults.teams, sim };
    // A history to look at: the defaults stored at the first start and one
    // change by a person, both inside the trend window.
    this.current = this.defaults;
    this.version = 0;
    this.record(this.defaults, 'defaults', nowUs - 50 * 60_000_000);
    const teams = { ...this.defaults.teams, form_load: this.defaults.teams.form_load + 5 };
    this.record({ ...this.defaults, teams }, 'guest', nowUs - 20 * 60_000_000);
  }

  /** The block for one connection: "by" only with the right to change. */
  block(mayChange: boolean): SettingsBlock {
    return {
      version: this.version,
      current: this.current,
      defaults: this.defaults,
      bounds: this.bounds,
      changes: mayChange ? this.changes : this.changes.map((c) => ({ changed_at_us: c.changed_at_us, version: c.version, fields: c.fields })),
      may_change: mayChange,
    };
  }

  /** Stores a new set as the next version and logs what changed. */
  record(next: StandSettings, by: string, nowUs: number): SettingsChange {
    const fields = diff(this.version === 0 && this.changes.length === 0 ? { teams: next.teams } : this.current, next);
    this.version += 1;
    this.current = next;
    const change: SettingsChange = { changed_at_us: Math.round(nowUs), version: this.version, by, fields };
    this.changes = [change, ...this.changes].slice(0, MAX_CHANGES);
    return change;
  }

  /** The reasons a set is refused, like the gateway answers invalid_request. */
  check(next: StandSettings): FieldReason[] {
    if ((next.sim === undefined) !== (this.current.sim === undefined)) {
      return [{ field: 'sim', reason: this.current.sim === undefined ? 'must be absent: the current set has no sim part' : 'is missing' }];
    }
    return checkValues(values(next, this.bounds));
  }

  addTrendPoint(point: TrendPoint, intervalMs: number): void {
    this.trend.push(point);
    const max = Math.ceil(TREND_WINDOW_MS / intervalMs);
    if (this.trend.length > max) this.trend.splice(0, this.trend.length - max);
  }
}

interface FieldValue {
  readonly name: string;
  readonly v: number;
  readonly b: Bound;
}

function values(s: StandSettings, bounds: StandBounds): FieldValue[] {
  const teams = TEAM_FIELDS.map((f) => ({ name: `teams.${f}`, v: s.teams[f], b: bounds.teams[f] }));
  const simSet = s.sim;
  const sim = simSet === undefined ? [] : SIM_FIELDS.map((f) => ({ name: `sim.${f}`, v: simSet[f], b: bounds.sim[f] }));
  return [...teams, ...sim];
}

function checkValues(vals: readonly FieldValue[]): FieldReason[] {
  const out: FieldReason[] = [];
  const ok = new Map<string, number>();
  for (const f of vals) {
    if (!(f.v >= f.b.min && f.v <= f.b.max)) {
      out.push({ field: f.name, reason: `${f.v} is not between ${f.b.min} and ${f.b.max}` });
      continue;
    }
    ok.set(f.name, f.v);
  }
  for (const f of vals) {
    const v = ok.get(f.name);
    if (v === undefined) continue;
    const below = f.b.below === undefined ? undefined : ok.get(f.b.below);
    if (below !== undefined && !(v < below)) out.push({ field: f.name, reason: `${v} is not below ${f.b.below ?? ''} (${below})` });
    const notAbove = f.b.not_above === undefined ? undefined : ok.get(f.b.not_above);
    if (notAbove !== undefined && v > notAbove) out.push({ field: f.name, reason: `${v} is above ${f.b.not_above ?? ''} (${notAbove})` });
  }
  return out;
}

function flat(s: StandSettings): Map<string, number> {
  const out = new Map<string, number>();
  for (const f of TEAM_FIELDS) out.set(`teams.${f}`, s.teams[f]);
  if (s.sim !== undefined) for (const f of SIM_FIELDS) out.set(`sim.${f}`, s.sim[f]);
  return out;
}

/** The values that differ, like opapi.Diff: a value that was not there has no "old". */
export function diff(before: StandSettings, after: StandSettings): FieldChange[] {
  const old = flat(before);
  const out: FieldChange[] = [];
  for (const [field, v] of flat(after)) {
    const o = old.get(field);
    if (o === undefined) out.push({ field, new: v });
    else if (o !== v) out.push({ field, old: o, new: v });
  }
  return out;
}

export function sameSettings(a: StandSettings, b: StandSettings): boolean {
  const fa = flat(a);
  const fb = flat(b);
  return fa.size === fb.size && [...fa].every(([k, v]) => fb.get(k) === v);
}

/** A set with some fields replaced, by their protocol names ("teams.form_load"). */
export function withFields(base: StandSettings, fields: Readonly<Record<string, number>>): StandSettings {
  const teams: Record<string, number> = { ...base.teams };
  const sim: Record<string, number> | undefined = base.sim === undefined ? undefined : { ...base.sim };
  for (const [name, v] of Object.entries(fields)) {
    const [part, field] = name.split('.');
    if (field === undefined) continue;
    if (part === 'teams' && field in teams) teams[field] = v;
    if (part === 'sim' && sim !== undefined && field in sim) sim[field] = v;
  }
  return sim === undefined ? { teams: teams as unknown as StandSettings['teams'] } : { teams: teams as unknown as StandSettings['teams'], sim: sim as unknown as SimSettings };
}
