// Number tiles and the state bar, shared by the supervisor screen and the
// public page.

import type { ReactNode } from 'react';

import type { HelpId } from '../lib/help-texts.ts';
import type { StateCounts } from '../protocol/types.ts';
import { formatCount, loadBarWidths, spokenLoad } from '../lib/format.ts';
import { HelpHint } from './help-hint.tsx';

type StatTone = 'neutral' | 'wait' | 'late' | 'done';

const VALUE_TONES: Readonly<Record<StatTone, string>> = {
  neutral: 'text-ink',
  wait: 'text-wait',
  late: 'text-late',
  done: 'text-done',
};

interface StatProps {
  readonly label: string;
  /** A help text about the number, next to the label. */
  readonly hint?: HelpId;
  readonly value: ReactNode;
  readonly unit?: string;
  readonly detail?: ReactNode;
  readonly tone?: StatTone;
}

export function Stat({ label, hint, value, unit, detail, tone = 'neutral' }: StatProps) {
  return (
    <div className="flex min-w-0 flex-col gap-1.5 bg-panel px-4 py-3">
      <dt className="eyebrow flex min-w-0 items-center gap-1.5">
        <span className="truncate">{label}</span>
        {hint !== undefined && <HelpHint id={hint} label={label} />}
      </dt>
      <dd className={`font-mono text-2xl leading-none font-medium tnum ${VALUE_TONES[tone]}`}>
        {value}
        {unit !== undefined && <span className="ml-1 text-sm font-normal text-ink-3">{unit}</span>}
      </dd>
      {detail !== undefined && <dd className="truncate text-xs text-ink-3">{detail}</dd>}
    </div>
  );
}

interface StatGridProps {
  readonly label: string;
  readonly children: ReactNode;
}

export function StatGrid({ label, children }: StatGridProps) {
  return (
    <dl
      aria-label={label}
      className="grid min-w-0 grid-cols-2 gap-px overflow-hidden rounded-lg border border-line bg-line sm:grid-cols-3 xl:grid-cols-6"
    >
      {children}
    </dl>
  );
}

interface MeterProps {
  readonly value: number;
  readonly max: number;
  readonly label: string;
  /**
   * The part of the value that waits for an accept. It shows in a paler
   * tone after the solid part, and the label says it in words.
   */
  readonly offered?: number;
}

type MeterTone = 'accent' | 'wait' | 'late';

const METER_TONES: Readonly<Record<MeterTone, { readonly solid: string; readonly muted: string }>> = {
  accent: { solid: 'bg-accent', muted: 'bg-accent-muted' },
  wait: { solid: 'bg-wait', muted: 'bg-wait-muted' },
  late: { solid: 'bg-late', muted: 'bg-late-muted' },
};

/** Load against capacity. Full shows amber, over capacity red. */
export function Meter({ value, max, label, offered }: MeterProps) {
  const tone = METER_TONES[value > max ? 'late' : value === max ? 'wait' : 'accent'];
  const widths = loadBarWidths(value, max, offered ?? 0);
  const name = offered === undefined ? label : `${label}: ${spokenLoad(value, max, offered)}`;
  return (
    <span
      role="meter"
      aria-label={name}
      aria-valuemin={0}
      aria-valuemax={max}
      aria-valuenow={value}
      className="inline-flex h-1.5 w-16 shrink-0 overflow-hidden rounded-full bg-sunken align-middle"
    >
      <span className={`h-full ${tone.solid}`} style={{ width: `${widths.accepted}%` }} />
      {widths.offered > 0 && <span className={`h-full ${tone.muted}`} style={{ width: `${widths.offered}%` }} />}
    </span>
  );
}

const STATE_SEGMENTS = [
  { key: 'open', label: 'Open', className: 'bg-ink-3/40', hint: 'state.open' },
  { key: 'waiting', label: 'Waiting', className: 'bg-wait', hint: 'state.waiting' },
  { key: 'offered', label: 'Offered', className: 'bg-accent/60', hint: 'state.offered' },
  { key: 'assigned', label: 'Assigned', className: 'bg-accent', hint: 'state.assigned' },
  { key: 'resolved', label: 'Resolved', className: 'bg-done', hint: 'state.resolved' },
] as const satisfies readonly { readonly key: keyof StateCounts; readonly label: string; readonly className: string; readonly hint: HelpId }[];

/** Open conversations by state, as one bar with a legend. */
export function StateBar({ counts }: { readonly counts: StateCounts }) {
  const total = STATE_SEGMENTS.reduce((sum, s) => sum + counts[s.key], 0);
  return (
    <figure className="flex flex-col gap-3">
      <div className="flex h-3 w-full overflow-hidden rounded-sm bg-sunken" aria-hidden="true">
        {STATE_SEGMENTS.map((s) => (
          <span key={s.key} className={`${s.className} h-full transition-[width] duration-500`} style={{ width: `${total === 0 ? 0 : (counts[s.key] / total) * 100}%` }} />
        ))}
      </div>
      <figcaption>
        <ul className="flex flex-wrap gap-x-5 gap-y-1 text-xs text-ink-2">
          {STATE_SEGMENTS.map((s) => (
            <li key={s.key} className="flex items-center gap-1.5">
              <span aria-hidden="true" className={`size-2 rounded-[2px] ${s.className}`} />
              {s.label}
              <span className="font-mono text-ink tnum">{formatCount(counts[s.key])}</span>
              <HelpHint id={s.hint} label={`${s.label} state`} />
            </li>
          ))}
        </ul>
      </figcaption>
    </figure>
  );
}
