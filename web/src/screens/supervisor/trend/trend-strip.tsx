// The trend strip of the supervisor screen (decision 0014, point 5A): six
// numbers over the last hour, with a mark for each settings change in that
// hour. Shown only when the gateway sent a trend history: an older gateway
// gets no strip at all.

import { formatCount, formatShortDuration } from '../../../lib/format.ts';
import type { HelpId } from '../../../lib/help-texts.ts';
import { useLive } from '../../../live/hooks.ts';
import { TREND_WINDOW_MS } from '../../../live/trend.ts';
import type { SettingsBlock, SettingsChange, TrendPoint } from '../../../protocol/types.ts';
import { HelpHint } from '../../../ui/help-hint.tsx';
import { Panel } from '../../../ui/panel.tsx';
import { describeChange, formatChangeTime } from '../settings/describe.ts';
import { Sparkline, type ChangeMark } from './sparkline.tsx';

interface Series {
  readonly label: string;
  readonly hint: HelpId;
  readonly value: (p: TrendPoint) => number;
  readonly format: (v: number) => string;
}

const SERIES: readonly Series[] = [
  { label: 'Waiting', hint: 'supervisor.trend.waiting', value: (p) => p.waiting, format: formatCount },
  { label: 'Longest wait', hint: 'supervisor.trend.longest', value: (p) => p.longest_wait_us, format: formatShortDuration },
  { label: 'Resolved per minute', hint: 'supervisor.trend.resolved', value: (p) => p.resolved_last_minute, format: formatCount },
  { label: 'Teams', hint: 'supervisor.trend.teams', value: (p) => p.teams, format: formatCount },
  { label: 'Operators in teams', hint: 'supervisor.trend.in-teams', value: (p) => p.operators_in_teams, format: formatCount },
  { label: 'Online operators', hint: 'supervisor.trend.online', value: (p) => p.operators_online, format: formatCount },
];

export function TrendStrip() {
  const trend = useLive((s) => s.trend);
  const settings = useLive((s) => s.settings);
  if (trend === null) return null;

  const newest = trend.points.at(-1);
  const toUs = newest?.sampled_at_us ?? 0;
  const fromUs = toUs - TREND_WINDOW_MS * 1000;
  const marks = newest === undefined || settings === null ? [] : marksIn(settings.changes, fromUs, toUs + trend.intervalMs * 1000, toUs, settings);
  const every = `${Math.round(trend.intervalMs / 1000)} s`;

  return (
    <Panel title="Last hour" hint="supervisor.panel.trend" meta={`a point every ${every}`}>
      {newest === undefined ? (
        <p className="px-4 py-6 text-sm text-ink-3">No points yet. The gateway adds one every {every}.</p>
      ) : (
        <>
          <ul aria-label="Trends of the last hour" className="grid min-w-0 grid-cols-2 gap-px bg-line sm:grid-cols-3 xl:grid-cols-6">
            {SERIES.map((series) => (
              <li key={series.label} className="flex min-w-0 flex-col gap-1 bg-panel px-4 pt-3 pb-4">
                <span className="eyebrow flex min-w-0 items-center gap-1.5">
                  <span className="truncate">{series.label}</span>
                  <HelpHint id={series.hint} label={`${series.label}, last hour`} />
                </span>
                <Sparkline
                  label={series.label}
                  points={trend.points}
                  value={series.value}
                  format={series.format}
                  fromUs={fromUs}
                  toUs={toUs}
                  intervalMs={trend.intervalMs}
                  marks={marks}
                />
              </li>
            ))}
          </ul>
          <div className="flex flex-wrap items-baseline justify-between gap-x-4 border-t border-line px-4 py-1.5 font-mono text-[11px] text-ink-3">
            <span>60 min ago</span>
            <span>now</span>
          </div>
          {marks.length > 0 && <MarksList marks={marks} />}
        </>
      )}
    </Panel>
  );
}

function MarksList({ marks }: { readonly marks: readonly ChangeMark[] }) {
  return (
    <div className="flex flex-col gap-1.5 border-t border-line px-4 py-3">
      <div className="flex items-center gap-1.5">
        <h3 className="eyebrow">Settings changes in this hour</h3>
        <HelpHint id="supervisor.trend.marks" label="Settings changes in this hour" />
      </div>
      <ol aria-label="Settings changes in this hour" className="flex flex-col gap-1 text-xs text-ink-2">
        {marks.map((m) => (
          <li key={m.n} className="flex gap-2">
            <span aria-hidden="true" className="flex h-4 min-w-4 shrink-0 items-center justify-center rounded-full border border-line-strong font-mono text-[10px] text-ink-2">
              {m.n}
            </span>
            <span>
              <span className="sr-only">Mark {m.n}: </span>
              <span className="font-mono tnum">{formatChangeTime(m.atUs)}</span> {m.text}
            </span>
          </li>
        ))}
      </ol>
    </div>
  );
}

/**
 * The changes inside the window, oldest first and numbered from 1. A change
 * after the newest point (it comes before the next one) sits at the right
 * edge.
 */
function marksIn(changes: readonly SettingsChange[], fromUs: number, untilUs: number, toUs: number, block: SettingsBlock): ChangeMark[] {
  return changes
    .filter((c) => c.changed_at_us >= fromUs && c.changed_at_us <= untilUs)
    .sort((a, b) => a.changed_at_us - b.changed_at_us)
    .map((c, i) => ({ n: i + 1, atUs: Math.min(c.changed_at_us, toUs), text: describeChange(c, block) }));
}
