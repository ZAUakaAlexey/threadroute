// One small line chart of the trend strip, drawn by hand in SVG. The plot
// has a fixed height and a time axis of the whole window, so new points
// never change the layout. One series, so no legend: the tile names it.
// Marks of settings changes are thin vertical lines with a number; the
// list under the strip says what each number changed.

import { useState, type PointerEvent } from 'react';

import type { TrendPoint } from '../../../protocol/types.ts';

export interface ChangeMark {
  readonly n: number;
  readonly atUs: number;
  readonly text: string;
}

interface SparklineProps {
  readonly label: string;
  readonly points: readonly TrendPoint[];
  readonly value: (p: TrendPoint) => number;
  readonly format: (v: number) => string;
  readonly fromUs: number;
  readonly toUs: number;
  readonly intervalMs: number;
  readonly marks: readonly ChangeMark[];
}

const W = 300;
const H = 56;
/** Room above the highest point, as a share of the plot height. */
const HEADROOM = 0.15;

export function Sparkline({ label, points, value, format, fromUs, toUs, intervalMs, marks }: SparklineProps) {
  const [hover, setHover] = useState<number | null>(null);
  const values = points.map(value);
  const max = Math.max(1, ...values);
  const top = max * (1 + HEADROOM);
  const span = Math.max(1, toUs - fromUs);
  const xOf = (us: number) => ((us - fromUs) / span) * W;
  const yOf = (v: number) => H - (v / top) * H;
  const path = linePath(points, values, xOf, yOf, intervalMs);
  const last = values.at(-1);
  const hovered = hover === null ? undefined : points[hover];
  const shown = hovered === undefined ? last : value(hovered);

  function handlePointerMove(event: PointerEvent<HTMLDivElement>) {
    const box = event.currentTarget.getBoundingClientRect();
    if (box.width === 0 || points.length === 0) return;
    const atUs = fromUs + ((event.clientX - box.left) / box.width) * span;
    setHover(nearest(points, atUs));
  }

  function handlePointerLeave() {
    setHover(null);
  }

  const lowest = values.length === 0 ? 0 : Math.min(...values);
  const summary =
    last === undefined ? `${label}: no points yet` : `${label} over the last hour: now ${format(last)}, lowest ${format(lowest)}, highest ${format(Math.max(...values))}`;

  return (
    <div className="flex flex-col gap-1.5">
      {/* A fixed line: the value of the point under the pointer, or the newest one. */}
      <p className="flex h-7 items-baseline gap-2 whitespace-nowrap">
        <span className="font-mono text-xl leading-none font-medium tnum">{shown === undefined ? 'none' : format(shown)}</span>
        <span className="truncate text-xs text-ink-3">{hovered === undefined ? 'now' : `at ${clock(hovered.sampled_at_us)}`}</span>
      </p>
      <div className="relative h-14 touch-pan-y" onPointerMove={handlePointerMove} onPointerLeave={handlePointerLeave}>
        <svg role="img" aria-label={summary} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="absolute inset-0 size-full overflow-visible">
          <line x1={0} y1={H} x2={W} y2={H} stroke="var(--line-strong)" strokeWidth={1} vectorEffect="non-scaling-stroke" />
          {marks.map((m) => (
            <line
              key={m.n}
              x1={xOf(m.atUs)}
              x2={xOf(m.atUs)}
              y1={0}
              y2={H}
              stroke="var(--ink-3)"
              strokeWidth={1}
              strokeDasharray="3 3"
              vectorEffect="non-scaling-stroke"
            />
          ))}
          {path !== '' && (
            <path d={path} fill="none" stroke="var(--accent)" strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
          )}
          {hovered !== undefined && (
            <line x1={xOf(hovered.sampled_at_us)} x2={xOf(hovered.sampled_at_us)} y1={0} y2={H} stroke="var(--ink-2)" strokeWidth={1} vectorEffect="non-scaling-stroke" />
          )}
        </svg>
        {/* Dots and numbers in HTML, so the stretched SVG does not distort them. */}
        {hovered !== undefined && shown !== undefined && (
          <span
            aria-hidden="true"
            className="pointer-events-none absolute size-2 -translate-x-1/2 -translate-y-1/2 rounded-full bg-accent ring-2 ring-panel"
            style={{ left: `${(xOf(hovered.sampled_at_us) / W) * 100}%`, top: `${(yOf(shown) / H) * 100}%` }}
          />
        )}
        {marks.map((m) => (
          <span
            key={m.n}
            aria-hidden="true"
            title={m.text}
            className="absolute -top-2 flex h-3.5 min-w-3.5 -translate-x-1/2 items-center justify-center rounded-full border border-line-strong bg-panel px-0.5 font-mono text-[9px] leading-none text-ink-2"
            style={{ left: `${(xOf(m.atUs) / W) * 100}%` }}
          >
            {m.n}
          </span>
        ))}
      </div>
    </div>
  );
}

/** A line through the points, broken where points are missing (a gateway restart). */
function linePath(points: readonly TrendPoint[], values: readonly number[], xOf: (us: number) => number, yOf: (v: number) => number, intervalMs: number): string {
  const gapUs = intervalMs * 1000 * 3;
  let d = '';
  let previous: number | null = null;
  points.forEach((p, i) => {
    const isGap = previous === null || p.sampled_at_us - previous > gapUs;
    d += `${isGap ? 'M' : 'L'}${xOf(p.sampled_at_us).toFixed(2)},${yOf(values[i] ?? 0).toFixed(2)}`;
    previous = p.sampled_at_us;
  });
  return d;
}

function nearest(points: readonly TrendPoint[], atUs: number): number {
  let best = 0;
  let bestDistance = Infinity;
  points.forEach((p, i) => {
    const distance = Math.abs(p.sampled_at_us - atUs);
    if (distance < bestDistance) {
      best = i;
      bestDistance = distance;
    }
  });
  return best;
}

function clock(atUs: number): string {
  return new Date(atUs / 1000).toLocaleTimeString('en-GB', { hour12: false });
}
