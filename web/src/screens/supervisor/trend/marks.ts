// The marks of settings changes on the trend strip. Every change keeps its
// own dashed line on the charts, but the numbered labels above the lines
// would overlap when changes are close together (seven changes in two
// minutes are one point on an hour axis). Labels that would touch merge
// into one label with the range of their numbers, for example "6-12". The
// list under the strip still has one entry per change.
//
// The grouping uses the narrowest chart of the strip, so all six charts
// and all screen widths show the same labels, and the labels never overlap
// on a wider chart either.

export interface ChangeMark {
  /** Number of the change in the hour, from 1, oldest first. */
  readonly n: number;
  readonly atUs: number;
  readonly text: string;
}

export interface MarkGroup {
  /** Oldest first; the numbers follow each other. */
  readonly marks: readonly ChangeMark[];
  /** "7" for one change, "6-12" for several. */
  readonly label: string;
  /** Centre of the label as a share of the chart width, 0 to 1, kept so the label stays inside the chart. */
  readonly at: number;
}

/** Width of the narrowest chart: two columns on a 360 px phone. */
export const NARROW_CHART_PX = 130;
/** IBM Plex Mono at 9 px: 0.6 em per character. */
const CHAR_PX = 5.4;
/** Padding (2 px each side) and border (1 px each side) of a label. */
const LABEL_PAD_PX = 6;
/** min-w-3.5 */
const LABEL_MIN_PX = 14;
/** Free space between two labels. */
const LABEL_GAP_PX = 2;

/** The width in pixels a label takes on the chart. */
export function labelWidthPx(label: string): number {
  return Math.max(LABEL_MIN_PX, label.length * CHAR_PX + LABEL_PAD_PX);
}

/**
 * Groups the marks so that no two labels overlap on a chart of chartPx
 * pixels: neighbours that would touch merge, and the merged label sits in
 * the middle of its first and last line. Repeats until no labels touch.
 */
export function groupMarks(marks: readonly ChangeMark[], fromUs: number, toUs: number, chartPx = NARROW_CHART_PX): MarkGroup[] {
  const span = Math.max(1, toUs - fromUs);
  const shareOf = (us: number) => Math.min(1, Math.max(0, (us - fromUs) / span));

  function place(group: readonly ChangeMark[]): MarkGroup & { readonly widthPx: number } {
    const first = group[0];
    const last = group.at(-1);
    if (first === undefined || last === undefined) throw new Error('empty mark group');
    const label = group.length === 1 ? String(first.n) : `${first.n}-${last.n}`;
    const widthPx = labelWidthPx(label);
    const half = widthPx / 2 / chartPx;
    const middle = (shareOf(first.atUs) + shareOf(last.atUs)) / 2;
    return { marks: group, label, at: Math.min(Math.max(middle, half), 1 - half), widthPx };
  }

  let groups = [...marks].sort((a, b) => a.atUs - b.atUs || a.n - b.n).map((m) => [m]);
  for (;;) {
    const placed = groups.map(place);
    const touching = placed.findIndex((g, i) => {
      const left = placed[i - 1];
      return left !== undefined && (g.at - left.at) * chartPx < (g.widthPx + left.widthPx) / 2 + LABEL_GAP_PX;
    });
    if (touching === -1) return placed.map(({ marks: m, label, at }) => ({ marks: m, label, at }));
    groups = [...groups.slice(0, touching - 1), [...(groups[touching - 1] ?? []), ...(groups[touching] ?? [])], ...groups.slice(touching + 1)];
  }
}
