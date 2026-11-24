// @vitest-environment node
// The grouping of settings-change marks on the trend strip.

import { NARROW_CHART_PX, groupMarks, labelWidthPx, type ChangeMark, type MarkGroup } from './marks.ts';

const MIN_US = 60_000_000;
const FROM = 1_000 * MIN_US;
const TO = FROM + 60 * MIN_US;

/** Marks at the given minutes after the start of the window, numbered from 1. */
function marksAt(...minutes: number[]): ChangeMark[] {
  return minutes.map((m, i) => ({ n: i + 1, atUs: FROM + m * MIN_US, text: `change ${i + 1}` }));
}

function labels(groups: readonly MarkGroup[]): string[] {
  return groups.map((g) => g.label);
}

/** No two labels overlap and every label stays inside the chart. */
function expectReadable(groups: readonly MarkGroup[], chartPx = NARROW_CHART_PX) {
  groups.forEach((g, i) => {
    const half = labelWidthPx(g.label) / 2 / chartPx;
    expect(g.at - half).toBeGreaterThanOrEqual(-1e-9);
    expect(g.at + half).toBeLessThanOrEqual(1 + 1e-9);
    const left = groups[i - 1];
    if (left !== undefined) {
      const rightEdgeOfLeft = left.at * chartPx + labelWidthPx(left.label) / 2;
      expect(g.at * chartPx - labelWidthPx(g.label) / 2).toBeGreaterThan(rightEdgeOfLeft);
    }
  });
}

/** The minimum distance of two one-digit labels, in minutes of the window. */
const TOUCH_MIN = ((labelWidthPx('1') + 2) / NARROW_CHART_PX) * 60;

describe('groupMarks', () => {
  it('has no group without marks', () => {
    expect(groupMarks([], FROM, TO)).toEqual([]);
  });

  it('keeps marks that are far apart as their own labels, at their own place', () => {
    const groups = groupMarks(marksAt(10, 40), FROM, TO);
    expect(labels(groups)).toEqual(['1', '2']);
    expect(groups[0]?.at).toBeCloseTo(10 / 60);
    expect(groups[1]?.at).toBeCloseTo(40 / 60);
    expectReadable(groups);
  });

  it('keeps two labels apart when they just do not touch, and merges them when they would', () => {
    expect(labels(groupMarks(marksAt(30, 30 + TOUCH_MIN * 1.001), FROM, TO))).toEqual(['1', '2']);
    const merged = groupMarks(marksAt(30, 30 + TOUCH_MIN * 0.999), FROM, TO);
    expect(labels(merged)).toEqual(['1-2']);
    // The merged label sits between its two lines.
    expect(merged[0]?.at).toBeCloseTo((30 + TOUCH_MIN * 0.999 / 2) / 60);
  });

  it('shows seven changes within two minutes as one label with the range', () => {
    const groups = groupMarks(marksAt(5, 18, 30, 30.2, 30.5, 30.9, 31.2, 31.6, 32), FROM, TO);
    expect(labels(groups)).toEqual(['1', '2', '3-9']);
    expect(groups[2]?.marks.map((m) => m.n)).toEqual([3, 4, 5, 6, 7, 8, 9]);
    expectReadable(groups);
  });

  it('merges a label that grows into its neighbour after a first merge', () => {
    // 1 and 2 do not touch, 2 and 3 do. The wider "2-3" then touches 1.
    const apart = TOUCH_MIN * (17 / 16);
    expect(labels(groupMarks(marksAt(20, 20 + apart), FROM, TO))).toEqual(['1', '2']);
    expect(labels(groupMarks(marksAt(20, 20 + apart, 20 + apart + 0.1), FROM, TO))).toEqual(['1-3']);
  });

  it('splits a long row of changes into labels that do not overlap, and keeps every change once', () => {
    const minutes = Array.from({ length: 40 }, (_, i) => 1 + i * 1.5);
    const groups = groupMarks(marksAt(...minutes), FROM, TO);
    expect(groups.length).toBeGreaterThan(1);
    expect(groups.flatMap((g) => g.marks.map((m) => m.n))).toEqual(minutes.map((_, i) => i + 1));
    for (const g of groups) {
      const first = g.marks[0]?.n ?? 0;
      expect(g.label).toBe(g.marks.length === 1 ? String(first) : `${first}-${first + g.marks.length - 1}`);
    }
    expectReadable(groups);
  });

  it('keeps a label at the right edge inside the chart', () => {
    const groups = groupMarks(marksAt(20, 60), FROM, TO);
    expect(labels(groups)).toEqual(['1', '2']);
    expect(groups[1]?.at).toBeCloseTo(1 - labelWidthPx('2') / 2 / NARROW_CHART_PX);
    expectReadable(groups);
  });

  it('merges a mark into the label at the right edge when the edge pushes them together', () => {
    // Far enough apart in the middle of the chart, but the label at the edge moves left.
    const apart = TOUCH_MIN * 1.05;
    expect(labels(groupMarks(marksAt(30, 30 + apart), FROM, TO))).toEqual(['1', '2']);
    const groups = groupMarks(marksAt(60 - apart, 60), FROM, TO);
    expect(labels(groups)).toEqual(['1-2']);
    expectReadable(groups);
  });

  it('keeps a mark at the left edge inside the chart', () => {
    const groups = groupMarks(marksAt(0, 0.1), FROM, TO);
    expect(labels(groups)).toEqual(['1-2']);
    expectReadable(groups);
  });

  it('gives the same labels whatever the order of the input', () => {
    const marks = marksAt(5, 30, 30.5, 59);
    expect(groupMarks([...marks].reverse(), FROM, TO)).toEqual(groupMarks(marks, FROM, TO));
  });

  it('never overlaps on a wider chart either', () => {
    const groups = groupMarks(marksAt(2, 9, 17, 18, 30, 44, 45, 46, 59, 60), FROM, TO);
    expectReadable(groups);
    // The same places on a chart three times as wide.
    expectReadable(groups, NARROW_CHART_PX * 3);
  });
});
