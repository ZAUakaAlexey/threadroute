// The trend history of the supervisor view (decision 0014, point 5A): the
// points of welcome.trend, then one point per trend message. The gateway
// keeps the last 60 minutes; the console keeps no more than that.

import type { Trend, TrendPoint } from '../protocol/types.ts';

/** The window of the history, from docs/gateway-protocol.md ("trend"). */
export const TREND_WINDOW_MS = 60 * 60 * 1000;

export interface TrendHistory {
  /** Machine time between two points, from the gateway. */
  readonly intervalMs: number;
  /** Oldest first, at most maxPoints. */
  readonly points: readonly TrendPoint[];
  readonly maxPoints: number;
}

export function trendFromWelcome(trend: Trend | undefined): TrendHistory | null {
  if (trend === undefined) return null;
  const maxPoints = Math.max(1, Math.ceil(TREND_WINDOW_MS / trend.interval_ms));
  const sorted = [...trend.points].sort((a, b) => a.sampled_at_us - b.sampled_at_us);
  return { intervalMs: trend.interval_ms, maxPoints, points: trim(sorted, maxPoints) };
}

/**
 * Adds a point. A point that is not newer than the last one replaces the
 * point with the same time or is dropped, so a repeated frame does not
 * draw a step back in time.
 */
export function appendTrendPoint(history: TrendHistory, point: TrendPoint): TrendHistory {
  const last = history.points.at(-1);
  if (last !== undefined && point.sampled_at_us <= last.sampled_at_us) {
    if (point.sampled_at_us !== last.sampled_at_us) return history;
    return { ...history, points: [...history.points.slice(0, -1), point] };
  }
  return { ...history, points: trim([...history.points, point], history.maxPoints) };
}

/** Keeps the newest maxPoints, and drops points older than the window before the newest. */
function trim(points: readonly TrendPoint[], maxPoints: number): readonly TrendPoint[] {
  const newest = points.at(-1);
  if (newest === undefined) return points;
  const fromUs = newest.sampled_at_us - TREND_WINDOW_MS * 1000;
  const inWindow = points.filter((p) => p.sampled_at_us >= fromUs);
  return inWindow.length > maxPoints ? inWindow.slice(inWindow.length - maxPoints) : inWindow;
}
