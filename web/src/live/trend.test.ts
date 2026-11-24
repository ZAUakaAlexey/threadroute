// @vitest-environment node
// The trend history and the settings block in the live store.

import { readFixture, readSettingsFixture } from '../test/fixtures.ts';
import { parseServerMessage } from '../protocol/parse.ts';
import type { ServerMessage, TrendPoint, Welcome } from '../protocol/types.ts';
import { createLiveStore } from './store.ts';
import { TREND_WINDOW_MS, appendTrendPoint, trendFromWelcome } from './trend.ts';

function point(atUs: number, waiting = 1): TrendPoint {
  return { sampled_at_us: atUs, waiting, longest_wait_us: 0, resolved_last_minute: 0, teams: 0, operators_in_teams: 0, operators_online: 0 };
}

function message(raw: unknown): ServerMessage {
  const parsed = parseServerMessage(JSON.stringify(raw));
  if (parsed.kind !== 'message') throw new Error('not a message');
  return parsed.message;
}

const T0 = 1_767_323_100_000_001;

describe('trend history', () => {
  it('keeps at most the points of the window the server names by its interval', () => {
    const history = trendFromWelcome({ interval_ms: 10_000, points: [] });
    expect(history?.maxPoints).toBe(TREND_WINDOW_MS / 10_000);
    let h = history ?? (undefined as never);
    for (let i = 0; i < 400; i += 1) h = appendTrendPoint(h, point(T0 + i * 10_000_000, i));
    expect(h.points).toHaveLength(360);
    expect(h.points[0]?.waiting).toBe(40);
    expect(h.points.at(-1)?.waiting).toBe(399);
  });

  it('drops points older than the window before the newest, also after a pause', () => {
    const h = trendFromWelcome({ interval_ms: 10_000, points: [point(T0), point(T0 + 10_000_000)] });
    const later = appendTrendPoint(h ?? (undefined as never), point(T0 + 2 * TREND_WINDOW_MS * 1000));
    expect(later.points).toHaveLength(1);
  });

  it('replaces a point with the same time and ignores an older one', () => {
    const h = trendFromWelcome({ interval_ms: 10_000, points: [point(T0, 1), point(T0 + 10_000_000, 2)] }) ?? (undefined as never);
    expect(appendTrendPoint(h, point(T0 + 10_000_000, 5)).points.map((p) => p.waiting)).toEqual([1, 5]);
    expect(appendTrendPoint(h, point(T0, 9))).toBe(h);
  });

  it('sorts the history of the welcome oldest first', () => {
    const h = trendFromWelcome({ interval_ms: 10_000, points: [point(T0 + 10_000_000, 2), point(T0, 1)] });
    expect(h?.points.map((p) => p.waiting)).toEqual([1, 2]);
  });
});

describe('live store, settings and trend', () => {
  const withSettings = message(readSettingsFixture('server_welcome_supervisor_settings_demo.json'));
  const oldWelcome = message(readFixture('server_welcome_supervisor_full.json'));

  it('takes the settings and the trend from the welcome, then follows the messages', () => {
    const store = createLiveStore();
    store.getState().applyMessage(withSettings);
    expect(store.getState().settings?.version).toBe(3);
    expect(store.getState().trend?.points).toHaveLength(3);
    store.getState().applyMessage(message(readSettingsFixture('server_settings_update_demo.json')));
    expect(store.getState().settings?.version).toBe(4);
    store.getState().applyMessage(message(readSettingsFixture('server_trend_demo.json')));
    expect(store.getState().trend?.points.at(-1)?.waiting).toBe(141);
  });

  it('has neither from an older gateway, and ignores the messages a gateway with them would send', () => {
    const store = createLiveStore();
    store.getState().applyMessage(oldWelcome);
    store.getState().applyMessage(message(readSettingsFixture('server_settings_update_demo.json')));
    store.getState().applyMessage(message(readSettingsFixture('server_trend_demo.json')));
    expect(store.getState().settings).toBeNull();
    expect(store.getState().trend).toBeNull();
  });

  it('forgets both when a new connection reaches an older gateway', () => {
    const store = createLiveStore();
    store.getState().applyMessage(withSettings);
    store.getState().applyMessage({ ...(oldWelcome as Welcome) });
    expect(store.getState().settings).toBeNull();
    expect(store.getState().trend).toBeNull();
  });
});
