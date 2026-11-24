// @vitest-environment node
// Points of docs/gateway-protocol.md that the fixtures do not show.

import { readFixture, readSettingsFixture } from '../test/fixtures.ts';
import { buildChangeSettings } from './build.ts';
import { ProtocolError, parseServerMessage } from './parse.ts';

function parse(message: unknown) {
  return parseServerMessage(JSON.stringify(message), { level: 'full' });
}

describe('parseServerMessage, enum values', () => {
  const snapshot = readFixture('server_state_supervisor_snapshot_full.json') as {
    conversations: { set: Record<string, unknown>[] };
  };
  const row = snapshot.conversations.set[0] ?? {};

  it.each([
    ['result code', { type: 'result', request_id: 'r1', outcome: 'rejected', code: 'something_new' }],
    ['result outcome', { type: 'result', request_id: 'r1', outcome: 'maybe' }],
    ['status reason', { type: 'status', stale: true, reason: 'resting' }],
    ['message_removed reason', { type: 'message_removed', conversation_id: 'c1', message_id: 'm1', reason: 'hidden' }],
    [
      'conversation state',
      { type: 'state', seq: 2, snapshot: false, server_time_us: 1, conversations: { set: [{ ...row, state: 'parked' }] } },
    ],
  ])('refuses an unknown %s as a broken frame', (_name, message) => {
    expect(() => parse(message)).toThrow(ProtocolError);
  });

  it('accepts every result code of version 1', () => {
    const parsed = parse({ type: 'result', request_id: 'r1', outcome: 'rejected', code: 'stale_connection', message: 'accept: the gateway data is stale' });
    expect(parsed).toMatchObject({ kind: 'message', message: { code: 'stale_connection' } });
  });
});

describe('parseServerMessage, status', () => {
  it.each(['catching_up', 'source_unavailable', 'lagging'])('accepts lag_us with the reason %s', (reason) => {
    expect(parse({ type: 'status', stale: true, reason, lag_us: 4_000_000 })).toMatchObject({
      message: { reason, lag_us: 4_000_000 },
    });
  });
});

describe('parseServerMessage, a snapshot in several frames', () => {
  it('takes the later frames as updates with the next seq', () => {
    const first = parse({ type: 'state', seq: 1, snapshot: true, server_time_us: 1 });
    const next = parse({ type: 'state', seq: 2, snapshot: false, server_time_us: 1, conversations: { set: [] } });
    expect([first, next].map((p) => (p.kind === 'message' && p.message.type === 'state' ? p.message.snapshot : null))).toEqual([true, false]);
  });
});

describe('stand settings (decision 0014)', () => {
  const welcome = readSettingsFixture('server_welcome_supervisor_settings_full_may_change.json') as { settings: { defaults: Record<string, unknown> } } & Record<string, unknown>;

  it('refuses a block whose defaults have a sim part when the current set has none', () => {
    const first = readSettingsFixture('server_settings_first_start_demo.json') as { settings: Record<string, unknown> };
    const broken = { type: 'settings', settings: { ...first.settings, defaults: welcome.settings.defaults } };
    expect(() => parse(broken)).toThrow(ProtocolError);
  });

  it('refuses a fraction in a count of the set', () => {
    const settings = welcome.settings as unknown as { current: { teams: Record<string, number> } };
    const broken = { ...welcome, settings: { ...welcome.settings, current: { ...settings.current, teams: { ...settings.current.teams, form_load: 20.5 } } } };
    expect(() => parse(broken)).toThrow(ProtocolError);
  });

  it('reads fields and retry_after_ms of a result, and the three new codes', () => {
    for (const code of ['settings_conflict', 'settings_too_soon', 'settings_off']) {
      expect(parse({ type: 'result', request_id: 'r1', outcome: 'rejected', code })).toMatchObject({ message: { code } });
    }
    expect(() => parse({ type: 'result', request_id: 'r1', outcome: 'rejected', code: 'settings_too_soon', retry_after_ms: 0 })).toThrow(ProtocolError);
  });
});

describe('buildChangeSettings', () => {
  const teams = { form_load: 20, release_load: 8, release_after_us: 900_000_000, min_size: 2, max_size: 20, max_share: 0.5 };

  it('refuses a fraction in an integer field, so the gateway never closes with 4400', () => {
    expect(() => buildChangeSettings('r1', 0, { teams: { ...teams, release_after_us: 900_000_000.5 } })).toThrow(ProtocolError);
    expect(() => buildChangeSettings('r1', -1, { teams })).toThrow(ProtocolError);
  });

  it('copies only the fields of the set', () => {
    const extra = { ...teams, note: 1 } as typeof teams;
    expect(buildChangeSettings('r1', 0, { teams: extra }).settings).toEqual({ teams });
  });
});
