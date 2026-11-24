// @vitest-environment node
// Points of docs/gateway-protocol.md that the fixtures do not show.

import { readFixture } from '../test/fixtures.ts';
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
