// @vitest-environment node
// Drives the fake gateway over a real WebSocket at both levels. Every frame
// the fake sends passed the console parser already; here the flows and the
// privacy of the demo level are checked.

import WebSocket from 'ws';

import { parseServerMessage } from '../src/protocol/parse.ts';
import type { ServerMessage } from '../src/protocol/types.ts';
import { FULL_TOKEN, startFakeGateway, type FakeGateway } from './server.ts';
import { PSEUDONYM_RE } from './synthetic.ts';

let gateway: FakeGateway;

beforeEach(async () => {
  gateway = await startFakeGateway({ port: 0, seed: 11, speedFactor: 4, isManual: true, tickMs: 50 });
});

afterEach(async () => {
  await gateway.close();
});

interface Peer {
  readonly frames: string[];
  readonly messages: ServerMessage[];
  send(message: unknown): void;
  next(predicate: (m: ServerMessage) => boolean): Promise<ServerMessage>;
  closed: Promise<number>;
  close(): void;
}

async function connect(hello: Record<string, unknown>): Promise<Peer> {
  const ws = new WebSocket(`${gateway.url.replace('http', 'ws')}/v1/ws`);
  const frames: string[] = [];
  const messages: ServerMessage[] = [];
  const waiters: { predicate: (m: ServerMessage) => boolean; resolve: (m: ServerMessage) => void }[] = [];
  const closed = new Promise<number>((resolve) => ws.on('close', (code) => resolve(code)));
  ws.on('message', (data: Buffer) => {
    const frame = data.toString('utf8');
    frames.push(frame);
    const parsed = parseServerMessage(frame);
    if (parsed.kind !== 'message') return;
    const waiter = waiters.find((w) => w.predicate(parsed.message));
    if (waiter === undefined) {
      messages.push(parsed.message);
      return;
    }
    waiters.splice(waiters.indexOf(waiter), 1);
    waiter.resolve(parsed.message);
  });
  await new Promise((resolve) => ws.on('open', resolve));
  ws.send(JSON.stringify({ type: 'hello', version: 1, ...hello }));
  return {
    frames,
    messages,
    closed,
    send: (m) => ws.send(JSON.stringify(m)),
    close: () => ws.close(),
    next: (predicate) =>
      new Promise((resolve, reject) => {
        const found = messages.find(predicate);
        if (found !== undefined) {
          messages.splice(messages.indexOf(found), 1);
          resolve(found);
          return;
        }
        const timer = setTimeout(() => reject(new Error('timeout')), 3000);
        waiters.push({
          predicate,
          resolve: (m) => {
            clearTimeout(timer);
            resolve(m);
          },
        });
      }),
  };
}

const isType = <T extends ServerMessage['type']>(type: T) => (m: ServerMessage): m is Extract<ServerMessage, { type: T }> => m.type === type;
const isResult = (id: string) => (m: ServerMessage) => m.type === 'result' && m.request_id === id;

describe('fake gateway', () => {
  it('runs an operator through offer, accept, watch, reply and resolve at the demo level', async () => {
    const peer = await connect({ view: 'operator', languages: ['en', 'und'] });
    const welcome = await peer.next(isType('welcome'));
    expect(welcome).toMatchObject({ level: 'demo', view: 'operator', speed_factor: 4 });
    await peer.next(isType('status'));
    await peer.next((m) => m.type === 'state' && m.snapshot);

    peer.send({ type: 'action', request_id: 'r1', action: 'online' });
    expect(await peer.next(isResult('r1'))).toMatchObject({ outcome: 'done' });
    const offer = (await (await fetch(`${gateway.url}/__control/offer`, { method: 'POST' })).json()) as { offer_id: string };
    expect(offer.offer_id).toMatch(/^of-/);

    peer.send({ type: 'action', request_id: 'r2', action: 'accept', offer_id: offer.offer_id });
    expect(await peer.next(isResult('r2'))).toMatchObject({ outcome: 'done' });
    const state = await peer.next(
      (m) => m.type === 'state' && (m.my_conversations?.set ?? []).some((c) => c.state === 'assigned'),
    );
    const held = state.type === 'state' ? state.my_conversations?.set?.[0] : undefined;
    expect(held?.id).toMatch(PSEUDONYM_RE);

    peer.send({ type: 'watch', request_id: 'r3', conversation_id: held?.id });
    expect(await peer.next(isResult('r3'))).toMatchObject({ outcome: 'done' });
    const messages = await peer.next(isType('messages'));
    expect(messages.type === 'messages' && messages.messages.every((m) => m.text === undefined)).toBe(true);

    await fetch(`${gateway.url}/__control/remove-message`, { method: 'POST' });
    expect(await peer.next(isType('message_removed'))).toMatchObject({ reason: 'deleted' });

    peer.send({ type: 'action', request_id: 'r4', action: 'reply', offer_id: offer.offer_id });
    expect(await peer.next(isResult('r4'))).toMatchObject({ outcome: 'done' });
    peer.send({ type: 'action', request_id: 'r4', action: 'reply', offer_id: offer.offer_id });
    expect(await peer.next(isResult('r4'))).toMatchObject({ outcome: 'already_done' });
    peer.send({ type: 'action', request_id: 'r5', action: 'resolve', offer_id: offer.offer_id });
    expect(await peer.next(isResult('r5'))).toMatchObject({ outcome: 'done' });

    for (const frame of peer.frames) {
      expect(frame).not.toContain('"text"');
      expect(frame).not.toContain('did:');
    }
    peer.close();
  });

  it('answers repeated watches and unwatches like the real gateway, and ends a watch by itself', async () => {
    const peer = await connect({ view: 'operator', languages: ['en'] });
    await peer.next((m) => m.type === 'state' && m.snapshot);
    peer.send({ type: 'action', request_id: 'on', action: 'online' });
    await peer.next(isResult('on'));
    peer.send({ type: 'unwatch', request_id: 'u0', conversation_id: 'ps-aaaaaaaaaaaa' });
    expect(await peer.next(isResult('u0'))).toMatchObject({ outcome: 'already_done' });

    const offer = (await (await fetch(`${gateway.url}/__control/offer`, { method: 'POST' })).json()) as { offer_id: string };
    peer.send({ type: 'action', request_id: 'acc', action: 'accept', offer_id: offer.offer_id });
    await peer.next(isResult('acc'));
    const state = await peer.next((m) => m.type === 'state' && (m.my_conversations?.set ?? []).some((c) => c.state === 'assigned'));
    const id = state.type === 'state' ? (state.my_conversations?.set?.[0]?.id ?? '') : '';

    peer.send({ type: 'watch', request_id: 'w1', conversation_id: id });
    expect(await peer.next(isResult('w1'))).toMatchObject({ outcome: 'done' });
    expect(await peer.next(isType('messages'))).toMatchObject({ conversation_id: id, snapshot: true });
    peer.send({ type: 'watch', request_id: 'w2', conversation_id: id });
    expect(await peer.next(isResult('w2'))).toMatchObject({ outcome: 'already_done' });
    expect(await peer.next(isType('messages'))).toMatchObject({ conversation_id: id, snapshot: true });

    // Resolving ends the watch without a message.
    const framesBefore = peer.frames.length;
    peer.send({ type: 'action', request_id: 'res', action: 'resolve', offer_id: offer.offer_id });
    await peer.next(isResult('res'));
    await peer.next((m) => m.type === 'state' && (m.my_conversations?.remove ?? []).includes(id));
    expect(peer.frames.slice(framesBefore).some((f) => f.includes('"messages"') || f.includes('message_removed'))).toBe(false);
    peer.send({ type: 'unwatch', request_id: 'u1', conversation_id: id });
    expect(await peer.next(isResult('u1'))).toMatchObject({ outcome: 'already_done' });
    peer.send({ type: 'watch', request_id: 'w3', conversation_id: id });
    expect(await peer.next(isResult('w3'))).toMatchObject({ outcome: 'rejected', code: 'unknown_conversation' });
    peer.close();
  });

  it('rejects only offer actions while stale, with the gateway text', async () => {
    const peer = await connect({ view: 'operator', languages: ['en'] });
    await peer.next((m) => m.type === 'state' && m.snapshot);
    await fetch(`${gateway.url}/__control/stale`, { method: 'POST', body: JSON.stringify({ stale: true }) });
    expect(await peer.next((m) => m.type === 'status' && m.stale)).toMatchObject({ reason: 'source_unavailable', lag_us: 4_000_000 });
    peer.send({ type: 'action', request_id: 'on', action: 'online' });
    expect(await peer.next(isResult('on'))).toMatchObject({ outcome: 'done' });
    peer.send({ type: 'action', request_id: 'acc', action: 'accept', offer_id: 'of-1' });
    expect(await peer.next(isResult('acc'))).toMatchObject({
      outcome: 'rejected',
      code: 'stale_connection',
      message: 'accept: the gateway data is stale',
    });
    peer.close();
  });

  it('gives a rejected token the demo level, like the real gateway', async () => {
    const peer = await connect({ view: 'supervisor', token: 'synthetic.rejected.token' });
    expect(await peer.next(isType('welcome'))).toMatchObject({ level: 'demo' });
    peer.close();
  });

  it('sends the code name as the close reason', async () => {
    const ws = new WebSocket(`${gateway.url.replace('http', 'ws')}/v1/ws`);
    const closed = new Promise<[number, string]>((resolve) => ws.on('close', (code, reason) => resolve([code, reason.toString('utf8')])));
    await new Promise((resolve) => ws.on('open', resolve));
    ws.send(JSON.stringify({ type: 'hello', version: 1, view: 'supervisor' }));
    await new Promise((resolve) => ws.once('message', resolve));
    await fetch(`${gateway.url}/__control/close`, { method: 'POST', body: JSON.stringify({ code: 1013 }) });
    expect(await closed).toEqual([1013, 'try_again_later']);
  });

  it('lets an offer expire', async () => {
    const peer = await connect({ view: 'operator', languages: ['en'] });
    await peer.next((m) => m.type === 'state' && m.snapshot);
    peer.send({ type: 'action', request_id: 'on', action: 'online' });
    await peer.next(isResult('on'));
    const offer = (await (await fetch(`${gateway.url}/__control/offer`, { method: 'POST' })).json()) as { offer_id: string };
    // 20 s of model time at speed 4 are 5 s of machine time.
    await new Promise((resolve) => setTimeout(resolve, 5200));
    peer.send({ type: 'action', request_id: 'late', action: 'accept', offer_id: offer.offer_id });
    expect(await peer.next(isResult('late'))).toMatchObject({ outcome: 'rejected', code: 'unknown_offer' });
    peer.close();
  }, 10_000);

  it('sends text and real keys at the full level', async () => {
    const peer = await connect({ view: 'supervisor', token: FULL_TOKEN });
    expect(await peer.next(isType('welcome'))).toMatchObject({ level: 'full' });
    const snapshot = await peer.next((m) => m.type === 'state' && m.snapshot);
    const row = snapshot.type === 'state' ? snapshot.conversations?.set?.[0] : undefined;
    expect(row?.id).toContain('did:plc:synth');
    peer.send({ type: 'watch', request_id: 'w1', conversation_id: row?.id });
    const messages = await peer.next(isType('messages'));
    expect(messages.type === 'messages' && messages.messages.every((m) => typeof m.text === 'string')).toBe(true);
    peer.close();
  });

  it('closes on a wrong version, a bad message, and a sixth demo operator', async () => {
    const old = await connect({ version: 2, view: 'supervisor' });
    expect(await old.closed).toBe(4426);
    const bad = await connect({ view: 'operator' });
    expect(await bad.closed).toBe(4400);
    const peers = await Promise.all([1, 2, 3, 4, 5].map(() => connect({ view: 'operator', languages: ['en'] })));
    await Promise.all(peers.map((p) => p.next(isType('welcome'))));
    const sixth = await connect({ view: 'operator', languages: ['en'] });
    expect(await sixth.closed).toBe(4503);
    for (const p of peers) p.close();
  });

  it('reports stale and fresh status to every connection', async () => {
    const peer = await connect({ view: 'supervisor' });
    await peer.next(isType('status'));
    await fetch(`${gateway.url}/__control/stale`, { method: 'POST', body: JSON.stringify({ stale: true }) });
    expect(await peer.next(isType('status'))).toMatchObject({ stale: true, reason: 'source_unavailable' });
    await fetch(`${gateway.url}/__control/stale`, { method: 'POST', body: JSON.stringify({ stale: false }) });
    expect(await peer.next(isType('status'))).toMatchObject({ stale: false });
    peer.close();
  });

  it('serves a public snapshot with totals only', async () => {
    const body = await (await fetch(`${gateway.url}/v1/public/snapshot`)).text();
    expect(Object.keys(JSON.parse(body) as object).sort()).toEqual(['generated_at_us', 'queues', 'speed_factor', 'stale', 'totals', 'version']);
    expect(body).not.toMatch(/did:|ps-|sim-op|web-/);
  });
});

describe('fake gateway, a snapshot in several frames', () => {
  it('sends the first frame as the snapshot and the other rows as updates', async () => {
    await gateway.close();
    gateway = await startFakeGateway({ port: 0, seed: 11, speedFactor: 4, isManual: true, tickMs: 60_000, snapshotFrameRows: 2 });
    const total = gateway.world.conversations.size;
    expect(total).toBeGreaterThan(4);
    const peer = await connect({ view: 'supervisor' });
    const states: Extract<ServerMessage, { type: 'state' }>[] = [];
    while (states.reduce((n, m) => n + (m.conversations?.set?.length ?? 0), 0) < total) {
      const m = await peer.next(isType('state'));
      if (m.type === 'state') states.push(m);
    }
    expect(states.map((m) => m.seq)).toEqual(states.map((_, i) => i + 1));
    expect(states.map((m) => m.snapshot)).toEqual(states.map((_, i) => i === 0));
    expect(states.every((m) => (m.conversations?.set?.length ?? 0) <= 2)).toBe(true);
    expect(states[0]?.operators?.set?.length).toBeGreaterThan(0);
    expect(states.slice(1).every((m) => Object.keys(m).sort().join() === 'conversations,seq,server_time_us,snapshot,type')).toBe(true);

    // A later snapshot (the gateway rebuilt its view) is split the same way.
    await fetch(`${gateway.url}/__control/snapshot`, { method: 'POST' });
    const again = await peer.next((m) => m.type === 'state' && m.snapshot);
    expect(again.type === 'state' && again.seq).toBe(states.length + 1);
    peer.close();
  });
});

describe('fake gateway scenario', () => {
  it('ends the stale spell that it starts', async () => {
    const { World } = await import('./world.ts');
    const start = 1_000_000_000_000;
    const world = new World({ seed: 1, speedFactor: 1, isManual: false }, start);
    const seen: boolean[] = [];
    for (let t = 0; t <= 120_000_000; t += 250_000) {
      world.step(start + t);
      if (seen.at(-1) !== world.status.stale) seen.push(world.status.stale);
    }
    expect(seen).toEqual([false, true, false]);
  });
});

describe('fake gateway, stand settings (decision 0014)', () => {
  const control = (name: string, body: unknown = {}) => fetch(`${gateway.url}/__control/${name}`, { method: 'POST', body: JSON.stringify(body) });

  async function supervisor() {
    const peer = await connect({ view: 'supervisor' });
    const welcome = await peer.next(isType('welcome'));
    if (welcome.type !== 'welcome' || welcome.settings === undefined) throw new Error('no settings in the welcome');
    return { peer, welcome, settings: welcome.settings };
  }

  it('sends settings with bounds from the Go example and a trend history', async () => {
    const { peer, welcome, settings } = await supervisor();
    expect(settings.bounds.teams.form_load).toMatchObject({ min: 2, max: 500, integer: true });
    expect(settings.may_change).toBe(true);
    expect(welcome.trend?.interval_ms).toBe(10_000);
    expect(welcome.trend?.points.length).toBeGreaterThan(300);
    peer.close();
  });

  it('applies a change: the settings message reaches every connection before the result', async () => {
    const { peer, settings } = await supervisor();
    const other = await connect({ view: 'operator', languages: ['en'] });
    await other.next(isType('welcome'));
    const teams = { ...settings.current.teams, max_share: 0.3 };
    peer.send({ type: 'change_settings', request_id: 's1', base_version: settings.version, settings: { ...settings.current, teams } });
    const pushed = await peer.next(isType('settings'));
    expect(pushed.type === 'settings' && pushed.settings.version).toBe(settings.version + 1);
    expect(await peer.next(isResult('s1'))).toMatchObject({ outcome: 'done' });
    const seen = peer.frames.map((f) => JSON.parse(f) as { type: string; request_id?: string });
    expect(seen.findIndex((m) => m.type === 'settings')).toBeLessThan(seen.findIndex((m) => m.request_id === 's1'));
    expect(await other.next(isType('settings'))).toMatchObject({ settings: { current: { teams: { max_share: 0.3 } } } });

    // The same set again is already done; a second change within 10 s is too soon.
    peer.send({ type: 'change_settings', request_id: 's2', base_version: settings.version + 1, settings: { ...settings.current, teams } });
    expect(await peer.next(isResult('s2'))).toMatchObject({ outcome: 'already_done' });
    peer.send({ type: 'change_settings', request_id: 's3', base_version: settings.version + 1, settings: { ...settings.current, teams: { ...teams, max_share: 0.4 } } });
    const tooSoon = await peer.next(isResult('s3'));
    expect(tooSoon).toMatchObject({ code: 'settings_too_soon' });
    expect(tooSoon.type === 'result' && (tooSoon.retry_after_ms ?? 0) > 0).toBe(true);
    peer.close();
    other.close();
  });

  it('answers invalid values with fields, an old base with a conflict, and no right with not_allowed', async () => {
    const { peer, settings } = await supervisor();
    peer.send({ type: 'change_settings', request_id: 'i1', base_version: settings.version, settings: { ...settings.current, teams: { ...settings.current.teams, release_load: 900 } } });
    expect(await peer.next(isResult('i1'))).toMatchObject({ code: 'invalid_request', fields: [{ field: 'teams.release_load', reason: '900 is not between 1 and 499' }] });
    peer.send({ type: 'change_settings', request_id: 'i2', base_version: settings.version - 1, settings: { ...settings.current, teams: { ...settings.current.teams, max_size: 19 } } });
    expect(await peer.next(isResult('i2'))).toMatchObject({ code: 'settings_conflict' });
    await control('settings-answer', { answer: 'off' });
    peer.send({ type: 'change_settings', request_id: 'i3', base_version: settings.version, settings: settings.current });
    expect(await peer.next(isResult('i3'))).toMatchObject({ code: 'settings_off' });
    peer.close();

    await control('settings-right', { may_change: false });
    const reader = await supervisor();
    expect(reader.settings.may_change).toBe(false);
    expect(reader.settings.changes.every((c) => c.by === undefined)).toBe(true);
    reader.peer.send({ type: 'change_settings', request_id: 'i4', base_version: reader.settings.version, settings: reader.settings.current });
    expect(await reader.peer.next(isResult('i4'))).toMatchObject({ code: 'not_allowed' });
    reader.peer.close();
  });

  it('acts like an older gateway in the old mode: no settings, no trend, and 4400 on change_settings', async () => {
    await control('settings-mode', { mode: 'old' });
    const peer = await connect({ view: 'supervisor' });
    const welcome = await peer.next(isType('welcome'));
    expect(welcome).not.toHaveProperty('settings');
    expect(welcome).not.toHaveProperty('trend');
    await control('trend-point');
    peer.send({ type: 'change_settings', request_id: 'o1', base_version: 0, settings: { teams: { form_load: 20, release_load: 8, release_after_us: 900000000, min_size: 2, max_size: 20, max_share: 0.5 } } });
    expect(await peer.closed).toBe(4400);
    expect(peer.frames.some((f) => f.includes('"trend"') || f.includes('"settings"'))).toBe(false);
  });

  it('moves the numbers: fewer simulated operators go offline, and the trend shows it', async () => {
    const { peer, settings } = await supervisor();
    const sim = settings.current.sim;
    if (sim === undefined) throw new Error('no sim part');
    const before = gateway.world.simCount();
    peer.send({ type: 'change_settings', request_id: 'm1', base_version: settings.version, settings: { ...settings.current, sim: { ...sim, operators: 10 } } });
    expect(await peer.next(isResult('m1'))).toMatchObject({ outcome: 'done' });
    expect(before).toBeGreaterThan(10);
    expect(gateway.world.simCount()).toBe(10);
    await control('trend-point');
    const point = await peer.next(isType('trend'));
    expect(point.type === 'trend' && point.point.operators_online).toBeLessThanOrEqual(10);
    peer.close();
  });

  it('moves the teams toward a new team share, one member per trend point', async () => {
    const { peer, settings } = await supervisor();
    const inTeams = async () => {
      await control('trend-point');
      const m = await peer.next(isType('trend'));
      return m.type === 'trend' ? m.point.operators_in_teams : -1;
    };
    const start = await inTeams();
    peer.send({ type: 'change_settings', request_id: 't1', base_version: settings.version, settings: { ...settings.current, teams: { ...settings.current.teams, max_share: 0.05 } } });
    expect(await peer.next(isResult('t1'))).toMatchObject({ outcome: 'done' });
    const lower = [await inTeams(), await inTeams(), await inTeams()];
    expect(lower[0]).toBe(start - 1);
    expect(lower[2]).toBe(1);
    await control('settings-ready');
    peer.send({ type: 'change_settings', request_id: 't2', base_version: settings.version + 1, settings: { ...settings.current, teams: { ...settings.current.teams, max_share: 0.5 } } });
    expect(await peer.next(isResult('t2'))).toMatchObject({ outcome: 'done' });
    const higher = [await inTeams(), await inTeams()];
    expect(higher).toEqual([2, 3]);
    peer.close();
  });
});
