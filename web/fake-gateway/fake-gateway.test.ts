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
