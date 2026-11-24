import { readFixture } from '../test/fixtures.ts';
import { socketFactory } from '../test/fake-socket.ts';
import { buildOfferAction, buildWatch } from './build.ts';
import { GatewayClient, type ConnectionState, type ResumeTokenStore } from './client.ts';
import { backoffDelayMs, closeInfo } from './close-codes.ts';
import type { ServerMessage, View } from './types.ts';

const welcomeOperator = readFixture('server_welcome_operator_demo.json');
const welcomeSupervisor = readFixture('server_welcome_supervisor_full.json');

function memoryTokens(initial?: string): ResumeTokenStore & { value: string | undefined } {
  const store = {
    value: initial,
    load: () => store.value,
    save: (t: string) => {
      store.value = t;
    },
    clear: () => {
      store.value = undefined;
    },
  };
  return store;
}

function setup(view: View = 'operator', options: { token?: string; resume?: string } = {}) {
  const factory = socketFactory();
  const messages: ServerMessage[] = [];
  const states: ConnectionState[] = [];
  const tokens = memoryTokens(options.resume);
  const client = new GatewayClient({
    url: 'ws://gateway.test/v1/ws',
    view,
    getLanguages: () => ['en', 'und'],
    getToken: () => options.token,
    resumeTokens: tokens,
    onMessage: (m) => messages.push(m),
    onConnection: (s) => states.push(s),
    createSocket: factory.create,
    random: () => 0.5,
  });
  return { client, sockets: factory.sockets, messages, states, tokens };
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('GatewayClient handshake', () => {
  it('sends hello first with languages and the resume token, and keeps the new token', () => {
    const { client, sockets, tokens, states } = setup('operator', { resume: 'rt_old' });
    client.connect();
    sockets[0]?.serverOpen();
    expect(sockets[0]?.sent[0]).toEqual({
      type: 'hello',
      version: 1,
      view: 'operator',
      resume_token: 'rt_old',
      languages: ['en', 'und'],
    });
    sockets[0]?.serverSend(welcomeOperator);
    expect(tokens.value).toBe('rt_test_0000000000000001');
    expect(states.at(-1)?.phase).toBe('open');
    expect(client.level).toBe('demo');
  });

  it('a supervisor hello has the token and no languages', () => {
    const { client, sockets } = setup('supervisor', { token: 'synthetic.site.token' });
    client.connect();
    sockets[0]?.serverOpen();
    expect(sockets[0]?.sent[0]).toEqual({ type: 'hello', version: 1, view: 'supervisor', token: 'synthetic.site.token' });
  });

  it('closes with a protocol error when the first message is not a welcome', () => {
    const { client, sockets } = setup();
    client.connect();
    sockets[0]?.serverOpen();
    sockets[0]?.serverSend({ type: 'status', stale: false });
    expect(sockets[0]?.closedWith).toBe(4000);
  });

  it('stops and asks for a reload on close code 4426', () => {
    const { client, sockets, states } = setup();
    client.connect();
    sockets[0]?.serverOpen();
    sockets[0]?.serverClose(4426);
    expect(states.at(-1)).toMatchObject({ phase: 'stopped', closeCode: 4426 });
    vi.advanceTimersByTime(60_000);
    expect(sockets).toHaveLength(1);
  });

  it('skips a server message of an unknown type', () => {
    const { client, sockets, messages } = setup();
    client.connect();
    sockets[0]?.serverOpen();
    sockets[0]?.serverSend(welcomeOperator);
    sockets[0]?.serverSend({ type: 'something_new', value: 1 });
    expect(messages.map((m) => m.type)).toEqual(['welcome']);
    expect(sockets[0]?.closedWith).toBeNull();
  });

  it('refuses text at the demo level and closes', () => {
    const { client, sockets, messages } = setup();
    client.connect();
    sockets[0]?.serverOpen();
    sockets[0]?.serverSend(welcomeOperator);
    sockets[0]?.serverSend(readFixture('server_messages_snapshot_full.json'));
    expect(messages.map((m) => m.type)).toEqual(['welcome']);
    expect(sockets[0]?.closedWith).toBe(4000);
  });
});

describe('GatewayClient reconnect', () => {
  it('reconnects after a lost connection with backoff', () => {
    const { client, sockets, states } = setup();
    client.connect();
    sockets[0]?.serverOpen();
    sockets[0]?.serverSend(welcomeOperator);
    sockets[0]?.serverClose(1006);
    expect(states.at(-1)).toMatchObject({ phase: 'waiting', attempt: 1 });
    vi.advanceTimersByTime(backoffDelayMs({ attempt: 0, random: () => 0.5 }) - 1);
    expect(sockets).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(sockets).toHaveLength(2);
    sockets[1]?.serverClose(1001);
    expect(states.at(-1)).toMatchObject({ phase: 'waiting', attempt: 2 });
    vi.advanceTimersByTime(backoffDelayMs({ attempt: 1, random: () => 0.5 }));
    sockets[2]?.serverOpen();
    sockets[2]?.serverSend(welcomeOperator);
    expect(states.at(-1)).toMatchObject({ phase: 'open' });
  });

  it('keeps backing off when every connection fails right after welcome', () => {
    const { client, sockets, states } = setup();
    client.connect();
    const delays: number[] = [];
    for (let i = 0; i < 4; i += 1) {
      const socket = sockets.at(-1);
      socket?.serverOpen();
      socket?.serverSend(welcomeOperator);
      socket?.serverSend({ type: 'status', stale: 'not a boolean' });
      expect(socket?.closedWith).toBe(4000);
      socket?.serverClose(4000);
      const state = states.at(-1);
      delays.push((state?.retryAtMs ?? 0) - Date.now());
      vi.advanceTimersByTime(30_000);
    }
    expect(delays).toEqual([0, 1, 2, 3].map((attempt) => backoffDelayMs({ attempt, random: () => 0.5 })));
  });

  it('starts the backoff again after a connection that stayed open', () => {
    const { client, sockets, states } = setup();
    client.connect();
    sockets[0]?.serverOpen();
    sockets[0]?.serverClose(1006);
    vi.advanceTimersByTime(30_000);
    sockets[1]?.serverOpen();
    sockets[1]?.serverSend(welcomeOperator);
    vi.advanceTimersByTime(10_000);
    sockets[1]?.serverClose(1006);
    expect((states.at(-1)?.retryAtMs ?? 0) - Date.now()).toBe(backoffDelayMs({ attempt: 0, random: () => 0.5 }));
  });

  it('drops the site token after 4401 and connects at the demo level', () => {
    const { client, sockets } = setup('supervisor', { token: 'synthetic.site.token' });
    client.connect();
    sockets[0]?.serverOpen();
    sockets[0]?.serverClose(4401);
    vi.advanceTimersByTime(30_000);
    sockets[1]?.serverOpen();
    expect(sockets[1]?.sent[0]).toEqual({ type: 'hello', version: 1, view: 'supervisor' });
  });

  it('does not reconnect when another tab took the operator (4409)', () => {
    const { client, sockets, states } = setup();
    client.connect();
    sockets[0]?.serverOpen();
    sockets[0]?.serverSend(welcomeOperator);
    sockets[0]?.serverClose(4409);
    vi.advanceTimersByTime(60_000);
    expect(sockets).toHaveLength(1);
    expect(states.at(-1)?.close?.name).toBe('replaced');
  });

  it('follows the Go reconnect rule for every close code', () => {
    const reconnect = [1001, 1005, 1006, 1011, 1013, 4401, 4410, 4429, 4503, 4504];
    const stop = [1000, 1009, 4400, 4408, 4409, 4426];
    expect(reconnect.every((c) => closeInfo(c).reaction.startsWith('reconnect'))).toBe(true);
    expect(stop.every((c) => !closeInfo(c).reaction.startsWith('reconnect'))).toBe(true);
  });

  it('backoff doubles up to 30 seconds', () => {
    const delays = [0, 1, 2, 3, 4, 5, 6].map((attempt) => backoffDelayMs({ attempt, random: () => 1 }));
    expect(delays).toEqual([1000, 2000, 4000, 8000, 16_000, 30_000, 30_000]);
  });
});

describe('GatewayClient requests', () => {
  function connected(view: View = 'operator') {
    const ctx = setup(view);
    ctx.client.connect();
    ctx.sockets[0]?.serverOpen();
    ctx.sockets[0]?.serverSend(view === 'operator' ? welcomeOperator : welcomeSupervisor);
    return ctx;
  }

  it('resolves each request with its own result, in any order', async () => {
    const { client, sockets } = connected();
    const first = client.request(buildOfferAction('req-a', 'accept', 'of-1'));
    const second = client.request(buildWatch('req-b', 'ps-k2m3n4p5q6r7'));
    sockets[0]?.serverSend({ type: 'result', request_id: 'req-b', outcome: 'done' });
    sockets[0]?.serverSend({ type: 'result', request_id: 'req-a', outcome: 'rejected', code: 'offer_expired' });
    await expect(second).resolves.toMatchObject({ outcome: 'done' });
    await expect(first).resolves.toMatchObject({ outcome: 'rejected', code: 'offer_expired' });
  });

  it('answers locally when not connected', async () => {
    const { client } = setup();
    await expect(client.request(buildWatch('req-x', 'ps-k2m3n4p5q6r7'))).resolves.toMatchObject({
      outcome: 'rejected',
      code: 'not_connected',
    });
  });

  it('sends a reply again with the same request ID after a reconnect', async () => {
    const { client, sockets } = connected();
    const accept = client.request(buildOfferAction('req-accept', 'accept', 'of-1'));
    const reply = client.request(buildOfferAction('req-reply', 'reply', 'of-1'));
    sockets[0]?.serverClose(1006);
    await expect(accept).resolves.toMatchObject({ code: 'connection_lost' });
    vi.advanceTimersByTime(30_000);
    sockets[1]?.serverOpen();
    sockets[1]?.serverSend(welcomeOperator);
    expect(sockets[1]?.lastSent()).toEqual({ type: 'action', request_id: 'req-reply', action: 'reply', offer_id: 'of-1' });
    sockets[1]?.serverSend({ type: 'result', request_id: 'req-reply', outcome: 'already_done' });
    await expect(reply).resolves.toMatchObject({ outcome: 'already_done' });
  });

  it('makes valid, unique request IDs', () => {
    const { client } = setup();
    const ids = new Set(Array.from({ length: 50 }, () => client.nextRequestId()));
    expect(ids.size).toBe(50);
    expect([...ids].every((id) => /^[A-Za-z0-9._-]{1,64}$/.test(id))).toBe(true);
  });
});
