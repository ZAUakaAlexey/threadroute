// The session against an in-memory socket: watches that the gateway ends,
// repeated watches, and a snapshot in several frames (docs/gateway-protocol.md,
// "watch" and "A snapshot in several frames").

import { readFixture } from '../test/fixtures.ts';
import { socketFactory, type FakeSocket } from '../test/fake-socket.ts';
import type { View } from '../protocol/types.ts';
import { createLiveSession, type LiveSession } from './session.ts';

interface Row {
  readonly id: string;
  readonly [key: string]: unknown;
}

const operatorSnapshot = readFixture('server_state_operator_snapshot_demo.json') as { my_conversations: { set: Row[] } };
const supervisorSnapshot = readFixture('server_state_supervisor_snapshot_full.json') as {
  conversations: { set: Row[] };
};
const HELD = 'ps-k2m3n4p5q6r7';
const [ROW_A, ROW_B] = supervisorSnapshot.conversations.set as [Row, Row];

function start(view: View): { session: LiveSession; socket: FakeSocket } {
  const factory = socketFactory();
  const session = createLiveSession({
    view,
    url: 'ws://gateway.test/v1/ws',
    getLanguages: () => ['en', 'und'],
    getToken: () => undefined,
    resumeTokens: { load: () => undefined, save: () => undefined, clear: () => undefined },
    createSocket: factory.create,
  });
  session.connect();
  const socket = factory.sockets[0]!;
  socket.serverOpen();
  socket.serverSend(readFixture(view === 'operator' ? 'server_welcome_operator_demo.json' : 'server_welcome_supervisor_full.json'));
  socket.serverSend({ type: 'status', stale: false });
  return { session, socket };
}

function sentOfType(socket: FakeSocket, type: string): Record<string, unknown>[] {
  return (socket.sent as Record<string, unknown>[]).filter((m) => m.type === type);
}

function answerLast(socket: FakeSocket, type: string, answer: Record<string, unknown>): void {
  const request = sentOfType(socket, type).at(-1);
  socket.serverSend({ type: 'result', request_id: request?.request_id, ...answer });
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

function messagesFrame(conversationId: string, snapshot: boolean, ids: readonly string[]) {
  return {
    type: 'messages',
    conversation_id: conversationId,
    snapshot,
    messages: ids.map((id, i) => ({ id, direction: 'customer', source_us: 1_767_323_000_000_000 + i, length: 5, edited: false })),
  };
}

describe('live session, watches the gateway ends', () => {
  it('drops the messages and sends an unwatch whose already_done shows no notice', async () => {
    const { session, socket } = start('operator');
    socket.serverSend(operatorSnapshot);
    // The operator sees the conversation they hold at once.
    expect(session.store.getState().selectedId).toBe(HELD);
    expect(sentOfType(socket, 'watch').at(-1)).toMatchObject({ conversation_id: HELD });
    answerLast(socket, 'watch', { outcome: 'done' });
    socket.serverSend(messagesFrame(HELD, true, ['m1']));
    await flush();
    expect(session.store.getState().messages.get(HELD)).toHaveLength(1);

    // Resolved: the gateway ends the watch by itself, without a message.
    socket.serverSend({ type: 'state', seq: 2, snapshot: false, server_time_us: 1_767_323_100_250_001, my_conversations: { remove: [HELD] } });
    const s = session.store.getState();
    expect(s.selectedId).toBeNull();
    expect(s.watched.has(HELD)).toBe(false);
    expect(s.messages.has(HELD)).toBe(false);
    expect(sentOfType(socket, 'unwatch').at(-1)).toMatchObject({ conversation_id: HELD });
    answerLast(socket, 'unwatch', { outcome: 'already_done' });
    await flush();
    expect(session.store.getState().notice).toBeNull();
  });

  it('keeps a repeated watch that gets already_done and takes its new snapshot', async () => {
    const { session, socket } = start('operator');
    socket.serverSend(operatorSnapshot);
    answerLast(socket, 'watch', { outcome: 'done' });
    socket.serverSend(messagesFrame(HELD, true, ['m1', 'm2']));
    await flush();

    // A later snapshot ends the watch; the session watches again.
    socket.serverSend({ ...operatorSnapshot, seq: 2 });
    expect(sentOfType(socket, 'watch')).toHaveLength(2);
    answerLast(socket, 'watch', { outcome: 'already_done' });
    socket.serverSend(messagesFrame(HELD, true, ['m3']));
    await flush();
    const s = session.store.getState();
    expect(s.watched.has(HELD)).toBe(true);
    expect(s.messages.get(HELD)?.map((m) => m.id)).toEqual(['m3']);
    expect(s.notice).toBeNull();
  });
});

describe('live session, a snapshot in several frames', () => {
  it('keeps the shown conversation when its row comes in a later frame', async () => {
    const { session, socket } = start('supervisor');
    socket.serverSend({ type: 'state', seq: 1, snapshot: true, server_time_us: 1_767_323_100_000_001, conversations: { set: [ROW_A, ROW_B] } });
    session.select(ROW_B.id);
    answerLast(socket, 'watch', { outcome: 'done' });
    await flush();

    // The gateway rebuilt its view: a new snapshot in two frames.
    socket.serverSend({ type: 'state', seq: 2, snapshot: true, server_time_us: 1_767_323_100_000_002, conversations: { set: [ROW_A] } });
    expect(session.store.getState().selectedId).toBe(ROW_B.id);
    expect(sentOfType(socket, 'watch').at(-1)).toMatchObject({ conversation_id: ROW_B.id });
    socket.serverSend({ type: 'state', seq: 3, snapshot: false, server_time_us: 1_767_323_100_000_003, conversations: { set: [ROW_B] } });
    answerLast(socket, 'watch', { outcome: 'done' });
    await flush();
    const s = session.store.getState();
    expect([...s.conversations.keys()]).toEqual([ROW_A.id, ROW_B.id]);
    expect(s.selectedId).toBe(ROW_B.id);
    expect(s.watched.has(ROW_B.id)).toBe(true);
    expect(sentOfType(socket, 'unwatch')).toHaveLength(0);
  });

  it('lets the selection go, without a notice, when the gateway no longer knows it', async () => {
    const { session, socket } = start('supervisor');
    socket.serverSend({ type: 'state', seq: 1, snapshot: true, server_time_us: 1_767_323_100_000_001, conversations: { set: [ROW_A, ROW_B] } });
    session.select(ROW_B.id);
    answerLast(socket, 'watch', { outcome: 'done' });
    await flush();

    socket.serverSend({ type: 'state', seq: 2, snapshot: true, server_time_us: 1_767_323_100_000_002, conversations: { set: [ROW_A] } });
    answerLast(socket, 'watch', { outcome: 'rejected', code: 'unknown_conversation', message: 'the conversation is not in this view or may not be watched' });
    await flush();
    const s = session.store.getState();
    expect(s.selectedId).toBeNull();
    expect(s.watched.size).toBe(0);
    expect(s.notice).toBeNull();
  });
});

describe('live session, stale_connection', () => {
  it('shows the cause the gateway names', async () => {
    const { session, socket } = start('operator');
    socket.serverSend(operatorSnapshot);
    const done = session.offerAction('accept', 'of-102');
    answerLast(socket, 'action', { outcome: 'rejected', code: 'stale_connection', message: 'accept: the gateway data is stale' });
    await done;
    expect(session.store.getState().notice?.text).toMatch(/^The data is stale/);
  });
});
