import { parseServerMessage } from '../protocol/parse.ts';
import type { Level, ServerMessage } from '../protocol/types.ts';
import { readFixtureText } from '../test/fixtures.ts';
import { selectHeld, selectOffers } from './selectors.ts';
import { createLiveStore, isLive, type LiveStore } from './store.ts';

const NOW_MS = 1_767_323_100_000;

function fixture(file: string, level: Level): ServerMessage {
  const parsed = parseServerMessage(readFixtureText(file), { level });
  if (parsed.kind !== 'message') throw new Error(`fixture ${file} is not a known message`);
  return parsed.message;
}

function openStore(welcomeFile: string, level: Level): LiveStore {
  const store = createLiveStore();
  store.getState().setConnection({ phase: 'open', attempt: 0 }, NOW_MS);
  store.getState().applyMessage(fixture(welcomeFile, level), NOW_MS);
  return store;
}

function storageText(): string {
  const parts: string[] = [];
  for (const storage of [window.localStorage, window.sessionStorage]) {
    for (let i = 0; i < storage.length; i += 1) {
      const key = storage.key(i) ?? '';
      parts.push(key, storage.getItem(key) ?? '');
    }
  }
  return parts.join('\n');
}

describe('live store, operator view', () => {
  it('takes the clock offset from welcome', () => {
    const store = openStore('server_welcome_operator_demo.json', 'demo');
    expect(store.getState().clockOffsetUs).toBe(1_767_323_100_000_001 - NOW_MS * 1000);
  });

  it('replaces the view on a snapshot and applies set and remove on an update', () => {
    const store = openStore('server_welcome_operator_demo.json', 'demo');
    store.getState().applyMessage(fixture('server_state_operator_snapshot_demo.json', 'demo'), NOW_MS);
    expect(selectOffers(store.getState()).map((c) => c.offer_id)).toEqual(['of-102']);
    expect(selectHeld(store.getState()).map((c) => c.offer_id)).toEqual(['of-101']);
    expect(store.getState().me?.load).toBe(2);

    store.getState().applyMessage(fixture('server_state_operator_update_demo.json', 'demo'), NOW_MS);
    expect(selectOffers(store.getState())).toEqual([]);
    expect(selectHeld(store.getState())[0]?.replied).toBe(true);
    expect(store.getState().me?.load).toBe(1);
    expect(store.getState().seq).toBe(2);
  });

  it('a later snapshot drops rows that it does not list', () => {
    const store = openStore('server_welcome_operator_demo.json', 'demo');
    store.getState().applyMessage(fixture('server_state_operator_snapshot_demo.json', 'demo'), NOW_MS);
    store.getState().applyMessage(
      { type: 'state', seq: 9, snapshot: true, server_time_us: 1_767_323_200_000_000, me: { id: 'web-test0001', kind: 'human', languages: ['en'], capacity: 2, presence: 'offline', load: 0, offered: 0 } },
      NOW_MS,
    );
    expect(store.getState().myConversations.size).toBe(0);
    expect(store.getState().me?.presence).toBe('offline');
  });

  it('keeps the selection only while the conversation is held', () => {
    const store = openStore('server_welcome_operator_demo.json', 'demo');
    store.getState().applyMessage(fixture('server_state_operator_snapshot_demo.json', 'demo'), NOW_MS);
    store.getState().select('ps-k2m3n4p5q6r7');
    store.getState().applyMessage(
      { type: 'state', seq: 2, snapshot: false, server_time_us: 1_767_323_100_250_001, my_conversations: { remove: ['ps-k2m3n4p5q6r7'] } },
      NOW_MS,
    );
    expect(store.getState().selectedId).toBeNull();
  });
});

describe('live store, messages', () => {
  function watchedStore(): LiveStore {
    const store = openStore('server_welcome_operator_demo.json', 'full');
    store.getState().setWatched('did:plc:testalice0000000000000000|did:plc:testbob000000000000000000', true);
    return store;
  }

  it('ignores messages of a conversation that is not watched', () => {
    const store = openStore('server_welcome_operator_demo.json', 'full');
    store.getState().applyMessage(fixture('server_messages_snapshot_full.json', 'full'));
    expect(store.getState().messages.size).toBe(0);
  });

  it('replaces on a snapshot and sorts updates in by source time', () => {
    const store = watchedStore();
    store.getState().applyMessage(fixture('server_messages_snapshot_full.json', 'full'));
    store.getState().applyMessage(fixture('server_messages_update_full.json', 'full'));
    const list = [...store.getState().messages.values()][0] ?? [];
    expect(list.map((m) => m.id)).toEqual(['test-msg-0001', 'test-msg-0002', 'test-msg-0003', 'test-msg-0004']);
    store.getState().applyMessage({
      type: 'messages',
      conversation_id: 'did:plc:testalice0000000000000000|did:plc:testbob000000000000000000',
      snapshot: false,
      messages: [{ id: 'late', direction: 'customer', source_us: 1, length: 4, text: 'late', edited: false }],
    });
    expect(([...store.getState().messages.values()][0] ?? [])[0]?.id).toBe('late');
  });

  it('drops the text of a removed message at once', () => {
    const store = watchedStore();
    store.getState().applyMessage(fixture('server_messages_snapshot_full.json', 'full'));
    store.getState().applyMessage(fixture('server_message_removed_full.json', 'full'));
    const removed = ([...store.getState().messages.values()][0] ?? []).find((m) => m.id === 'test-msg-0001');
    expect(removed?.removedReason).toBe('deleted');
    expect(removed).not.toHaveProperty('text');
    expect(JSON.stringify([...store.getState().messages.values()])).not.toContain('synthetic customer question one');
  });

  it('an empty snapshot clears the list', () => {
    const store = openStore('server_welcome_operator_demo.json', 'demo');
    store.getState().setWatched('ps-k2m3n4p5q6r7', true);
    store.getState().applyMessage(fixture('server_messages_snapshot_demo.json', 'demo'));
    expect(store.getState().messages.get('ps-k2m3n4p5q6r7')).toHaveLength(3);
    store.getState().applyMessage(fixture('server_messages_snapshot_empty.json', 'demo'));
    expect(store.getState().messages.get('ps-k2m3n4p5q6r7')).toEqual([]);
  });

  it('a lost connection drops all messages', () => {
    const store = watchedStore();
    store.getState().applyMessage(fixture('server_messages_snapshot_full.json', 'full'));
    store.getState().setConnection({ phase: 'waiting', attempt: 1 });
    expect(store.getState().messages.size).toBe(0);
  });

  it('never writes text to browser storage', () => {
    const store = watchedStore();
    store.getState().applyMessage(fixture('server_messages_snapshot_full.json', 'full'));
    expect(storageText()).not.toContain('synthetic');
  });
});

describe('live store, supervisor view and status', () => {
  it('builds every collection from a snapshot and applies an update', () => {
    const store = openStore('server_welcome_supervisor_full.json', 'full');
    store.getState().applyMessage(fixture('server_state_supervisor_snapshot_full.json', 'full'));
    const s = store.getState();
    expect([s.conversations.size, s.operators.size, s.queues.size, s.teams.size]).toEqual([2, 3, 2, 1]);
    expect(s.totals?.messages_per_second).toBe(3.5);
    expect(s.welcome?.speed_factor).toBe(50);
  });

  it('stops the timers while stale or disconnected, and runs them again', () => {
    const store = openStore('server_welcome_operator_demo.json', 'demo');
    expect(isLive(store.getState())).toBe(true);
    expect(store.getState().frozenAtUs).toBeNull();

    store.getState().applyMessage(fixture('server_status_stale.json', 'demo'), NOW_MS + 5000);
    const frozen = store.getState().frozenAtUs;
    expect(frozen).toBe((NOW_MS + 5000) * 1000 + store.getState().clockOffsetUs);

    store.getState().setConnection({ phase: 'waiting', attempt: 1 }, NOW_MS + 9000);
    expect(store.getState().frozenAtUs).toBe(frozen);

    store.getState().setConnection({ phase: 'open', attempt: 0 }, NOW_MS + 10_000);
    store.getState().applyMessage(fixture('server_status_fresh.json', 'demo'), NOW_MS + 10_000);
    expect(store.getState().frozenAtUs).toBeNull();
  });
});
