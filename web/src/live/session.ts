// Glue between the WebSocket client and the store: it applies server
// messages, keeps the watch of the shown conversation across snapshots,
// and turns results into notices. No React here.

import { GatewayClient, type ResumeTokenStore, type SocketLike } from '../protocol/client.ts';
import { buildOfferAction, buildPresenceAction, buildSetLanguages, buildUnwatch, buildWatch } from '../protocol/build.ts';
import type { ClientRequest, OfferActionName, Result, ServerMessage, View } from '../protocol/types.ts';
import { resultMessage } from './result-messages.ts';
import { selectHeld } from './selectors.ts';
import { createLiveStore, type LiveStore } from './store.ts';

export interface LiveSessionOptions {
  readonly view: View;
  readonly url: string;
  readonly getLanguages: () => readonly string[];
  readonly getToken: () => string | undefined;
  readonly resumeTokens: ResumeTokenStore;
  readonly createSocket?: (url: string) => SocketLike;
  readonly store?: LiveStore;
}

export interface LiveSession {
  readonly store: LiveStore;
  readonly view: View;
  connect(): void;
  dispose(): void;
  setOnline(isOnline: boolean): Promise<void>;
  setLanguages(languages: readonly string[]): Promise<void>;
  offerAction(action: OfferActionName, offerId: string): Promise<void>;
  /** Shows one conversation's messages (or none): unwatch the old, watch the new. */
  select(id: string | null): void;
}

export function createLiveSession(options: LiveSessionOptions): LiveSession {
  const store = options.store ?? createLiveStore();
  const client = new GatewayClient({
    url: options.url,
    view: options.view,
    getLanguages: options.getLanguages,
    getToken: options.getToken,
    resumeTokens: options.resumeTokens,
    onMessage: handleMessage,
    onConnection: (state) => store.getState().setConnection(state),
    ...(options.createSocket ? { createSocket: options.createSocket } : {}),
  });

  function handleMessage(message: ServerMessage): void {
    const before = store.getState().watched;
    store.getState().applyMessage(message);
    if (message.type !== 'state') return;
    const s = store.getState();
    if (message.snapshot) {
      // The snapshot ended every watch; watch the shown one again.
      if (s.selectedId !== null) watch(s.selectedId);
    } else {
      // The shown conversation left the view (resolved, returned, closed).
      for (const id of before) {
        if (s.selectedId !== id) unwatch(id);
      }
    }
    if (options.view === 'operator' && store.getState().selectedId === null) selectFirstHeld();
  }

  /** An operator always sees a conversation they hold, if they hold one. */
  function selectFirstHeld(): void {
    const first = selectHeld(store.getState())[0];
    if (first !== undefined) session.select(first.id);
  }

  async function send(request: ClientRequest, busyKey: string | null): Promise<Result> {
    const s = store.getState();
    if (busyKey !== null) s.setBusy(busyKey, true);
    const result = await client.request(request);
    if (busyKey !== null) store.getState().setBusy(busyKey, false);
    if (result.outcome === 'rejected') store.getState().showNotice('error', resultMessage(result));
    return result;
  }

  function watch(id: string): void {
    store.getState().setWatched(id, true);
    void send(buildWatch(client.nextRequestId(), id), null).then((result) => {
      if (result.outcome === 'rejected') store.getState().setWatched(id, false);
    });
  }

  function unwatch(id: string): void {
    if (!store.getState().watched.has(id)) return;
    store.getState().setWatched(id, false);
    // The gateway may have ended the watch already; its answer does not matter.
    void client.request(buildUnwatch(client.nextRequestId(), id));
  }

  const session: LiveSession = {
    store,
    view: options.view,
    connect: () => client.connect(),
    dispose: () => client.dispose(),
    async setOnline(isOnline) {
      await send(buildPresenceAction(client.nextRequestId(), isOnline ? 'online' : 'offline'), 'presence');
    },
    async setLanguages(languages) {
      await send(buildSetLanguages(client.nextRequestId(), languages), 'languages');
    },
    async offerAction(action, offerId) {
      await send(buildOfferAction(client.nextRequestId(), action, offerId), `${action}:${offerId}`);
    },
    select(id) {
      const s = store.getState();
      if (s.selectedId === id) return;
      const previous = s.selectedId;
      s.select(id);
      if (previous !== null) unwatch(previous);
      if (id !== null && s.connection.phase === 'open') watch(id);
    },
  };
  return session;
}
