// Glue between the WebSocket client and the store: it applies server
// messages, keeps the watch of the shown conversation across snapshots,
// and turns results into notices. No React here.

import { GatewayClient, type ResumeTokenStore, type SocketLike } from '../protocol/client.ts';
import { buildChangeSettings, buildOfferAction, buildPresenceAction, buildSetLanguages, buildUnwatch, buildWatch } from '../protocol/build.ts';
import type { ClientRequest, OfferActionName, Result, ServerMessage, StandSettings, View } from '../protocol/types.ts';
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
  /**
   * Asks for a change of the stand settings and resolves with the result.
   * The answer is shown by the settings panel, not as a notice.
   */
  changeSettings(baseVersion: number, settings: StandSettings): Promise<Result>;
}

/** The local code of a change that was not sent, because this gateway has no stand settings or no right. */
export const SETTINGS_NOT_SENT = 'not_allowed';

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
      // The snapshot ended every watch; watch the shown one again. Its row
      // may come in a later frame of the snapshot, so the gateway decides.
      if (s.selectedId !== null) watch(s.selectedId, false);
    } else {
      // The shown conversation left the view (resolved, returned, closed).
      // The gateway may have ended the watch by itself already.
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

  async function send(request: ClientRequest, busyKey: string | null, isNoticeShown = true): Promise<Result> {
    const s = store.getState();
    if (busyKey !== null) s.setBusy(busyKey, true);
    const result = await client.request(request);
    if (busyKey !== null) store.getState().setBusy(busyKey, false);
    if (result.outcome === 'rejected' && isNoticeShown) store.getState().showNotice('error', resultMessage(result));
    return result;
  }

  /**
   * Watches a conversation. "already_done" (a repeated watch) also brings a
   * new messages snapshot, so it counts as success. When the gateway does
   * not know the conversation, the selection goes.
   */
  function watch(id: string, isNoticeShown = true): void {
    store.getState().setWatched(id, true);
    void send(buildWatch(client.nextRequestId(), id), null, isNoticeShown).then((result) => {
      if (result.outcome !== 'rejected') return;
      store.getState().setWatched(id, false);
      if (result.code === 'unknown_conversation' && store.getState().selectedId === id) {
        session.select(null);
        if (options.view === 'operator') selectFirstHeld();
      }
    });
  }

  function unwatch(id: string): void {
    if (!store.getState().watched.has(id)) return;
    store.getState().setWatched(id, false);
    // The gateway may have ended the watch already ("already_done"); its
    // answer does not matter.
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
    changeSettings(baseVersion, settings) {
      // An older gateway closes the connection on a client message it does
      // not know. The check reads the block of the current connection at
      // the moment of sending: a reconnect to an older gateway clears it.
      const s = store.getState();
      const requestId = client.nextRequestId();
      const mayChange = s.settings?.may_change === true;
      if (!mayChange || s.connection.phase !== 'open') {
        return Promise.resolve({ type: 'result', request_id: requestId, outcome: 'rejected', code: mayChange ? 'not_connected' : SETTINGS_NOT_SENT });
      }
      return client.request(buildChangeSettings(requestId, baseVersion, settings));
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
