// Renders a screen against an in-memory gateway connection.

import { act, render } from '@testing-library/react';
import type { ReactNode } from 'react';

import { LiveSessionProvider } from '../live/provider.tsx';
import { createLiveSession, type LiveSession } from '../live/session.ts';
import type { View } from '../protocol/types.ts';
import { socketFactory, type FakeSocket } from './fake-socket.ts';
import { readFixture } from './fixtures.ts';

export interface Harness {
  readonly session: LiveSession;
  readonly socket: () => FakeSocket;
  /** Sends a server message and lets React render. */
  readonly push: (message: unknown) => void;
}

export function renderLive(view: View, ui: ReactNode, welcome: unknown = defaultWelcome(view)): Harness {
  const factory = socketFactory();
  const session = createLiveSession({
    view,
    url: 'ws://gateway.test/v1/ws',
    getLanguages: () => ['en', 'und'],
    getToken: () => undefined,
    resumeTokens: { load: () => undefined, save: () => undefined, clear: () => undefined },
    createSocket: factory.create,
  });
  render(
    <LiveSessionProvider view={view} session={session}>
      {ui}
    </LiveSessionProvider>,
  );
  const socket = () => {
    const s = factory.sockets.at(-1);
    if (s === undefined) throw new Error('no socket yet');
    return s;
  };
  const push = (message: unknown) => {
    act(() => {
      socket().serverSend(message);
    });
  };
  act(() => {
    socket().serverOpen();
  });
  push(welcome);
  push({ type: 'status', stale: false });
  return { session, socket, push };
}

function defaultWelcome(view: View): unknown {
  const welcome = readFixture(view === 'operator' ? 'server_welcome_operator_demo.json' : 'server_welcome_supervisor_full.json');
  // Times relative to now, so the timers show sensible values.
  return { ...(welcome as object), server_time_us: Date.now() * 1000 };
}

export function nowUs(offsetSeconds = 0): number {
  return Date.now() * 1000 + offsetSeconds * 1_000_000;
}
