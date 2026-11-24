// Hooks of a live session. The session itself comes from
// LiveSessionProvider (provider.tsx).

import { createContext, use } from 'react';
import { useStore } from 'zustand';

import { useTickMs } from './clock.ts';
import { selectSpeedFactor } from './selectors.ts';
import type { LiveSession } from './session.ts';
import type { LiveState } from './store.ts';

export const LiveSessionContext = createContext<LiveSession | null>(null);

export function useSession(): LiveSession {
  const session = use(LiveSessionContext);
  if (session === null) throw new Error('useSession needs a LiveSessionProvider');
  return session;
}

export function useLive<T>(selector: (s: LiveState) => T): T {
  return useStore(useSession().store, selector);
}

/**
 * Server time now, in microseconds, from the shared clock tick and the
 * clock offset. It stands still while the timers are stopped.
 */
export function useServerNowUs(): number {
  const tickMs = useTickMs();
  const offset = useLive((s) => s.clockOffsetUs);
  const frozen = useLive((s) => s.frozenAtUs);
  return frozen ?? tickMs * 1000 + offset;
}

/** Model time left until a deadline; negative once it passed. */
export function useRemainingUs(deadlineUs: number): number {
  const now = useServerNowUs();
  const speed = useLive(selectSpeedFactor);
  return (deadlineUs - now) * speed;
}

/** Model time since a start time. */
export function useElapsedUs(sinceUs: number): number {
  const now = useServerNowUs();
  const speed = useLive(selectSpeedFactor);
  return Math.max(0, (now - sinceUs) * speed);
}
