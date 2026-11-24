// One shared clock tick for the whole page. Every timer on a screen reads
// the same value, so there is one interval however many rows show a timer.
// The interval runs only while a component listens.

import { useSyncExternalStore } from 'react';

export const TICK_MS = 250;

const listeners = new Set<() => void>();
let nowMs = Date.now();
let interval: ReturnType<typeof setInterval> | null = null;

function tick(): void {
  nowMs = Date.now();
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  if (interval === null) {
    nowMs = Date.now();
    interval = setInterval(tick, TICK_MS);
  }
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0 && interval !== null) {
      clearInterval(interval);
      interval = null;
    }
  };
}

function getSnapshot(): number {
  return nowMs;
}

/** Browser wall clock in milliseconds, updated every TICK_MS. */
export function useTickMs(): number {
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
