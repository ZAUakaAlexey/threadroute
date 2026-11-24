import { useEffect, type EffectCallback } from 'react';

/**
 * Runs `effect` once after the first render and its cleanup on unmount.
 * The only effect hook of the console: setup that has to touch the outside
 * world (a WebSocket) when a screen appears.
 */
export function useMountEffect(effect: EffectCallback): void {
  // eslint-disable-next-line react-hooks/exhaustive-deps -- runs once by design
  useEffect(effect, []);
}
