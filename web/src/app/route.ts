// Hash routes, so the built files work under any path of another site
// without rewrite rules. Only the screen is in the URL: never a
// conversation, because at the full level its ID is an author ID.

import { useSyncExternalStore } from 'react';

export const ROUTES = { public: '#/', operator: '#/operator', supervisor: '#/supervisor' } as const;
export type Route = keyof typeof ROUTES;

export function routeOf(hash: string): Route {
  if (hash.startsWith('#/operator')) return 'operator';
  if (hash.startsWith('#/supervisor')) return 'supervisor';
  return 'public';
}

function subscribe(listener: () => void): () => void {
  window.addEventListener('hashchange', listener);
  return () => window.removeEventListener('hashchange', listener);
}

export function useRoute(): Route {
  return useSyncExternalStore(subscribe, () => routeOf(window.location.hash), () => 'public');
}
