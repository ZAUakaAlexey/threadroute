// The public snapshot: plain HTTP, asked for every two seconds (decision
// 0013, point 1B). TanStack Query holds it; the live store is not used.

import { queryOptions } from '@tanstack/react-query';

import { parsePublicSnapshot } from '../../protocol/parse.ts';
import type { PublicSnapshot } from '../../protocol/types.ts';

export const POLL_MS = 2000;

export const publicSnapshotKeys = { all: ['public-snapshot'] as const };

export function publicSnapshotQuery(url: string) {
  return queryOptions({
    queryKey: [...publicSnapshotKeys.all, url],
    queryFn: ({ signal }) => fetchPublicSnapshot(url, signal),
    refetchInterval: POLL_MS,
    refetchIntervalInBackground: false,
    staleTime: 1000,
    retry: 1,
  });
}

export async function fetchPublicSnapshot(url: string, signal?: AbortSignal): Promise<PublicSnapshot> {
  const response = await fetch(url, { signal: signal ?? null, headers: { accept: 'application/json' } });
  if (!response.ok) throw new Error(`public snapshot: HTTP ${response.status}`);
  return parsePublicSnapshot(await response.json());
}
