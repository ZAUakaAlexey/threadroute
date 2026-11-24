// Sentences for a stale status (docs/gateway-protocol.md, "status").

import { formatShortDuration } from '../lib/format.ts';
import type { Status } from '../protocol/types.ts';

const STALE_REASONS = {
  catching_up: 'The gateway is still reading the event history.',
  source_unavailable: 'The gateway cannot read its data source.',
  lagging: 'The gateway is behind its data source.',
} as const;

/**
 * The reason and, when the gateway knows it, the lag. `lag_us` may come with
 * any reason; with `source_unavailable` it is the time since the gateway
 * last saw the end of its data.
 */
export function staleDetail(status: Pick<Status, 'reason' | 'lag_us'>): string {
  const reason = status.reason === undefined ? '' : STALE_REASONS[status.reason];
  if (status.lag_us === undefined) return reason;
  const lag = formatShortDuration(status.lag_us);
  const sentence =
    status.reason === 'source_unavailable' ? `It last reached the end of its data ${lag} ago.` : `It is ${lag} behind.`;
  return reason === '' ? sentence : `${reason} ${sentence}`;
}
