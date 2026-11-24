// Sentences for rejected requests. The server's `message` is for people too,
// but it is technical, so the known codes get a plain sentence.

import type { Result } from '../protocol/types.ts';

const MESSAGES: Readonly<Record<string, string>> = {
  invalid_request: 'The request was not valid.',
  operator_not_found: 'The router does not know this operator yet. Try again in a moment.',
  offer_not_found: 'This offer is no longer known.',
  offer_expired: 'The offer expired before it was accepted.',
  offer_refused: 'You refused this offer before.',
  offer_accepted: 'The offer was already accepted.',
  offer_ended: 'The offer ended: the conversation closed or you went offline.',
  offer_not_accepted: 'Accept the offer first.',
  assignment_ended: 'This conversation is no longer yours.',
  unavailable: 'The router is busy. Try again in a moment.',
  internal: 'The gateway had an internal error.',
  not_allowed: 'This is not allowed on this screen.',
  rate_limited: 'Too many requests. Wait a second and try again.',
  unknown_offer: 'This offer is no longer live.',
  stale_connection: 'The data is stale, so the gateway could not check the request.',
  unknown_conversation: 'This conversation is no longer in your view.',
  too_many_watched: 'Too many conversations are open at the same time.',
  not_connected: 'Not connected to the gateway.',
  connection_lost: 'The connection was lost before the gateway answered.',
};

export function resultMessage(result: Result): string {
  const code = result.code ?? '';
  return MESSAGES[code] ?? `The request was rejected (${code}).`;
}
