// Close codes of the gateway (docs/gateway-protocol.md, "Close codes"). The
// reconnect rule follows CloseCode.Reconnect in internal/gwapi/codes.go: a
// code reconnects by itself only when the "Reconnect" column says yes. The
// client never reads the close reason; the code says everything.

export type CloseReaction =
  /** Reconnect by itself with backoff. */
  | 'reconnect'
  /** Stop. The user may reconnect by hand. */
  | 'stop'
  /** Stop. A newer console was deployed; the user must reload the page. */
  | 'reload';

export interface CloseInfo {
  readonly name: string;
  readonly reaction: CloseReaction;
  /** Forget the site token: the next connection goes at the demo level. */
  readonly dropsToken?: boolean;
  /** Smallest wait before the next try, in milliseconds. */
  readonly minDelayMs: number;
  /** A short sentence for the user. */
  readonly message: string;
}

const CLOSE_CODES: Readonly<Record<number, CloseInfo>> = {
  1000: { name: 'normal', reaction: 'stop', minDelayMs: 0, message: 'The connection was closed.' },
  1001: { name: 'going_away', reaction: 'reconnect', minDelayMs: 0, message: 'The gateway is restarting.' },
  1009: {
    name: 'message_too_big',
    reaction: 'stop',
    minDelayMs: 0,
    message: 'The console sent a message that was too large.',
  },
  1011: { name: 'internal', reaction: 'reconnect', minDelayMs: 0, message: 'The gateway had an internal error.' },
  1013: { name: 'try_again_later', reaction: 'reconnect', minDelayMs: 0, message: 'The gateway is busy.' },
  4400: {
    name: 'bad_message',
    reaction: 'stop',
    minDelayMs: 0,
    message: 'The gateway did not accept a message of this console.',
  },
  4401: {
    name: 'unauthorized',
    reaction: 'stop',
    dropsToken: true,
    minDelayMs: 0,
    message: 'Your sign-in was not accepted. Connect again to use the demo level.',
  },
  4408: { name: 'hello_timeout', reaction: 'stop', minDelayMs: 0, message: 'The connection timed out at the start.' },
  4409: {
    name: 'replaced',
    reaction: 'stop',
    minDelayMs: 0,
    message: 'This operator session moved to another tab or window.',
  },
  4410: { name: 'slow_client', reaction: 'reconnect', minDelayMs: 0, message: 'The connection was too slow.' },
  4426: {
    name: 'unsupported_version',
    reaction: 'reload',
    minDelayMs: 0,
    message: 'A newer version of the console is available. Reload the page.',
  },
  4429: {
    name: 'too_many_connections',
    reaction: 'reconnect',
    minDelayMs: 10_000,
    message: 'Too many connections from your address.',
  },
  4503: {
    name: 'no_operator_seat',
    reaction: 'reconnect',
    minDelayMs: 10_000,
    message: 'All demo operator seats are taken. You can watch the supervisor view meanwhile.',
  },
  4504: {
    name: 'auth_unavailable',
    reaction: 'reconnect',
    minDelayMs: 5_000,
    message: 'Your sign-in could not be checked right now.',
  },
};

const LOST: CloseInfo = {
  name: 'connection_lost',
  reaction: 'reconnect',
  minDelayMs: 0,
  message: 'The connection to the gateway was lost.',
};

/**
 * Information about a close code. 1005 and 1006 come from the browser only
 * (no code, or no close frame); they and unknown codes mean a lost
 * connection, handled like 1001.
 */
export function closeInfo(code: number): CloseInfo {
  return CLOSE_CODES[code] ?? LOST;
}

export interface BackoffInput {
  readonly attempt: number;
  readonly minDelayMs?: number;
  readonly random?: () => number;
}

/** 1, 2, 4, 8 ... seconds up to 30, with up to 30 percent jitter. */
export function backoffDelayMs({ attempt, minDelayMs = 0, random = Math.random }: BackoffInput): number {
  const base = Math.min(30_000, 1000 * 2 ** Math.max(0, attempt));
  const jittered = base * (0.7 + 0.3 * random());
  return Math.round(Math.max(minDelayMs, jittered));
}
