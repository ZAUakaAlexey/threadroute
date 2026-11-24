// WebSocket client of the gateway protocol: hello handshake, version check,
// close codes, reconnect with backoff and the resume token, and request IDs
// with exactly one result per request. It knows nothing about React.

import { backoffDelayMs, closeInfo, type CloseInfo } from './close-codes.ts';
import { buildHello, encodeClientMessage } from './build.ts';
import { ProtocolError, parseServerMessage } from './parse.ts';
import { PROTOCOL_VERSION, type ClientRequest, type Level, type Result, type ServerMessage, type View, type Welcome } from './types.ts';

/** The part of the browser WebSocket that the client uses. */
export interface SocketLike {
  readonly readyState: number;
  onopen: ((event: Event) => void) | null;
  onmessage: ((event: MessageEvent) => void) | null;
  onclose: ((event: CloseEvent) => void) | null;
  onerror: ((event: Event) => void) | null;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

export type ConnectionPhase =
  /** Not started, or disposed. */
  | 'idle'
  /** The socket is opening, or the hello was sent and no welcome came yet. */
  | 'connecting'
  /** The welcome arrived. */
  | 'open'
  /** Lost; waiting for the next try. */
  | 'waiting'
  /** Closed for good; the user decides what happens next. */
  | 'stopped';

export interface ConnectionState {
  readonly phase: ConnectionPhase;
  /** Tries since the last welcome. */
  readonly attempt: number;
  /** Wall clock time of the next try, while phase is "waiting". */
  readonly retryAtMs?: number;
  readonly closeCode?: number;
  readonly close?: CloseInfo;
}

export interface ResumeTokenStore {
  load(): string | undefined;
  save(token: string): void;
  clear(): void;
}

/** A result made by the client when the request never reached the gateway. */
export const LOCAL_CODES = { notConnected: 'not_connected', connectionLost: 'connection_lost' } as const;

export interface GatewayClientOptions {
  readonly url: string;
  readonly view: View;
  readonly getLanguages: () => readonly string[];
  readonly getToken: () => string | undefined;
  readonly resumeTokens: ResumeTokenStore;
  readonly onMessage: (message: ServerMessage) => void;
  readonly onConnection: (state: ConnectionState) => void;
  readonly createSocket?: (url: string) => SocketLike;
  readonly random?: () => number;
  readonly welcomeTimeoutMs?: number;
  /** Diagnostics. Never gets post text or a token. */
  readonly log?: (event: string, detail?: Readonly<Record<string, string | number>>) => void;
}

interface Pending {
  readonly request: ClientRequest;
  readonly resolve: (result: Result) => void;
  readonly isRetriedOnReconnect: boolean;
}

const OPEN = 1;
const CLIENT_PROTOCOL_ERROR = 4000;
const WELCOME_TIMEOUT_MS = 10_000;
/** A connection that stayed open this long resets the backoff. */
const STABLE_AFTER_MS = 10_000;

export class GatewayClient {
  private readonly options: GatewayClientOptions;
  private socket: SocketLike | null = null;
  private welcome: Welcome | null = null;
  private state: ConnectionState = { phase: 'idle', attempt: 0 };
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private welcomeTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly pending = new Map<string, Pending>();
  private readonly requestPrefix: string;
  private requestCounter = 0;
  private isTokenDropped = false;
  /** Failed tries in a row; a connection that stays open resets it. */
  private failStreak = 0;
  private welcomedAtMs: number | null = null;

  constructor(options: GatewayClientOptions) {
    this.options = options;
    const random = options.random ?? Math.random;
    this.requestPrefix = `c${Math.floor(random() * 36 ** 6).toString(36)}`;
  }

  get level(): Level | undefined {
    return this.welcome?.level;
  }

  get connection(): ConnectionState {
    return this.state;
  }

  /** Opens the connection. Calling it while connected does nothing. */
  connect(): void {
    if (this.state.phase === 'connecting' || this.state.phase === 'open') return;
    this.clearRetry();
    this.open(this.failStreak);
  }

  /** Closes the connection on purpose and stops reconnecting. */
  dispose(): void {
    this.clearRetry();
    this.clearWelcomeTimer();
    const socket = this.socket;
    this.socket = null;
    this.welcome = null;
    if (socket !== null) {
      detach(socket);
      socket.close(1000, 'normal');
    }
    this.failPending(LOCAL_CODES.connectionLost, true);
    this.setState({ phase: 'idle', attempt: 0 });
  }

  /** A new request ID, unique within this client. */
  nextRequestId(): string {
    this.requestCounter += 1;
    return `${this.requestPrefix}-${this.requestCounter}`;
  }

  /**
   * Sends a request and resolves with its one result. A "reply" action is
   * kept over a reconnect and sent again with the same request ID, as the
   * protocol asks; every other request resolves with a local
   * "connection_lost" result when the connection drops.
   */
  request(request: ClientRequest): Promise<Result> {
    const socket = this.socket;
    if (socket === null || this.welcome === null || socket.readyState !== OPEN) {
      return Promise.resolve(localResult(request.request_id, LOCAL_CODES.notConnected));
    }
    const isRetriedOnReconnect = request.type === 'action' && request.action === 'reply';
    return new Promise((resolve) => {
      this.pending.set(request.request_id, { request, resolve, isRetriedOnReconnect });
      socket.send(encodeClientMessage(request));
    });
  }

  private open(attempt: number): void {
    const create = this.options.createSocket ?? ((url: string) => new WebSocket(url));
    this.setState({ phase: 'connecting', attempt });
    let socket: SocketLike;
    try {
      socket = create(this.options.url);
    } catch {
      this.scheduleRetry(closeInfo(1006), 1006);
      return;
    }
    this.socket = socket;
    socket.onopen = () => this.handleOpen(socket);
    socket.onmessage = (event) => this.handleFrame(socket, event);
    socket.onclose = (event) => this.handleClose(socket, event.code);
    socket.onerror = () => undefined;
  }

  private handleOpen(socket: SocketLike): void {
    if (socket !== this.socket) return;
    const hello = buildHello({
      view: this.options.view,
      languages: this.options.getLanguages(),
      token: this.isTokenDropped ? undefined : this.options.getToken(),
      resumeToken: this.options.resumeTokens.load(),
    });
    socket.send(encodeClientMessage(hello));
    this.welcomeTimer = setTimeout(() => {
      this.options.log?.('welcome_timeout');
      socket.close(CLIENT_PROTOCOL_ERROR, 'welcome_timeout');
    }, this.options.welcomeTimeoutMs ?? WELCOME_TIMEOUT_MS);
  }

  private handleFrame(socket: SocketLike, event: MessageEvent): void {
    if (socket !== this.socket || typeof event.data !== 'string') return;
    try {
      const parsed = parseServerMessage(event.data, this.welcome === null ? {} : { level: this.welcome.level });
      if (parsed.kind === 'unknown') {
        this.options.log?.('unknown_type_skipped', { type: parsed.type.slice(0, 64) });
        return;
      }
      this.handleMessage(socket, parsed.message);
    } catch (error) {
      if (!(error instanceof ProtocolError)) throw error;
      // The path names the field; the value is never logged.
      this.options.log?.('protocol_error', { path: error.path });
      socket.close(CLIENT_PROTOCOL_ERROR, 'client_protocol_error');
    }
  }

  private handleMessage(socket: SocketLike, message: ServerMessage): void {
    if (this.welcome === null) {
      if (message.type !== 'welcome') throw new ProtocolError('type', 'the first message is not a welcome');
      this.handleWelcome(socket, message);
      return;
    }
    if (message.type === 'welcome') throw new ProtocolError('type', 'a second welcome');
    if (message.type === 'result') {
      const pending = this.pending.get(message.request_id);
      this.pending.delete(message.request_id);
      pending?.resolve(message);
    }
    this.options.onMessage(message);
  }

  private handleWelcome(socket: SocketLike, welcome: Welcome): void {
    if (welcome.version !== PROTOCOL_VERSION) {
      detach(socket);
      this.socket = null;
      this.clearWelcomeTimer();
      socket.close(1000, 'normal');
      this.stop(closeInfo(4426), 4426);
      return;
    }
    if (welcome.view !== this.options.view) throw new ProtocolError('view', 'is not the view of the hello');
    this.clearWelcomeTimer();
    this.welcome = welcome;
    this.welcomedAtMs = Date.now();
    if (welcome.resume_token !== undefined) this.options.resumeTokens.save(welcome.resume_token);
    this.setState({ phase: 'open', attempt: this.failStreak });
    this.options.onMessage(welcome);
    for (const pending of this.pending.values()) socket.send(encodeClientMessage(pending.request));
  }

  private handleClose(socket: SocketLike, code: number): void {
    if (socket !== this.socket) return;
    detach(socket);
    this.socket = null;
    this.welcome = null;
    this.clearWelcomeTimer();
    const info = closeInfo(code);
    this.options.log?.('closed', { code, name: info.name });
    this.failPending(LOCAL_CODES.connectionLost, false);
    // Only a connection that stayed open resets the backoff, so a failure
    // right after welcome (for example a frame the parser refuses) still
    // backs off.
    if (this.welcomedAtMs !== null && Date.now() - this.welcomedAtMs >= STABLE_AFTER_MS) this.failStreak = 0;
    this.welcomedAtMs = null;
    switch (info.reaction) {
      case 'reconnect':
        this.scheduleRetry(info, code);
        return;
      case 'reconnect-without-token':
        this.isTokenDropped = true;
        this.scheduleRetry(info, code);
        return;
      case 'stop':
      case 'reload':
        this.stop(info, code);
    }
  }

  private scheduleRetry(info: CloseInfo, code: number): void {
    const attempt = this.failStreak;
    this.failStreak += 1;
    const delay = backoffDelayMs({ attempt, minDelayMs: info.minDelayMs, random: this.options.random ?? Math.random });
    this.setState({ phase: 'waiting', attempt: this.failStreak, retryAtMs: Date.now() + delay, closeCode: code, close: info });
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.open(this.failStreak);
    }, delay);
  }

  private stop(info: CloseInfo, code: number): void {
    this.clearRetry();
    this.failPending(LOCAL_CODES.connectionLost, true);
    this.failStreak = 0;
    this.setState({ phase: 'stopped', attempt: 0, closeCode: code, close: info });
  }

  /** Resolves open requests locally. Replies stay unless isAll is set. */
  private failPending(code: string, isAll: boolean): void {
    for (const [id, pending] of this.pending) {
      if (!isAll && pending.isRetriedOnReconnect) continue;
      this.pending.delete(id);
      pending.resolve(localResult(id, code));
    }
  }

  private setState(state: ConnectionState): void {
    this.state = state;
    this.options.onConnection(state);
  }

  private clearRetry(): void {
    if (this.retryTimer !== null) clearTimeout(this.retryTimer);
    this.retryTimer = null;
  }

  private clearWelcomeTimer(): void {
    if (this.welcomeTimer !== null) clearTimeout(this.welcomeTimer);
    this.welcomeTimer = null;
  }
}

function detach(socket: SocketLike): void {
  socket.onopen = null;
  socket.onmessage = null;
  socket.onclose = null;
  socket.onerror = null;
}

function localResult(requestId: string, code: string): Result {
  return { type: 'result', request_id: requestId, outcome: 'rejected', code };
}
