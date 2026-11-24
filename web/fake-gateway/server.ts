// The fake gateway: the WebSocket protocol and the public snapshot over a
// synthetic world, for development and tests. Every frame it sends is
// checked with the console's own parser first, and demo frames are checked
// for privacy, so the fake cannot drift from the protocol silently.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { WebSocketServer, type RawData, type WebSocket } from 'ws';

import { closeInfo } from '../src/protocol/close-codes.ts';
import { parseClientMessage, parsePublicSnapshot, parseServerMessage } from '../src/protocol/parse.ts';
import type {
  Changes,
  ClientMessage,
  ConversationRow,
  Level,
  OperatorConversation,
  OperatorRow,
  ServerMessage,
  TeamRow,
  View,
} from '../src/protocol/types.ts';
import { PSEUDONYM_RE, pseudonym } from './synthetic.ts';
import { World, type Conversation, type WorldEvent, type WorldOptions } from './world.ts';

export interface FakeGatewayOptions extends WorldOptions {
  readonly port: number;
  readonly host?: string;
  readonly tickMs?: number;
  /**
   * The most conversation rows in one frame of a supervisor snapshot, like
   * GATEWAY_SNAPSHOT_FRAME_ROWS of the real gateway. A larger snapshot comes
   * in several frames: the first is the snapshot, the rest are updates.
   */
  readonly snapshotFrameRows?: number;
  readonly log?: (line: string) => void;
}

export const FULL_TOKEN = 'synthetic.full.token';
const MAX_DEMO_OPERATORS = 5;
const OFFLINE_GRACE_MS = 15_000;
const HELLO_TIMEOUT_MS = 5_000;
const MAX_WATCHED = 4;
const DEFAULT_SNAPSHOT_FRAME_ROWS = 500;
/** The fixed texts of the real gateway (internal/gateway/client.go). */
const STALE_OWNER_TEXT = 'the operator moved to another connection';
const STALE_DATA_TEXT = 'the gateway data is stale';
const OFFER_ACTIONS: ReadonlySet<string> = new Set(['accept', 'refuse', 'reply', 'resolve']);

type Collections = 'my_conversations' | 'conversations' | 'operators' | 'queues' | 'teams';

interface Connection {
  readonly ws: WebSocket;
  view: View | undefined;
  level: Level;
  operatorId: string | undefined;
  seq: number;
  readonly sent: Map<string, Map<string, string>>;
  meJson: string | undefined;
  totalsJson: string | undefined;
  /** Public ID of a watched conversation, mapped to its world key. */
  readonly watched: Map<string, string>;
  tokens: number;
  lastRefillMs: number;
  helloTimer: ReturnType<typeof setTimeout> | undefined;
}

export interface FakeGateway {
  readonly world: World;
  readonly server: Server;
  readonly url: string;
  close(): Promise<void>;
}

export async function startFakeGateway(options: FakeGatewayOptions): Promise<FakeGateway> {
  const log = options.log ?? (() => undefined);
  const nowUs = () => Date.now() * 1000;
  let world = new World(options, nowUs());
  const connections = new Set<Connection>();
  const resumeTokens = new Map<string, string>();
  const graceTimers = new Map<string, ReturnType<typeof setTimeout>>();
  let unsubscribe = world.on(handleWorldEvent);

  const server = createServer((req, res) => {
    void handleHttp(req, res).catch((error: unknown) => {
      log(`http error: ${String(error)}`);
      res.statusCode = 500;
      res.end();
    });
  });
  const wss = new WebSocketServer({ noServer: true, maxPayload: 4096 });
  server.on('upgrade', (req, socket, head) => {
    if (new URL(req.url ?? '/', 'http://fake').pathname !== '/v1/ws') {
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => handleConnection(ws));
  });

  const ticker = setInterval(() => {
    world.step(nowUs());
    for (const conn of connections) sendState(conn, false);
  }, options.tickMs ?? 250);

  // ------------------------------------------------------------ send

  function send(conn: Connection, message: ServerMessage): void {
    const frame = JSON.stringify(message);
    // The fake's frames go through the console parser: drift fails loudly.
    parseServerMessage(frame, { level: conn.level });
    if (conn.level === 'demo') assertDemoPrivacy(message);
    conn.ws.send(frame);
  }

  function label(conn: Connection, id: string): string {
    return conn.level === 'full' ? id : pseudonym(world.dayKey, id);
  }

  function project(conn: Connection): Partial<Record<Collections, Map<string, unknown>>> & { me?: OperatorRow } {
    const asMap = (rows: readonly { readonly id: string }[]) => new Map<string, unknown>(rows.map((r) => [r.id, r]));
    if (conn.view === 'operator') {
      const op = conn.operatorId === undefined ? undefined : world.operators.get(conn.operatorId);
      const mine = [...world.conversations.values()].filter(
        (c) => c.operatorId === conn.operatorId && (c.state === 'offered' || c.state === 'assigned'),
      );
      return {
        ...(op === undefined ? {} : { me: world.operatorRow(op) }),
        my_conversations: asMap(mine.map((c) => labelConversation(conn, world.operatorConversation(c)))),
      };
    }
    return {
      conversations: asMap([...world.conversations.values()].map((c) => labelConversation(conn, world.conversationRow(c)))),
      operators: asMap([...world.operators.values()].map((o) => labelOperator(conn, world.operatorRow(o)))),
      queues: asMap(world.queueRows()),
      teams: asMap(world.teamRows().map((t) => ({ ...t, id: label(conn, t.id) }))),
    };
  }

  function labelConversation<T extends OperatorConversation | ConversationRow>(conn: Connection, row: T): T {
    return { ...row, id: label(conn, row.id), company: label(conn, row.company), customer: label(conn, row.customer) };
  }

  function labelOperator(conn: Connection, row: OperatorRow): OperatorRow {
    return row.team === undefined ? row : { ...row, team: label(conn, row.team) };
  }

  function sendState(conn: Connection, isSnapshot: boolean): void {
    if (conn.view === undefined || conn.ws.readyState !== conn.ws.OPEN) return;
    const view = project(conn);
    const message: Record<string, unknown> = {};
    let hasChange = isSnapshot;
    for (const name of ['my_conversations', 'conversations', 'operators', 'queues', 'teams'] as const) {
      const rows = view[name];
      if (rows === undefined) continue;
      const changes = diff(conn.sent.get(name) ?? new Map<string, string>(), rows, isSnapshot);
      conn.sent.set(name, new Map([...rows].map(([id, row]) => [id, JSON.stringify(row)])));
      if (changes !== undefined) {
        message[name] = changes;
        hasChange = true;
      }
    }
    if (view.me !== undefined) {
      const json = JSON.stringify(view.me);
      if (isSnapshot || json !== conn.meJson) {
        message.me = view.me;
        hasChange = true;
      }
      conn.meJson = json;
    }
    if (conn.view === 'supervisor') {
      const totals = world.totals(nowUs());
      const json = JSON.stringify(totals);
      if (isSnapshot || json !== conn.totalsJson) {
        message.totals = totals;
        hasChange = true;
      }
      conn.totalsJson = json;
    }
    // A snapshot ends every watch, without a message.
    if (isSnapshot) conn.watched.clear();
    else pruneWatches(conn);
    if (!hasChange) return;
    const parts = isSnapshot ? splitConversations(message, options.snapshotFrameRows ?? DEFAULT_SNAPSHOT_FRAME_ROWS) : [];
    conn.seq += 1;
    send(conn, { type: 'state', seq: conn.seq, snapshot: isSnapshot, server_time_us: nowUs(), ...message });
    // The rest of a large snapshot: updates that only add rows, as the real
    // gateway sends them (docs/gateway-protocol.md, "A snapshot in several frames").
    for (const part of parts) {
      conn.seq += 1;
      send(conn, { type: 'state', seq: conn.seq, snapshot: false, server_time_us: nowUs(), conversations: { set: part } });
    }
  }

  /** May this connection watch the conversation now? Like mayWatch of the real gateway. */
  function mayWatch(conn: Connection, key: string): boolean {
    const conv = world.conversations.get(key);
    if (conv === undefined) return false;
    if (conn.view === 'supervisor') return true;
    return conv.state === 'assigned' && conv.operatorId !== undefined && conv.operatorId === conn.operatorId;
  }

  /**
   * Ends, without a message, every watch the connection may no longer hold:
   * the operator stopped holding the conversation, or the world forgot it.
   */
  function pruneWatches(conn: Connection): void {
    for (const [publicId, key] of conn.watched) {
      if (!mayWatch(conn, key)) conn.watched.delete(publicId);
    }
  }

  // ------------------------------------------------------------ world events

  function handleWorldEvent(event: WorldEvent): void {
    for (const conn of connections) {
      if (conn.view === undefined) continue;
      switch (event.kind) {
        case 'status':
          send(conn, statusMessage());
          break;
        case 'rekey':
          if (conn.level === 'demo') sendState(conn, true);
          break;
        case 'message_added':
        case 'message_removed': {
          pruneWatches(conn);
          const publicId = [...conn.watched].find(([, key]) => key === event.key)?.[0];
          if (publicId === undefined) break;
          if (event.kind === 'message_added') {
            send(conn, { type: 'messages', conversation_id: publicId, snapshot: false, messages: [textMessage(conn, event.message)] });
          } else {
            send(conn, { type: 'message_removed', conversation_id: publicId, message_id: label(conn, event.messageId), reason: event.reason });
          }
        }
      }
    }
  }

  function statusMessage(): ServerMessage {
    const s = world.status;
    if (!s.stale) return { type: 'status', stale: false };
    return { type: 'status', stale: true, reason: s.reason ?? 'source_unavailable', ...(s.lagUs === undefined ? {} : { lag_us: s.lagUs }) };
  }

  function textMessage(conn: Connection, m: Conversation['messages'][number]) {
    const base = { id: label(conn, m.id), direction: m.direction, source_us: m.sourceUs, length: Array.from(m.text).length, edited: m.edited };
    return conn.level === 'full' ? { ...base, text: m.text } : base;
  }

  // ------------------------------------------------------------ connections

  function handleConnection(ws: WebSocket): void {
    const conn: Connection = {
      ws,
      view: undefined,
      level: 'demo',
      operatorId: undefined,
      seq: 0,
      sent: new Map(),
      meJson: undefined,
      totalsJson: undefined,
      watched: new Map(),
      tokens: 10,
      lastRefillMs: Date.now(),
      helloTimer: setTimeout(() => ws.close(4408, 'hello_timeout'), HELLO_TIMEOUT_MS),
    };
    connections.add(conn);
    ws.on('message', (data, isBinary) => handleFrame(conn, data, isBinary));
    ws.on('close', () => handleClose(conn));
    ws.on('error', () => undefined);
  }

  function handleFrame(conn: Connection, data: RawData, isBinary: boolean): void {
    const text = isBinary ? '' : Buffer.isBuffer(data) ? data.toString('utf8') : '';
    let message: ClientMessage;
    try {
      const head = JSON.parse(text) as { type?: unknown; version?: unknown };
      if (head.type === 'hello' && head.version !== 1) {
        conn.ws.close(4426, 'unsupported_version');
        return;
      }
      message = parseClientMessage(text);
    } catch {
      conn.ws.close(4400, 'bad_message');
      return;
    }
    if (conn.view === undefined) {
      if (message.type !== 'hello') {
        conn.ws.close(4400, 'bad_message');
        return;
      }
      handleHello(conn, message);
      return;
    }
    if (message.type === 'hello') {
      conn.ws.close(4400, 'bad_message');
      return;
    }
    if (!takeToken(conn)) {
      send(conn, { type: 'result', request_id: message.request_id, outcome: 'rejected', code: 'rate_limited', message: 'more than 5 requests per second' });
      return;
    }
    if (message.type === 'action') handleAction(conn, message);
    else handleWatch(conn, message.type, message.request_id, message.conversation_id);
  }

  function handleHello(conn: Connection, hello: Extract<ClientMessage, { type: 'hello' }>): void {
    clearTimeout(conn.helloTimer);
    // Like the real gateway of version 1: any token other than the full one
    // (a rejected one too) gives the demo level. 4401 is never sent.
    conn.level = hello.token === FULL_TOKEN ? 'full' : 'demo';
    let resumeToken: string | undefined;
    if (hello.view === 'operator') {
      const resumed = hello.resume_token === undefined ? undefined : resumeTokens.get(hello.resume_token);
      if (resumed !== undefined && world.operators.has(resumed)) {
        for (const other of connections) {
          if (other !== conn && other.operatorId === resumed) {
            other.operatorId = undefined;
            other.ws.close(4409, 'replaced');
          }
        }
        clearTimeout(graceTimers.get(resumed));
        graceTimers.delete(resumed);
        conn.operatorId = resumed;
        resumeToken = hello.resume_token;
      } else {
        if (world.humanCount() >= MAX_DEMO_OPERATORS) {
          conn.ws.close(4503, 'no_operator_seat');
          return;
        }
        const op = world.createHuman(hello.languages ?? ['und']);
        conn.operatorId = op.id;
        resumeToken = `rt_synth_${op.id}_${Math.floor(Math.random() * 1e9).toString(36)}`;
        resumeTokens.set(resumeToken, op.id);
      }
    }
    conn.view = hello.view;
    send(conn, {
      type: 'welcome',
      version: 1,
      level: conn.level,
      view: hello.view,
      ...(conn.operatorId === undefined || resumeToken === undefined ? {} : { operator_id: conn.operatorId, resume_token: resumeToken }),
      server_time_us: nowUs(),
      speed_factor: world.speedFactor,
      accept_timeout_us: world.acceptTimeoutUs,
      assignment_target_us: world.assignmentTargetUs,
      first_response_target_us: world.firstResponseTargetUs,
      tick_ms: options.tickMs ?? 250,
      limits: { max_message_bytes: 4096, actions_per_second: 5, action_burst: 10, max_watched: MAX_WATCHED },
    });
    send(conn, statusMessage());
    sendState(conn, true);
  }

  function handleAction(conn: Connection, action: Extract<ClientMessage, { type: 'action' }>): void {
    const result = (outcome: Parameters<typeof resultMessage>[1]) => send(conn, resultMessage(action.request_id, outcome));
    if (conn.view !== 'operator') {
      result({ outcome: 'rejected', code: 'not_allowed', message: 'actions are allowed on an operator connection only' });
      return;
    }
    if (conn.operatorId === undefined) {
      result({ outcome: 'rejected', code: 'stale_connection', message: `${action.action}: ${STALE_OWNER_TEXT}` });
      return;
    }
    // Only offer actions are checked against the view, so only they fail
    // while the data is stale.
    if (OFFER_ACTIONS.has(action.action) && world.status.stale) {
      result({ outcome: 'rejected', code: 'stale_connection', message: `${action.action}: ${STALE_DATA_TEXT}` });
      return;
    }
    if (action.action === 'set_languages') {
      world.setLanguages(conn.operatorId, action.languages ?? []);
      result({ outcome: 'done' });
      return;
    }
    result(world.action(conn.operatorId, action.action, action.offer_id, action.request_id, nowUs()));
    // Show the new state at once instead of waiting for the next tick.
    for (const c of connections) sendState(c, false);
  }

  function handleWatch(conn: Connection, type: 'watch' | 'unwatch', requestId: string, publicId: string): void {
    pruneWatches(conn);
    if (type === 'unwatch') {
      // A conversation that is not watched (never, or no longer) is not an error.
      const isWatched = conn.watched.delete(publicId);
      send(conn, resultMessage(requestId, { outcome: isWatched ? 'done' : 'already_done' }));
      return;
    }
    const conv = [...world.conversations.values()].find((c) => label(conn, c.key) === publicId);
    if (conv === undefined || !mayWatch(conn, conv.key)) {
      send(conn, rejected(requestId, 'unknown_conversation', 'the conversation is not in this view or may not be watched'));
      return;
    }
    const isRepeated = conn.watched.has(publicId);
    if (!isRepeated && conn.watched.size >= MAX_WATCHED) {
      send(conn, rejected(requestId, 'too_many_watched', '4 conversations are watched already'));
      return;
    }
    conn.watched.set(publicId, conv.key);
    // A repeated watch is "already_done" and still gets a new snapshot.
    send(conn, resultMessage(requestId, { outcome: isRepeated ? 'already_done' : 'done' }));
    send(conn, { type: 'messages', conversation_id: publicId, snapshot: true, messages: conv.messages.map((m) => textMessage(conn, m)) });
  }

  function handleClose(conn: Connection): void {
    clearTimeout(conn.helloTimer);
    connections.delete(conn);
    const id = conn.operatorId;
    if (id === undefined) return;
    graceTimers.set(
      id,
      setTimeout(() => {
        graceTimers.delete(id);
        world.removeHuman(id, nowUs());
      }, OFFLINE_GRACE_MS),
    );
  }

  function takeToken(conn: Connection): boolean {
    const now = Date.now();
    conn.tokens = Math.min(10, conn.tokens + ((now - conn.lastRefillMs) / 1000) * 5);
    conn.lastRefillMs = now;
    if (conn.tokens < 1) return false;
    conn.tokens -= 1;
    return true;
  }

  // ------------------------------------------------------------ http

  async function handleHttp(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://fake');
    if (req.method === 'GET' && url.pathname === '/v1/public/snapshot') {
      const snapshot = world.publicSnapshot(nowUs());
      parsePublicSnapshot(JSON.parse(JSON.stringify(snapshot)) as unknown);
      json(res, 200, snapshot, { 'cache-control': 'public, max-age=1' });
      return;
    }
    if (url.pathname.startsWith('/__control/') && req.method === 'POST') {
      const body = await readBody(req);
      json(res, 200, control(url.pathname.slice('/__control/'.length), body));
      return;
    }
    if (url.pathname === '/healthz') {
      json(res, 200, { ok: true });
      return;
    }
    json(res, 404, { error: 'not found' });
  }

  /** Test controls. Each returns what a test needs to know, no text. */
  function control(name: string, body: Record<string, unknown>): Record<string, unknown> {
    const now = nowUs();
    const human = () => [...world.operators.values()].find((o) => o.kind === 'human' && o.presence === 'online');
    switch (name) {
      case 'reset': {
        for (const conn of connections) {
          // The old world's operators go with it; no grace timer for them.
          conn.operatorId = undefined;
          conn.ws.close(1001, 'going_away');
        }
        for (const timer of graceTimers.values()) clearTimeout(timer);
        graceTimers.clear();
        resumeTokens.clear();
        unsubscribe();
        world = new World({ ...options, seed: typeof body.seed === 'number' ? body.seed : options.seed }, now);
        unsubscribe = world.on(handleWorldEvent);
        return { ok: true };
      }
      case 'offer': {
        const op = human();
        const conv = op === undefined ? undefined : world.offerToHuman(op, now);
        for (const conn of connections) sendState(conn, false);
        return { ok: conv !== undefined, offer_id: conv?.offerId ?? null };
      }
      case 'customer-message': {
        const conv = world.heldByHumans()[0];
        const message = conv === undefined ? undefined : world.addCustomerMessage(conv, now);
        return { ok: message !== undefined };
      }
      case 'remove-message': {
        const conv = world.heldByHumans()[0];
        const message = conv?.messages.at(-1);
        const isRemoved = conv !== undefined && message !== undefined && world.removeMessage(conv, message.id, 'deleted');
        return { ok: isRemoved };
      }
      case 'stale':
        world.setStale(body.stale === true ? { stale: true, reason: 'source_unavailable', lagUs: 4_000_000 } : { stale: false });
        return { ok: true };
      case 'operator-offline': {
        const id = typeof body.id === 'string' ? body.id : 'sim-op-0005';
        world.setPresence(id, 'offline', now);
        return { ok: true };
      }
      case 'rekey':
        world.rekey();
        return { ok: true };
      case 'close': {
        const code = typeof body.code === 'number' ? body.code : 1001;
        // The close reason is always the name of the code.
        for (const conn of connections) conn.ws.close(code, closeInfo(code).name);
        return { ok: true };
      }
      case 'snapshot':
        // The gateway rebuilt its view: every connection gets a new snapshot.
        for (const conn of connections) sendState(conn, true);
        return { ok: true };
      default:
        return { ok: false, error: 'unknown control' };
    }
  }

  await new Promise<void>((resolve) => server.listen(options.port, options.host ?? '127.0.0.1', resolve));
  const address = server.address();
  const port = typeof address === 'object' && address !== null ? address.port : options.port;
  log(`fake gateway on http://${options.host ?? '127.0.0.1'}:${port} (seed ${options.seed}, speed ${options.speedFactor}, ${options.isManual ? 'manual' : 'scenario'})`);

  return {
    get world() {
      return world;
    },
    server,
    url: `http://${options.host ?? '127.0.0.1'}:${port}`,
    close: async () => {
      clearInterval(ticker);
      for (const timer of graceTimers.values()) clearTimeout(timer);
      for (const conn of connections) conn.ws.terminate();
      wss.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

/**
 * Takes the conversation rows over the first frameRows out of a snapshot
 * message and returns them in parts of frameRows. Like builder.snapshot of
 * the real gateway, only the conversations of the supervisor view are split.
 */
function splitConversations(message: Record<string, unknown>, frameRows: number): ConversationRow[][] {
  const changes = message.conversations as Changes<ConversationRow> | undefined;
  const rows = changes?.set ?? [];
  if (rows.length <= frameRows) return [];
  message.conversations = { set: rows.slice(0, frameRows) };
  const parts: ConversationRow[][] = [];
  for (let i = frameRows; i < rows.length; i += frameRows) parts.push(rows.slice(i, i + frameRows));
  return parts;
}

function diff(previous: Map<string, string>, current: Map<string, unknown>, isSnapshot: boolean): Changes<{ id: string }> | undefined {
  if (isSnapshot) return { set: [...current.values()] as { id: string }[] };
  const set: { id: string }[] = [];
  for (const [id, row] of current) {
    if (previous.get(id) !== JSON.stringify(row)) set.push(row as { id: string });
  }
  const remove = [...previous.keys()].filter((id) => !current.has(id));
  if (set.length === 0 && remove.length === 0) return undefined;
  return { ...(set.length > 0 ? { set } : {}), ...(remove.length > 0 ? { remove } : {}) };
}

function resultMessage(requestId: string, outcome: ReturnType<World['action']>): ServerMessage {
  return outcome.outcome === 'rejected'
    ? { type: 'result', request_id: requestId, outcome: 'rejected', code: outcome.code, message: outcome.message }
    : { type: 'result', request_id: requestId, outcome: outcome.outcome };
}

function rejected(requestId: string, code: string, message: string): ServerMessage {
  return { type: 'result', request_id: requestId, outcome: 'rejected', code, message };
}

/** At the demo level every key is a pseudonym and no message has text. */
export function assertDemoPrivacy(message: ServerMessage): void {
  const ids: string[] = [];
  if (message.type === 'state') {
    const rows = [...(message.my_conversations?.set ?? []), ...(message.conversations?.set ?? [])];
    for (const r of rows) ids.push(r.id, r.company, r.customer);
    ids.push(...(message.my_conversations?.remove ?? []), ...(message.conversations?.remove ?? []));
    ids.push(...(message.teams?.set ?? []).map((t: TeamRow) => t.id), ...(message.teams?.remove ?? []));
    ids.push(...(message.operators?.set ?? []).flatMap((o: OperatorRow) => (o.team === undefined ? [] : [o.team])));
  }
  if (message.type === 'messages') {
    ids.push(message.conversation_id, ...message.messages.map((m) => m.id));
    if (message.messages.some((m) => m.text !== undefined)) throw new Error('demo frame with text');
  }
  if (message.type === 'message_removed') ids.push(message.conversation_id, message.message_id);
  const bad = ids.find((id) => !PSEUDONYM_RE.test(id));
  if (bad !== undefined) throw new Error('demo frame with an ID that is not a pseudonym');
}

function json(res: ServerResponse, status: number, body: unknown, headers: Record<string, string> = {}): void {
  res.writeHead(status, { 'content-type': 'application/json', ...headers });
  res.end(JSON.stringify(body));
}

async function readBody(req: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  if (chunks.length === 0) return {};
  try {
    const value = JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown;
    return typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}
