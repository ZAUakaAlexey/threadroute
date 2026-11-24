// Builders for client messages. They check the same rules as the gateway,
// so a bug in the console fails here and not with close code 4400.

import {
  PROTOCOL_VERSION,
  SIM_FIELDS,
  TEAM_FIELDS,
  type Action,
  type ChangeSettings,
  type ClientMessage,
  type Hello,
  type OfferActionName,
  type SimSettings,
  type StandSettings,
  type TeamSettings,
  type Unwatch,
  type View,
  type Watch,
} from './types.ts';
import { ProtocolError, isValidLang, isValidRoutingId } from './read.ts';

export const MAX_CLIENT_MESSAGE_BYTES = 4096;

export interface HelloInput {
  readonly view: View;
  readonly languages?: readonly string[];
  readonly token?: string | undefined;
  readonly resumeToken?: string | undefined;
}

export function buildHello({ view, languages, token, resumeToken }: HelloInput): Hello {
  const isOperator = view === 'operator';
  if (isOperator) checkLanguages(languages ?? []);
  return {
    type: 'hello',
    version: PROTOCOL_VERSION,
    view,
    ...(token ? { token } : {}),
    ...(isOperator && resumeToken ? { resume_token: resumeToken } : {}),
    ...(isOperator && languages ? { languages: [...languages] } : {}),
  };
}

export function buildWatch(requestId: string, conversationId: string): Watch {
  checkRequestId(requestId);
  return { type: 'watch', request_id: requestId, conversation_id: conversationId };
}

export function buildUnwatch(requestId: string, conversationId: string): Unwatch {
  checkRequestId(requestId);
  return { type: 'unwatch', request_id: requestId, conversation_id: conversationId };
}

export function buildPresenceAction(requestId: string, action: 'online' | 'offline'): Action {
  checkRequestId(requestId);
  return { type: 'action', request_id: requestId, action };
}

export function buildSetLanguages(requestId: string, languages: readonly string[]): Action {
  checkRequestId(requestId);
  checkLanguages(languages);
  return { type: 'action', request_id: requestId, action: 'set_languages', languages: [...languages] };
}

export function buildOfferAction(requestId: string, action: OfferActionName, offerId: string): Action {
  checkRequestId(requestId);
  if (!isValidRoutingId(offerId)) throw new ProtocolError('offer_id', 'is not a routing ID');
  return { type: 'action', request_id: requestId, action, offer_id: offerId };
}

/** Fields that the gateway reads as integers: a fraction there closes the connection. */
const SHARE_FIELDS: ReadonlySet<string> = new Set(['max_share', 'missed_offer_share']);

/**
 * Asks for a change of the stand settings. Only the form is checked here;
 * the bounds are checked by the console form and, with authority, by the
 * gateway. The set is copied field by field, so nothing else goes out.
 */
export function buildChangeSettings(requestId: string, baseVersion: number, settings: StandSettings): ChangeSettings {
  checkRequestId(requestId);
  if (!Number.isSafeInteger(baseVersion) || baseVersion < 0) throw new ProtocolError('base_version', 'is not an integer of 0 or more');
  const teams = pick<TeamSettings>(settings.teams, TEAM_FIELDS, 'settings.teams');
  const sim = settings.sim === undefined ? undefined : pick<SimSettings>(settings.sim, SIM_FIELDS, 'settings.sim');
  return {
    type: 'change_settings',
    request_id: requestId,
    base_version: baseVersion,
    settings: sim === undefined ? { teams } : { teams, sim },
  };
}

function pick<T>(part: T, fields: readonly string[], path: string): T {
  const source = part as Readonly<Record<string, number>>;
  const out: Record<string, number> = {};
  for (const f of fields) {
    const v = source[f];
    if (v === undefined || !Number.isFinite(v) || v < 0) throw new ProtocolError(`${path}.${f}`, 'is not a number of 0 or more');
    if (!SHARE_FIELDS.has(f) && !Number.isSafeInteger(v)) throw new ProtocolError(`${path}.${f}`, 'is not an integer');
    out[f] = v;
  }
  return out as T;
}

/** Encodes a client message and checks the size limit of the gateway. */
export function encodeClientMessage(message: ClientMessage): string {
  const frame = JSON.stringify(message);
  if (new TextEncoder().encode(frame).length > MAX_CLIENT_MESSAGE_BYTES) {
    throw new ProtocolError(message.type, `is larger than ${MAX_CLIENT_MESSAGE_BYTES} bytes`);
  }
  return frame;
}

function checkRequestId(id: string): void {
  if (!isValidRoutingId(id)) throw new ProtocolError('request_id', 'is not 1 to 64 of A-Z a-z 0-9 . _ -');
}

function checkLanguages(languages: readonly string[]): void {
  if (languages.length === 0 || languages.length > 32) throw new ProtocolError('languages', 'needs 1 to 32 values');
  if (!languages.every(isValidLang)) throw new ProtocolError('languages', 'has a value that is not a language');
  if (new Set(languages).size !== languages.length) throw new ProtocolError('languages', 'has a value twice');
}
