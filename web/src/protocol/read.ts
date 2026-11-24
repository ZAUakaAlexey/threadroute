// Small run-time readers for JSON objects. Each reader checks one field and
// throws ProtocolError with the path of the field. Error messages hold the
// path and the rule only, never the value: a value can be post text or a
// token (decision 0004).

export class ProtocolError extends Error {
  readonly path: string;

  constructor(path: string, rule: string) {
    super(`${path}: ${rule}`);
    this.name = 'ProtocolError';
    this.path = path;
  }
}

export type Rec = Readonly<Record<string, unknown>>;

export const MAX_ID_LEN = 512;
const LANG_RE = /^[a-z]{2,3}$/;
const ROUTING_ID_RE = /^[A-Za-z0-9._-]{1,64}$/;

export function isRecord(v: unknown): v is Rec {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

export function asRecord(v: unknown, path: string): Rec {
  if (!isRecord(v)) throw new ProtocolError(path, 'is not an object');
  return v;
}

function at(path: string, key: string): string {
  return path === '' ? key : `${path}.${key}`;
}

export function readString(o: Rec, key: string, path: string, max = MAX_ID_LEN): string {
  const v = o[key];
  if (typeof v !== 'string') throw new ProtocolError(at(path, key), 'is not a string');
  if (v.length === 0 || v.length > max) throw new ProtocolError(at(path, key), `is not 1 to ${max} characters`);
  return v;
}

export function readOptString(o: Rec, key: string, path: string, max = MAX_ID_LEN): string | undefined {
  return o[key] === undefined ? undefined : readString(o, key, path, max);
}

/** Any string, also empty. Used for post text, which can be "". */
export function readOptText(o: Rec, key: string, path: string): string | undefined {
  const v = o[key];
  if (v === undefined) return undefined;
  if (typeof v !== 'string') throw new ProtocolError(at(path, key), 'is not a string');
  return v;
}

export function readInt(o: Rec, key: string, path: string, min = 0): number {
  const v = o[key];
  if (typeof v !== 'number' || !Number.isSafeInteger(v)) {
    throw new ProtocolError(at(path, key), 'is not a safe integer');
  }
  if (v < min) throw new ProtocolError(at(path, key), `is less than ${min}`);
  return v;
}

export function readOptInt(o: Rec, key: string, path: string, min = 0): number | undefined {
  return o[key] === undefined ? undefined : readInt(o, key, path, min);
}

export function readNumber(o: Rec, key: string, path: string, isPositive = false): number {
  const v = o[key];
  if (typeof v !== 'number' || !Number.isFinite(v)) throw new ProtocolError(at(path, key), 'is not a number');
  if (v < 0 || (isPositive && v === 0)) throw new ProtocolError(at(path, key), 'is out of range');
  return v;
}

export function readBool(o: Rec, key: string, path: string): boolean {
  const v = o[key];
  if (typeof v !== 'boolean') throw new ProtocolError(at(path, key), 'is not a boolean');
  return v;
}

export function readEnum<T extends string>(o: Rec, key: string, path: string, values: readonly T[]): T {
  const v = o[key];
  if (typeof v !== 'string' || !(values as readonly string[]).includes(v)) {
    throw new ProtocolError(at(path, key), `is not one of ${values.join(', ')}`);
  }
  return v as T;
}

export function readOptEnum<T extends string>(o: Rec, key: string, path: string, values: readonly T[]): T | undefined {
  return o[key] === undefined ? undefined : readEnum(o, key, path, values);
}

export function readArray<T>(o: Rec, key: string, path: string, item: (v: unknown, path: string) => T): T[] {
  const v = o[key];
  if (!Array.isArray(v)) throw new ProtocolError(at(path, key), 'is not a list');
  return v.map((x, i) => item(x, `${at(path, key)}[${i}]`));
}

export function readOptArray<T>(
  o: Rec,
  key: string,
  path: string,
  item: (v: unknown, path: string) => T,
): T[] | undefined {
  return o[key] === undefined ? undefined : readArray(o, key, path, item);
}

export function readOptObject<T>(o: Rec, key: string, path: string, read: (v: Rec, path: string) => T): T | undefined {
  const v = o[key];
  if (v === undefined) return undefined;
  return read(asRecord(v, at(path, key)), at(path, key));
}

export function readObject<T>(o: Rec, key: string, path: string, read: (v: Rec, path: string) => T): T {
  if (o[key] === undefined) throw new ProtocolError(at(path, key), 'is not set');
  return read(asRecord(o[key], at(path, key)), at(path, key));
}

export function langItem(v: unknown, path: string): string {
  if (typeof v !== 'string' || !LANG_RE.test(v)) throw new ProtocolError(path, 'is not 2 or 3 lower case letters');
  return v;
}

export function idItem(v: unknown, path: string): string {
  if (typeof v !== 'string' || v.length === 0 || v.length > MAX_ID_LEN) {
    throw new ProtocolError(path, `is not 1 to ${MAX_ID_LEN} characters`);
  }
  return v;
}

export function routingIdItem(v: unknown, path: string): string {
  if (typeof v !== 'string' || !ROUTING_ID_RE.test(v)) throw new ProtocolError(path, 'is not a routing ID');
  return v;
}

export function readLang(o: Rec, key: string, path: string): string {
  return langItem(o[key], at(path, key));
}

export function readRoutingId(o: Rec, key: string, path: string): string {
  return routingIdItem(o[key], at(path, key));
}

export function readOptRoutingId(o: Rec, key: string, path: string): string | undefined {
  return o[key] === undefined ? undefined : readRoutingId(o, key, path);
}

export function isValidLang(s: string): boolean {
  return LANG_RE.test(s);
}

export function isValidRoutingId(s: string): boolean {
  return ROUTING_ID_RE.test(s);
}

type OptionalKeys<T> = { [K in keyof T]-?: object extends Pick<T, K> ? K : never }[keyof T];
type Draft<T> = Omit<T, OptionalKeys<T>> & { [K in OptionalKeys<T>]?: T[K] | undefined };

/** Builds T from a draft whose optional fields may be undefined, dropping them. */
export function compact<T extends object>(draft: Draft<T>): T {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(draft)) {
    if (v !== undefined) out[k] = v;
  }
  return out as T;
}
