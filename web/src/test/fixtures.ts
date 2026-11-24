// Reads the protocol examples of the Go side, so both sides check the same
// files (docs/gateway-protocol.md, "Examples").

import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';

import type { Level, View } from '../protocol/types.ts';

export const FIXTURE_DIR = resolve(import.meta.dirname, '../../../internal/gwapi/testdata');

export interface FixtureEntry {
  readonly file: string;
  readonly direction: 'client' | 'server' | 'public';
  readonly type: string;
  readonly level: Level | '';
  readonly view: View | '';
  readonly purpose: string;
}

export function readFixtureIndex(): readonly FixtureEntry[] {
  return JSON.parse(readFileSync(resolve(FIXTURE_DIR, 'index.json'), 'utf8')) as FixtureEntry[];
}

export function listFixtureFiles(): readonly string[] {
  return readdirSync(FIXTURE_DIR)
    .filter((f) => f.endsWith('.json') && f !== 'index.json')
    .sort();
}

export function readFixtureText(file: string): string {
  return readFileSync(resolve(FIXTURE_DIR, file), 'utf8');
}

export function readFixture(file: string): unknown {
  return JSON.parse(readFixtureText(file)) as unknown;
}
