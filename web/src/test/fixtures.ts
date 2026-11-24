// Reads the protocol examples of the Go side, so both sides check the same
// files (docs/gateway-protocol.md, "Examples"). The examples of decision
// 0014 live in their own folder with their own index.

import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';

import type { Level, View } from '../protocol/types.ts';

export const FIXTURE_DIR = resolve(import.meta.dirname, '../../../internal/gwapi/testdata');
export const SETTINGS_FIXTURE_DIR = resolve(FIXTURE_DIR, 'settings');

export interface FixtureEntry {
  readonly file: string;
  readonly direction: 'client' | 'server' | 'public';
  readonly type: string;
  readonly level: Level | '';
  readonly view: View | '';
  readonly purpose: string;
  /** Settings examples only: the right of the connection to change settings. */
  readonly may_change_settings?: boolean;
}

export function readFixtureIndex(dir = FIXTURE_DIR): readonly FixtureEntry[] {
  return JSON.parse(readFileSync(resolve(dir, 'index.json'), 'utf8')) as FixtureEntry[];
}

export function listFixtureFiles(dir = FIXTURE_DIR): readonly string[] {
  return readdirSync(dir)
    .filter((f) => f.endsWith('.json') && f !== 'index.json')
    .sort();
}

export function readFixtureText(file: string, dir = FIXTURE_DIR): string {
  return readFileSync(resolve(dir, file), 'utf8');
}

export function readFixture(file: string, dir = FIXTURE_DIR): unknown {
  return JSON.parse(readFixtureText(file, dir)) as unknown;
}

export function readSettingsFixture(file: string): unknown {
  return readFixture(file, SETTINGS_FIXTURE_DIR);
}
