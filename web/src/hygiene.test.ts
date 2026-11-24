// @vitest-environment node
// Rules for every file of the console that is kept in git: plain ASCII
// text (no dashes, arrows or emoji), and no real-looking account IDs, post
// URIs or e-mail addresses (decision 0004). Synthetic IDs start with
// "did:plc:test" or "did:plc:synth".

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';

const WEB = resolve(import.meta.dirname, '..');
const WORKFLOW = resolve(WEB, '../.github/workflows/web.yml');
const SKIP_DIRS = new Set(['node_modules', 'dist', 'test-results', 'playwright-report', 'blob-report', '.vite']);
const TEXT_FILE = /\.(ts|tsx|js|mjs|json|css|html|md|svg|yml|yaml|gitignore|npmrc)$|^\.(gitignore|npmrc)$/;
const SKIP_FILES = new Set(['pnpm-lock.yaml']);

function listFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return SKIP_DIRS.has(name) ? [] : listFiles(path);
    return TEXT_FILE.test(name) && !SKIP_FILES.has(name) ? [path] : [];
  });
}

const files = [...listFiles(WEB), WORKFLOW];

describe('repository hygiene', () => {
  it.each(files.map((f) => [relative(WEB, f), f]))('%s is plain and synthetic', (_name, path) => {
    const text = readFileSync(path, 'utf8');
    const nonAscii = Array.from(text).filter((ch) => ch.charCodeAt(0) > 0x7e || (ch.charCodeAt(0) < 0x20 && ch !== '\n' && ch !== '\r' && ch !== '\t'));
    expect(nonAscii.map((ch) => `U+${ch.charCodeAt(0).toString(16).padStart(4, '0')}`)).toEqual([]);
    const dids = text.match(/did:[a-z]+:[a-z0-9]+/g) ?? [];
    expect(dids.filter((d) => !/^did:plc:(test|synth|x$)/.test(d) && d !== 'did:plc:shouldnotshow')).toEqual([]);
    expect(text).not.toMatch(/at:\/\/did/);
    expect(text.match(/[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Za-z]{2,}/g) ?? []).toEqual([]);
  });
});
