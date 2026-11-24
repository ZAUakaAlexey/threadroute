// @vitest-environment node
// The help texts and the screens cannot drift apart: every ID the screens
// use exists, and every text is used somewhere.

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';

import { HELP_PLACEHOLDERS, HELP_TEXTS, helpDuration, helpText, type HelpId } from './help-texts.ts';

const SRC = resolve(import.meta.dirname, '..');
const ids = Object.keys(HELP_TEXTS) as HelpId[];
const prefixes = new Set(ids.map((id) => id.split('.')[0]));

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === 'test' ? [] : sourceFiles(path);
    return /\.tsx?$/.test(name) && !/\.test\.tsx?$/.test(name) && name !== 'help-texts.ts' ? [path] : [];
  });
}

/** Every quoted string in the code that looks like a help-text ID. */
function usedIds(): Map<string, string[]> {
  const used = new Map<string, string[]>();
  for (const path of sourceFiles(SRC)) {
    for (const match of readFileSync(path, 'utf8').matchAll(/['"`]([a-z]+(?:\.[a-z0-9-]+)+)['"`]/g)) {
      const id = match[1] ?? '';
      if (!prefixes.has(id.split('.')[0] ?? '')) continue;
      used.set(id, [...(used.get(id) ?? []), relative(SRC, path)]);
    }
  }
  return used;
}

describe('help texts', () => {
  const used = usedIds();

  it('has a text for every ID that the screens use', () => {
    expect([...used.keys()].filter((id) => !(id in HELP_TEXTS))).toEqual([]);
  });

  it('has no text that the screens do not use', () => {
    expect(ids.filter((id) => !used.has(id))).toEqual([]);
  });

  it.each(ids)('%s follows the writing rules', (id) => {
    const text = HELP_TEXTS[id];
    // One to three sentences.
    const sentences = text.match(/[.!?](?=\s|$)/g) ?? [];
    expect(sentences.length).toBeGreaterThanOrEqual(1);
    expect(sentences.length).toBeLessThanOrEqual(3);
    // Plain ASCII: no dashes, arrows or emoji.
    expect(Array.from(text).filter((ch) => ch.charCodeAt(0) > 0x7e)).toEqual([]);
    expect(text).not.toMatch(/ - |--/);
    // Only known placeholders.
    for (const [, name] of text.matchAll(/\{(\w+)\}/g)) {
      expect(HELP_PLACEHOLDERS).toContain(name);
    }
  });

  it('keeps the public texts free of live placeholders, because the public page has no settings', () => {
    expect(ids.filter((id) => id.startsWith('public.') && HELP_TEXTS[id].includes('{'))).toEqual([]);
  });

  it('fills the placeholders with the live values or with plain words', () => {
    expect(helpText('supervisor.missed', { assignmentTarget: '60 seconds', firstResponseTarget: '120 seconds' })).toContain(
      'within 60 seconds after the conversation entered the queue',
    );
    expect(helpText('supervisor.missed')).not.toMatch(/[{}]/);
  });

  it('says durations in words', () => {
    expect(helpDuration(20_000_000)).toBe('20 seconds');
    expect(helpDuration(120_000_000)).toBe('120 seconds');
    expect(helpDuration(1_000_000)).toBe('1 second');
    expect(helpDuration(900_000_000)).toBe('15 minutes');
  });
});
