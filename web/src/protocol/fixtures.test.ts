// @vitest-environment node
// Every example in internal/gwapi/testdata (and in its settings folder, with
// its own index) goes through the parser and back to JSON, and the client
// examples are also made by the builders. The last test of each folder
// fails when a fixture file exists that no test here loaded.

import { FIXTURE_DIR, SETTINGS_FIXTURE_DIR, listFixtureFiles, readFixtureIndex, readFixtureText, type FixtureEntry } from '../test/fixtures.ts';
import { buildChangeSettings, buildHello, buildOfferAction, buildPresenceAction, buildSetLanguages, buildUnwatch, buildWatch } from './build.ts';
import { parseClientMessage, parsePublicSnapshot, parseServerMessage } from './parse.ts';
import type { Action, ClientMessage, ServerMessage, SettingsBlock } from './types.ts';

function roundTrip(value: unknown): unknown {
  return JSON.parse(JSON.stringify(value)) as unknown;
}

function build(message: ClientMessage): ClientMessage {
  switch (message.type) {
    case 'hello':
      return buildHello({
        view: message.view,
        ...(message.languages ? { languages: message.languages } : {}),
        token: message.token,
        resumeToken: message.resume_token,
      });
    case 'watch':
      return buildWatch(message.request_id, message.conversation_id);
    case 'unwatch':
      return buildUnwatch(message.request_id, message.conversation_id);
    case 'action':
      return buildAction(message);
    case 'change_settings':
      return buildChangeSettings(message.request_id, message.base_version, message.settings);
  }
}

function buildAction(message: Action): Action {
  switch (message.action) {
    case 'online':
    case 'offline':
      return buildPresenceAction(message.request_id, message.action);
    case 'set_languages':
      return buildSetLanguages(message.request_id, message.languages ?? []);
    default:
      return buildOfferAction(message.request_id, message.action, message.offer_id ?? '');
  }
}

function hasText(message: ServerMessage): boolean {
  return message.type === 'messages' && message.messages.some((m) => m.text !== undefined);
}

function settingsOf(message: ServerMessage): SettingsBlock | undefined {
  if (message.type === 'welcome') return message.settings;
  if (message.type === 'settings') return message.settings;
  return undefined;
}

describe.each([
  ['testdata', FIXTURE_DIR],
  ['testdata/settings', SETTINGS_FIXTURE_DIR],
])('protocol fixtures in %s', (_name, dir) => {
  const index = readFixtureIndex(dir);
  const loaded = new Set<string>();

  function load(entry: FixtureEntry): { readonly text: string; readonly json: unknown } {
    loaded.add(entry.file);
    const text = readFixtureText(entry.file, dir);
    return { text, json: JSON.parse(text) as unknown };
  }

  it('index.json lists exactly the files on disk', () => {
    expect(index.map((e) => e.file).sort()).toEqual(listFixtureFiles(dir));
  });

  describe.each(index.filter((e) => e.direction === 'server'))('server $file', (entry) => {
    it('parses for its level and serializes to the same JSON', () => {
      const { text, json } = load(entry);
      const level = entry.level === '' ? undefined : entry.level;
      const parsed = parseServerMessage(text, level === undefined ? {} : { level });
      expect(parsed.kind).toBe('message');
      if (parsed.kind !== 'message') return;
      expect(parsed.message.type).toBe(entry.type);
      expect(roundTrip(parsed.message)).toEqual(json);
      if (entry.level === 'demo') expect(hasText(parsed.message)).toBe(false);
      // "by" follows the right to change settings, never the level.
      const settings = settingsOf(parsed.message);
      if (settings !== undefined && entry.may_change_settings !== undefined) {
        expect(settings.may_change).toBe(entry.may_change_settings);
        if (!settings.may_change) expect(settings.changes.filter((c) => c.by !== undefined)).toEqual([]);
      }
    });
  });

  describe.each(index.filter((e) => e.direction === 'client'))('client $file', (entry) => {
    it('parses, serializes to the same JSON, and the builders make it', () => {
      const { text, json } = load(entry);
      const parsed = parseClientMessage(text);
      expect(parsed.type).toBe(entry.type);
      expect(roundTrip(parsed)).toEqual(json);
      expect(roundTrip(build(parsed))).toEqual(json);
    });
  });

  describe.each(index.filter((e) => e.direction === 'public'))('public $file', (entry) => {
    it('parses and serializes to the same JSON', () => {
      const { json } = load(entry);
      expect(roundTrip(parsePublicSnapshot(json))).toEqual(json);
    });
  });

  it('every fixture file was loaded by a test above', () => {
    expect([...loaded].sort()).toEqual(listFixtureFiles(dir));
  });
});
