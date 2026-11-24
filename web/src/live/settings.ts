// What the console keeps in the browser: settings in localStorage and the
// resume token in sessionStorage (docs/gateway-protocol.md, "Reconnecting").
// Never post text, IDs of conversations, or anything from the live view.

import { isValidLang } from '../protocol/read.ts';
import type { ResumeTokenStore } from '../protocol/client.ts';

const LANGUAGES_KEY = 'threadroute.console.languages';
const RESUME_KEY = 'threadroute.console.resume';
export const DEFAULT_LANGUAGES: readonly string[] = ['en', 'und'];

function safely<T>(read: () => T, fallback: T): T {
  try {
    return read();
  } catch {
    return fallback;
  }
}

export function loadLanguages(): readonly string[] {
  return safely(() => {
    const raw = window.localStorage.getItem(LANGUAGES_KEY);
    if (raw === null) return DEFAULT_LANGUAGES;
    const value: unknown = JSON.parse(raw);
    if (!Array.isArray(value)) return DEFAULT_LANGUAGES;
    const langs = [...new Set(value.filter((v): v is string => typeof v === 'string' && isValidLang(v)))];
    return langs.length > 0 && langs.length <= 32 ? langs : DEFAULT_LANGUAGES;
  }, DEFAULT_LANGUAGES);
}

export function saveLanguages(languages: readonly string[]): void {
  safely(() => window.localStorage.setItem(LANGUAGES_KEY, JSON.stringify(languages)), undefined);
}

export const sessionResumeTokens: ResumeTokenStore = {
  load: () => safely(() => window.sessionStorage.getItem(RESUME_KEY) ?? undefined, undefined),
  save: (token) => safely(() => window.sessionStorage.setItem(RESUME_KEY, token), undefined),
  clear: () => safely(() => window.sessionStorage.removeItem(RESUME_KEY), undefined),
};

/** The site's sign-in token, when the site keeps one under `key`. */
export function readSiteToken(key: string): string | undefined {
  if (key === '') return undefined;
  return safely(() => window.localStorage.getItem(key) ?? undefined, undefined);
}
