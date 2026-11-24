// Language names for the screens. "und" is the general queue.

const names = new Intl.DisplayNames(['en'], { type: 'language' });

export function languageName(code: string): string {
  if (code === 'und') return 'General queue';
  try {
    return names.of(code) ?? code;
  } catch {
    return code;
  }
}

/** Languages the operator can pick. */
export const PICKABLE_LANGUAGES: readonly string[] = ['en', 'de', 'es', 'fr', 'ja', 'pt', 'und'];
