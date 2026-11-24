// The live settings that some help texts name. The live screens provide
// them (live/help-values.tsx); the public page has none, so its texts get
// the fallback words.

import { createContext, use } from 'react';

import { helpText, type HelpId, type HelpValues } from '../lib/help-texts.ts';

export const HelpValuesContext = createContext<HelpValues>({});

/** The text of a hint or description, with the live values filled in. */
export function useHelpText(id: HelpId): string {
  return helpText(id, use(HelpValuesContext));
}
