// Operator settings that survive a reload: the chosen languages only.

import { useStore } from 'zustand';
import { createStore } from 'zustand/vanilla';

import { loadLanguages, saveLanguages } from './settings.ts';

interface SettingsState {
  readonly languages: readonly string[];
  readonly setLanguages: (languages: readonly string[]) => void;
}

export const settingsStore = createStore<SettingsState>()((set) => ({
  languages: loadLanguages(),
  setLanguages: (languages) => {
    saveLanguages(languages);
    set({ languages });
  },
}));

export function useSettings<T>(selector: (s: SettingsState) => T): T {
  return useStore(settingsStore, selector);
}
