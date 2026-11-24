import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { useId } from 'react';

import { useLive, useSession } from '../../live/hooks.ts';
import { useSettings } from '../../live/settings-store.ts';
import { Meter } from '../../ui/stats.tsx';
import { CheckIcon, ChevronDownIcon } from '../../ui/icons.tsx';
import { PICKABLE_LANGUAGES, languageName } from '../../ui/lang.ts';
import { HelpHint } from '../../ui/help-hint.tsx';
import { Panel } from '../../ui/panel.tsx';

export function MePanel() {
  const me = useLive((s) => s.me);
  const operatorId = useLive((s) => s.welcome?.operator_id ?? null);

  return (
    <Panel title="You" hint="operator.panel.you" meta={operatorId === null ? 'connecting' : <span className="font-mono">{operatorId}</span>}>
      <div className="flex flex-col gap-4 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <PresenceSwitch />
          {me !== null && (
            <span className="flex items-center gap-2 text-xs text-ink-2">
              <Meter value={me.load} max={me.capacity} label="Your load against capacity" />
              <span className="font-mono tnum">
                {me.load} of {me.capacity}
              </span>
              slots
              <HelpHint id="operator.slots" label="Slots" />
            </span>
          )}
        </div>
        <LanguagePicker />
      </div>
    </Panel>
  );
}

function PresenceSwitch() {
  const session = useSession();
  const labelId = useId();
  const presence = useLive((s) => s.me?.presence ?? 'offline');
  const isBusy = useLive((s) => s.busy.has('presence'));
  const isConnected = useLive((s) => s.connection.phase === 'open' && s.me !== null);
  const isOnline = presence === 'online';

  function handleToggle() {
    void session.setOnline(!isOnline);
  }

  return (
    <div className="flex items-center gap-3">
      <button
        type="button"
        role="switch"
        aria-checked={isOnline}
        aria-labelledby={labelId}
        disabled={!isConnected || isBusy}
        onClick={handleToggle}
        className="group relative h-6 w-11 shrink-0 rounded-full border border-line-strong bg-sunken transition-colors disabled:opacity-50 aria-checked:border-done aria-checked:bg-done"
      >
        <span className="absolute top-0.5 left-0.5 size-[18px] rounded-full bg-panel shadow-sm transition-transform group-aria-checked:translate-x-5" />
      </button>
      <span className="text-sm">
        <span id={labelId} className="font-medium">
          Online
        </span>
        <span className="block text-xs text-ink-3">{isOnline ? 'You get offers now' : 'Switch on to get offers'}</span>
      </span>
      <HelpHint id="operator.online" label="Online" />
    </div>
  );
}

function LanguagePicker() {
  const session = useSession();
  const serverLanguages = useLive((s) => s.me?.languages ?? null);
  const savedLanguages = useSettings((s) => s.languages);
  const setSaved = useSettings((s) => s.setLanguages);
  const isConnected = useLive((s) => s.connection.phase === 'open');
  const languages = serverLanguages ?? savedLanguages;

  function handleToggle(code: string, isChecked: boolean) {
    const next = isChecked ? [...languages, code] : languages.filter((l) => l !== code);
    if (next.length === 0) return;
    const sorted = [...new Set(next)].sort();
    setSaved(sorted);
    if (isConnected) void session.setLanguages(sorted);
  }

  return (
    <div className="flex flex-col gap-2">
      <span className="eyebrow flex items-center gap-1.5">
        Languages
        <HelpHint id="operator.languages" label="Languages" />
      </span>
      <div className="flex flex-wrap items-center gap-1.5">
        {languages.map((code) => (
          <span key={code} className="rounded-md bg-sunken px-2 py-1 font-mono text-xs text-ink-2">
            {code}
          </span>
        ))}
        <DropdownMenu.Root>
          <DropdownMenu.Trigger asChild>
            <button
              type="button"
              className="inline-flex h-7 items-center gap-1 rounded-md border border-line-strong px-2 text-xs text-ink-2 hover:bg-sunken hover:text-ink"
            >
              Change languages
              <ChevronDownIcon className="size-3.5" />
            </button>
          </DropdownMenu.Trigger>
          <DropdownMenu.Portal>
            <DropdownMenu.Content
              align="start"
              sideOffset={6}
              className="z-50 min-w-56 rounded-lg border border-line-strong bg-panel p-1 shadow-lg"
            >
              <DropdownMenu.Label className="eyebrow px-2 py-1.5">Queues you serve</DropdownMenu.Label>
              {PICKABLE_LANGUAGES.map((code) => (
                <DropdownMenu.CheckboxItem
                  key={code}
                  checked={languages.includes(code)}
                  disabled={languages.length === 1 && languages.includes(code)}
                  onCheckedChange={(checked) => handleToggle(code, checked)}
                  onSelect={(event) => event.preventDefault()}
                  className="flex cursor-default items-center gap-2 rounded-md px-2 py-1.5 text-sm outline-none select-none data-[disabled]:opacity-50 data-[highlighted]:bg-sunken"
                >
                  <span className="flex size-4 items-center justify-center">
                    <DropdownMenu.ItemIndicator>
                      <CheckIcon className="size-4 text-accent" />
                    </DropdownMenu.ItemIndicator>
                  </span>
                  <span className="flex-1">{languageName(code)}</span>
                  <span className="font-mono text-xs text-ink-3">{code}</span>
                </DropdownMenu.CheckboxItem>
              ))}
            </DropdownMenu.Content>
          </DropdownMenu.Portal>
        </DropdownMenu.Root>
      </div>
    </div>
  );
}
