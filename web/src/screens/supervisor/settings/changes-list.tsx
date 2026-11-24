// The newest changes of the stand settings: time, who (at the full level,
// when the gateway sends it), and each field from its old to its new value.

import { useId, useState } from 'react';

import { useLive } from '../../../live/hooks.ts';
import { selectLevel } from '../../../live/selectors.ts';
import type { SettingsBlock } from '../../../protocol/types.ts';
import { HelpHint } from '../../../ui/help-hint.tsx';
import { describeField, formatChangeTime } from './describe.ts';

const SHOWN = 5;

export function ChangesList({ block }: { readonly block: SettingsBlock }) {
  // "by" may hold a user ID of the site. The protocol sends it to every
  // connection with the right to change settings; the console shows it at
  // the full level only, so the demo level never shows who changed what.
  const isByShown = useLive(selectLevel) === 'full';
  const [isAll, setIsAll] = useState(false);
  const listId = useId();
  const changes = isAll ? block.changes : block.changes.slice(0, SHOWN);

  function handleToggle() {
    setIsAll((all) => !all);
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-1.5">
        <h3 className="eyebrow">Last changes</h3>
        <HelpHint id="supervisor.settings.changes" label="Last changes" />
      </div>
      {block.changes.length === 0 ? (
        <p className="text-sm text-ink-3">No change yet.</p>
      ) : (
        <ol id={listId} aria-label="Last changes of the stand settings" className="flex flex-col gap-2 text-sm">
          {changes.map((c) => (
            <li key={c.version} className="flex flex-col gap-0.5 sm:flex-row sm:gap-3">
              <span className="shrink-0 text-xs text-ink-3 sm:w-48 sm:pt-0.5">
                <span className="font-mono tnum">{formatChangeTime(c.changed_at_us)}</span>, version {c.version}
                {isByShown && c.by !== undefined && <span> by {c.by}</span>}
              </span>
              <ul className="min-w-0 text-ink-2">
                {c.fields.map((f) => (
                  <li key={f.field}>{describeField(f, block)}</li>
                ))}
              </ul>
            </li>
          ))}
        </ol>
      )}
      {block.changes.length > SHOWN && (
        <button type="button" aria-expanded={isAll} aria-controls={listId} onClick={handleToggle} className="self-start text-xs text-accent underline underline-offset-2">
          {isAll ? 'Show fewer changes' : `Show all ${block.changes.length} changes`}
        </button>
      )}
    </div>
  );
}
