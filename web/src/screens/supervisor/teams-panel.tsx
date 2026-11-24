import { formatCount, pluralWord } from '../../lib/format.ts';
import { useLive } from '../../live/hooks.ts';
import { selectTeams } from '../../live/selectors.ts';
import { Elapsed } from '../../live/timers.tsx';
import { Panel } from '../../ui/panel.tsx';

// Next to the queues panel (xl and up) this panel takes the height of the
// queues panel: contain-size keeps its content out of the row height, and
// self-stretch fills the row. Stacked, the list stops at 360px, like the
// operators table. In both cases the list scrolls under the panel header.
const PANEL = 'flex flex-col xl:self-stretch xl:contain-size';
const LIST = 'max-h-[360px] min-h-0 flex-1 overflow-y-auto focus-visible:outline-offset-[-2px] xl:max-h-none';

export function TeamsPanel() {
  const teams = useLive(selectTeams);

  return (
    <Panel title="Dedicated teams" meta={`${teams.length} ${pluralWord(teams.length, 'company', 'companies')}`} className={PANEL}>
      {teams.length === 0 ? (
        <p className="px-4 py-6 text-sm text-ink-3">No company has a dedicated team right now.</p>
      ) : (
        // The region is focusable so that a keyboard can scroll it.
        // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
        <div role="group" aria-label="Dedicated teams, scrollable" tabIndex={0} className={LIST}>
          <ul aria-label="Dedicated teams" className="divide-y divide-line">
            {teams.map((t) => (
              <li key={t.id} className="grid grid-cols-[1fr_auto] items-center gap-x-4 gap-y-1 px-4 py-3">
                <span className="truncate font-mono text-[13px]" title={t.id}>
                  {t.id}
                </span>
                <span className="text-right text-xs text-ink-3">
                  formed <Elapsed sinceUs={t.formed_at_us} label="Team age" format="short" className="text-ink-2" /> ago
                </span>
                <span className="text-xs text-ink-2">
                  <span className="font-mono text-ink tnum">{t.members.length}</span> {pluralWord(t.members.length, 'operator')},{' '}
                  <span className="font-mono text-ink tnum">{formatCount(t.active_load)}</span> active{' '}
                  {pluralWord(t.active_load, 'conversation')}
                </span>
                <span className="truncate text-right font-mono text-[11px] text-ink-3">{t.members.join(' ')}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </Panel>
  );
}
