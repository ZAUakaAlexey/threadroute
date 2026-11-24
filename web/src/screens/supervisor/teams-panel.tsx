import { formatCount } from '../../lib/format.ts';
import { useLive } from '../../live/hooks.ts';
import { selectTeams } from '../../live/selectors.ts';
import { Elapsed } from '../../live/timers.tsx';
import { Panel } from '../../ui/panel.tsx';

export function TeamsPanel() {
  const teams = useLive(selectTeams);

  return (
    <Panel title="Dedicated teams" meta={`${teams.length} ${teams.length === 1 ? 'company' : 'companies'}`}>
      {teams.length === 0 ? (
        <p className="px-4 py-6 text-sm text-ink-3">No company has a dedicated team right now.</p>
      ) : (
        <ul className="divide-y divide-line">
          {teams.map((t) => (
            <li key={t.id} className="grid grid-cols-[1fr_auto] items-center gap-x-4 gap-y-1 px-4 py-3">
              <span className="truncate font-mono text-[13px]" title={t.id}>
                {t.id}
              </span>
              <span className="text-right text-xs text-ink-3">
                formed <Elapsed sinceUs={t.formed_at_us} label="Team age" format="short" className="text-ink-2" /> ago
              </span>
              <span className="text-xs text-ink-2">
                <span className="font-mono text-ink tnum">{t.members.length}</span> operators,{' '}
                <span className="font-mono text-ink tnum">{formatCount(t.active_load)}</span> active conversations
              </span>
              <span className="truncate text-right font-mono text-[11px] text-ink-3">{t.members.join(' ')}</span>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}
