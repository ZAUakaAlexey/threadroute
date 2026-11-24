import { formatCount } from '../../lib/format.ts';
import { useLive } from '../../live/hooks.ts';
import { selectQueues } from '../../live/selectors.ts';
import { Elapsed } from '../../live/timers.tsx';
import { languageName } from '../../ui/lang.ts';
import { Panel } from '../../ui/panel.tsx';

export function QueuesPanel() {
  const queues = useLive(selectQueues);
  const target = useLive((s) => s.welcome?.assignment_target_us ?? null);

  return (
    <Panel title="Queues by language" meta={target === null ? undefined : `assignment target ${Math.round(target / 1_000_000)} s`}>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[520px] text-sm">
          <thead>
            <tr className="border-b border-line text-left">
              <th scope="col" className="eyebrow px-4 py-2 font-normal">
                Language
              </th>
              <th scope="col" className="eyebrow px-4 py-2 text-right font-normal">
                Waiting
              </th>
              <th scope="col" className="eyebrow px-4 py-2 text-right font-normal">
                Offered
              </th>
              <th scope="col" className="eyebrow px-4 py-2 text-right font-normal">
                Oldest wait
              </th>
              <th scope="col" className="eyebrow px-4 py-2 text-right font-normal">
                Missed
              </th>
            </tr>
          </thead>
          <tbody>
            {queues.map((q) => (
              <tr key={q.id} className="border-b border-line last:border-b-0">
                <th scope="row" className="px-4 py-2 text-left font-normal">
                  <span className="mr-2 font-mono text-xs text-ink-3">{q.id}</span>
                  {languageName(q.id)}
                </th>
                <td className={`px-4 py-2 text-right font-mono whitespace-nowrap tnum ${q.waiting > 0 ? 'text-wait' : 'text-ink-3'}`}>
                  {formatCount(q.waiting)}
                  {q.team_waiting > 0 && <span className="ml-1 text-xs text-ink-3">({q.team_waiting} team)</span>}
                </td>
                <td className="px-4 py-2 text-right font-mono tnum">{formatCount(q.offered)}</td>
                <td className="px-4 py-2 text-right">
                  {q.oldest_queued_at_us === undefined ? (
                    <span className="text-ink-3">none</span>
                  ) : (
                    <Elapsed sinceUs={q.oldest_queued_at_us} label={`Oldest wait in ${languageName(q.id)}`} />
                  )}
                </td>
                <td className={`px-4 py-2 text-right font-mono tnum ${q.missed_assignment > 0 ? 'font-medium text-late' : 'text-ink-3'}`}>
                  {formatCount(q.missed_assignment)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {queues.length === 0 && <p className="px-4 py-3 text-sm text-ink-3">No queue yet.</p>}
      </div>
    </Panel>
  );
}
