import { formatCount } from '../../lib/format.ts';
import { useLive } from '../../live/hooks.ts';
import { selectQueues } from '../../live/selectors.ts';
import { Elapsed } from '../../live/timers.tsx';
import { languageName } from '../../ui/lang.ts';
import { LabelWithHint, Panel } from '../../ui/panel.tsx';

// The table stops at 420px (the column header and about ten and a half rows,
// the same cap as the operators table) and scrolls under its sticky column
// header. At 1400 x 900 the panel then ends above the bottom of the window.
// The half row at the bottom shows that there is more. With few languages the
// table is only as high as its rows. The teams panel next to it takes this
// height.
// The focus ring sits just outside the area: inside it, the sticky column
// header would cover its top edge.
const SCROLL = 'max-h-[420px] overflow-auto focus-visible:outline-offset-0';
// The line under the column header is a shadow: a border of a sticky cell
// scrolls away with the collapsed table borders.
const HEAD = 'eyebrow sticky top-0 bg-panel px-4 py-2 font-normal shadow-[inset_0_-1px_0_var(--line)]';

export function QueuesPanel() {
  const queues = useLive(selectQueues);
  const target = useLive((s) => s.welcome?.assignment_target_us ?? null);

  return (
    <Panel title="Queues by language" hint="supervisor.panel.queues" meta={target === null ? undefined : `assignment target ${Math.round(target / 1_000_000)} s`}>
      {/* The region is focusable so that a keyboard can scroll it. */}
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex */}
      <div role="group" aria-label="Queues by language, scrollable" tabIndex={0} className={SCROLL}>
        <table aria-label="Queues by language" className="w-full min-w-[520px] text-sm">
          <thead>
            <tr className="text-left">
              {/* The label keeps the hint button out of the name of the column. */}
              <th scope="col" aria-label="Language" className={HEAD}>
                <LabelWithHint label="Language" hint="supervisor.queues.language" />
              </th>
              <th scope="col" aria-label="Waiting" className={`${HEAD} text-right`}>
                <LabelWithHint label="Waiting" hint="supervisor.queues.waiting" />
              </th>
              <th scope="col" aria-label="Offered" className={`${HEAD} text-right`}>
                <LabelWithHint label="Offered" hint="supervisor.queues.offered" />
              </th>
              <th scope="col" aria-label="Oldest wait" className={`${HEAD} text-right`}>
                <LabelWithHint label="Oldest wait" hint="supervisor.queues.oldest" />
              </th>
              <th scope="col" aria-label="Missed" className={`${HEAD} text-right`}>
                <LabelWithHint label="Missed" hint="supervisor.queues.missed" />
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
