import { useLive } from '../../live/hooks.ts';
import { selectOperators } from '../../live/selectors.ts';
import type { OperatorRow } from '../../protocol/types.ts';
import { Panel } from '../../ui/panel.tsx';
import { Meter } from '../../ui/stats.tsx';
import { VirtualTable, type Column } from '../../ui/virtual-table.tsx';

const COLUMNS: readonly Column<OperatorRow>[] = [
  {
    key: 'id',
    header: 'Operator',
    render: (o) => <span className="truncate font-mono text-[13px]">{o.id}</span>,
  },
  {
    key: 'kind',
    header: 'Kind',
    render: (o) =>
      o.kind === 'human' ? (
        <span className="rounded-full bg-accent-soft px-2 py-0.5 text-xs font-medium text-accent">human</span>
      ) : (
        <span className="text-xs text-ink-3">simulated</span>
      ),
  },
  {
    key: 'presence',
    header: 'Presence',
    render: (o) => (
      <span className={`flex items-center gap-1.5 text-xs ${o.presence === 'online' ? 'text-ink' : 'text-ink-3'}`}>
        <span aria-hidden="true" className={`size-1.5 rounded-full ${o.presence === 'online' ? 'bg-done' : 'bg-ink-3/50'}`} />
        {o.presence}
      </span>
    ),
  },
  {
    key: 'load',
    header: 'Load',
    render: (o) => (
      <span className="flex items-center gap-2">
        <Meter value={o.load} max={o.capacity} label={`Load of ${o.id}`} />
        <span className="font-mono text-xs tnum">
          {o.load}/{o.capacity}
          {o.offered > 0 && <span className="text-accent"> +{o.offered} offered</span>}
        </span>
      </span>
    ),
  },
  {
    key: 'languages',
    header: 'Languages',
    render: (o) => <span className="truncate font-mono text-xs text-ink-2">{o.languages.join(' ')}</span>,
  },
  {
    key: 'team',
    header: 'Team',
    render: (o) => <span className="truncate font-mono text-xs text-ink-3">{o.team ?? ''}</span>,
  },
];

export function OperatorsPanel() {
  const operators = useLive(selectOperators);
  const online = operators.filter((o) => o.presence === 'online').length;

  return (
    <Panel title="Operators" meta={`${online} of ${operators.length} online`}>
      <VirtualTable
        label="Operators"
        rows={operators}
        columns={COLUMNS}
        template="minmax(120px,1.2fr) 90px 80px minmax(150px,1fr) minmax(100px,1fr) minmax(100px,1fr)"
        rowKey={(o) => o.id}
        maxHeight={360}
        empty="No operators yet."
        minWidth={760}
      />
    </Panel>
  );
}
