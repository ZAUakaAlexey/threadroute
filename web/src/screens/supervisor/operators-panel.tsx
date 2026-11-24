import { useLive } from '../../live/hooks.ts';
import { selectOperators } from '../../live/selectors.ts';
import type { OperatorRow } from '../../protocol/types.ts';
import { LabelWithHint, Panel } from '../../ui/panel.tsx';
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
    header: <LabelWithHint label="Kind" hint="supervisor.operators.kind" />,
    label: 'Kind',
    render: (o) =>
      o.kind === 'human' ? (
        <span className="rounded-full bg-accent-soft px-2 py-0.5 text-xs font-medium text-accent">human</span>
      ) : (
        <span className="text-xs text-ink-3">simulated</span>
      ),
  },
  {
    key: 'presence',
    header: <LabelWithHint label="Presence" hint="supervisor.operators.presence" />,
    label: 'Presence',
    render: (o) => (
      <span className={`flex items-center gap-1.5 text-xs ${o.presence === 'online' ? 'text-ink' : 'text-ink-3'}`}>
        <span aria-hidden="true" className={`size-1.5 rounded-full ${o.presence === 'online' ? 'bg-done' : 'bg-ink-3/50'}`} />
        {o.presence}
      </span>
    ),
  },
  {
    key: 'load',
    header: <LabelWithHint label="Load" hint="supervisor.operators.load" />,
    label: 'Load',
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
    header: <LabelWithHint label="Languages" hint="supervisor.operators.languages" />,
    label: 'Languages',
    render: (o) => <span className="truncate font-mono text-xs text-ink-2">{o.languages.join(' ')}</span>,
  },
  {
    key: 'team',
    header: <LabelWithHint label="Team" hint="supervisor.operators.team" />,
    label: 'Team',
    render: (o) => <span className="truncate font-mono text-xs text-ink-3">{o.team ?? ''}</span>,
  },
];

export function OperatorsPanel() {
  const operators = useLive(selectOperators);
  const online = operators.filter((o) => o.presence === 'online').length;

  return (
    <Panel title="Operators" hint="supervisor.panel.operators" meta={`${online} of ${operators.length} online`}>
      <VirtualTable
        label="Operators"
        rows={operators}
        columns={COLUMNS}
        template="minmax(120px,1.2fr) 90px 96px minmax(150px,1fr) minmax(100px,1fr) minmax(100px,1fr)"
        rowKey={(o) => o.id}
        maxHeight={360}
        empty="No operators yet."
        minWidth={760}
      />
    </Panel>
  );
}
