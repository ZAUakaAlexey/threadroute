import { useVirtualizer } from '@tanstack/react-virtual';
import { useRef } from 'react';

import { useLive, useSession } from '../../live/hooks.ts';
import { selectHeld } from '../../live/selectors.ts';
import { Countdown } from '../../live/timers.tsx';
import type { OperatorConversation } from '../../protocol/types.ts';
import { Panel } from '../../ui/panel.tsx';
import { WaitTimesInline } from './wait-times.tsx';

const ROW_HEIGHT = 68;
const MAX_VISIBLE_ROWS = 4;

export function HeldPanel() {
  const held = useLive(selectHeld);
  const capacity = useLive((s) => s.me?.capacity ?? 2);
  const selectedId = useLive((s) => s.selectedId);
  const scrollRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line react-hooks/incompatible-library -- the virtualizer keeps its own state on purpose
  const virtualizer = useVirtualizer({
    count: held.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 4,
    initialRect: { width: 360, height: ROW_HEIGHT * MAX_VISIBLE_ROWS },
  });
  const height = Math.min(held.length, MAX_VISIBLE_ROWS) * ROW_HEIGHT;

  return (
    <Panel title="Your conversations" hint="operator.panel.held" meta={`${held.length} held, capacity ${capacity}`}>
      {held.length === 0 ? (
        <p className="px-4 py-6 text-sm text-ink-3">Accepted conversations appear here.</p>
      ) : (
        <div ref={scrollRef} className="overflow-y-auto" style={{ height }}>
          <ul aria-label="Conversations you hold" className="relative" style={{ height: virtualizer.getTotalSize() }}>
            {virtualizer.getVirtualItems().map((item) => {
              const row = held[item.index];
              if (row === undefined) return null;
              return (
                <li
                  key={row.id}
                  aria-setsize={held.length}
                  aria-posinset={item.index + 1}
                  className="absolute inset-x-0 top-0"
                  style={{ height: item.size, transform: `translateY(${item.start}px)` }}
                >
                  <HeldRow row={row} isSelected={row.id === selectedId} />
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </Panel>
  );
}

function HeldRow({ row, isSelected }: { readonly row: OperatorConversation; readonly isSelected: boolean }) {
  const session = useSession();

  function handleSelect() {
    session.select(row.id);
  }

  return (
    <button
      type="button"
      onClick={handleSelect}
      aria-current={isSelected ? 'true' : undefined}
      className="grid h-full w-full grid-cols-[auto_1fr_auto] items-center gap-x-3 border-b border-line px-4 text-left hover:bg-sunken aria-[current=true]:bg-accent-soft/60 aria-[current=true]:shadow-[inset_3px_0_0_var(--accent)]"
    >
      <span className="rounded bg-sunken px-1.5 py-0.5 font-mono text-[11px] text-ink-2">{row.lang}</span>
      <span className="min-w-0">
        <span className="block truncate font-mono text-[13px]">{row.customer}</span>
        <span className="flex min-w-0 gap-3 text-xs text-ink-3">
          <WaitTimesInline conversation={row} />
          <span className="truncate">
            <span className="font-mono tnum text-ink-2">{row.customer_messages + row.company_messages}</span>{' '}
            {row.customer_messages + row.company_messages === 1 ? 'message' : 'messages'}
          </span>
        </span>
      </span>
      <FirstResponseBadge row={row} />
    </button>
  );
}

interface FirstResponseBadgeProps {
  readonly row: OperatorConversation;
  /** Show the words "reply in" above the countdown. */
  readonly hasCaption?: boolean;
}

export function FirstResponseBadge({ row, hasCaption = true }: FirstResponseBadgeProps) {
  if (row.replied) {
    return <span className="rounded-full bg-done-soft px-2 py-0.5 text-xs font-medium text-done">Replied</span>;
  }
  if (row.first_response_missed) {
    return <span className="rounded-full bg-late-soft px-2 py-0.5 text-xs font-medium text-late">Reply late</span>;
  }
  if (row.first_response_due_us === undefined) return <span />;
  const countdown = <Countdown deadlineUs={row.first_response_due_us} label="Time to first response" />;
  if (!hasCaption) return countdown;
  return (
    <span className="flex flex-col items-end text-xs">
      <span className="text-ink-3">reply in</span>
      {countdown}
    </span>
  );
}
