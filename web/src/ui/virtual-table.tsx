// A table whose body is virtualized, so a list of thousands of rows renders
// only what is visible. ARIA table roles with row indexes keep it readable
// for screen readers; the scroll area is focusable for keyboard scrolling.

import { useVirtualizer } from '@tanstack/react-virtual';
import { useRef, type ReactNode } from 'react';

export interface Column<T> {
  readonly key: string;
  readonly header: ReactNode;
  readonly render: (row: T) => ReactNode;
  readonly align?: 'start' | 'end';
}

interface VirtualTableProps<T> {
  readonly label: string;
  readonly rows: readonly T[];
  readonly columns: readonly Column<T>[];
  /** CSS grid-template-columns of a row. */
  readonly template: string;
  readonly rowKey: (row: T) => string;
  readonly rowHeight?: number;
  readonly maxHeight?: number;
  readonly empty: ReactNode;
  /** Narrowest width before the table scrolls sideways. */
  readonly minWidth?: number;
}

export function VirtualTable<T>({
  label,
  rows,
  columns,
  template,
  rowKey,
  rowHeight = 40,
  maxHeight = 420,
  empty,
  minWidth = 640,
}: VirtualTableProps<T>) {
  const scrollRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line react-hooks/incompatible-library -- the virtualizer keeps its own state on purpose
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => rowHeight,
    overscan: 8,
    initialRect: { width: minWidth, height: maxHeight },
  });
  const headerHeight = 36;
  const height = Math.min(maxHeight, headerHeight + Math.max(1, rows.length) * rowHeight);
  const cellAlign = (c: Column<T>) => (c.align === 'end' ? 'justify-end text-right' : '');

  return (
    // The region is focusable so that a keyboard can scroll it.
    // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
    <div ref={scrollRef} role="group" aria-label={`${label}, scrollable`} tabIndex={0} className="overflow-auto" style={{ height }}>
      <div role="table" aria-label={label} aria-rowcount={rows.length + 1} style={{ minWidth }}>
        <div role="rowgroup" className="sticky top-0 z-10 bg-panel">
          <div
            role="row"
            aria-rowindex={1}
            className="grid items-center gap-3 border-b border-line px-4"
            style={{ gridTemplateColumns: template, height: headerHeight }}
          >
            {columns.map((c) => (
              <span key={c.key} role="columnheader" className={`eyebrow flex ${cellAlign(c)}`}>
                {c.header}
              </span>
            ))}
          </div>
        </div>
        {rows.length === 0 ? (
          <div className="px-4 py-3 text-sm text-ink-3">{empty}</div>
        ) : (
          <div role="rowgroup" className="relative" style={{ height: virtualizer.getTotalSize() }}>
            {virtualizer.getVirtualItems().map((item) => {
              const row = rows[item.index];
              if (row === undefined) return null;
              return (
                <div
                  key={rowKey(row)}
                  role="row"
                  aria-rowindex={item.index + 2}
                  className="absolute inset-x-0 top-0 grid items-center gap-3 border-b border-line px-4 text-sm hover:bg-sunken/60"
                  style={{ gridTemplateColumns: template, height: item.size, transform: `translateY(${item.start}px)` }}
                >
                  {columns.map((c) => (
                    <span key={c.key} role="cell" className={`flex min-w-0 items-center ${cellAlign(c)}`}>
                      {c.render(row)}
                    </span>
                  ))}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
