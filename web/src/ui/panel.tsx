import { useId, type ReactNode } from 'react';

interface PanelProps {
  readonly title: string;
  readonly meta?: ReactNode;
  readonly actions?: ReactNode;
  readonly children: ReactNode;
  readonly className?: string;
}

/** A titled section of a screen. */
export function Panel({ title, meta, actions, children, className = '' }: PanelProps) {
  const headingId = useId();
  return (
    <section aria-labelledby={headingId} className={`min-w-0 rounded-lg border border-line bg-panel ${className}`}>
      <header className="flex min-h-11 shrink-0 flex-wrap items-center gap-x-3 gap-y-1 border-b border-line px-4 py-2">
        <h2 id={headingId} className="eyebrow">
          {title}
        </h2>
        {meta !== undefined && <div className="text-xs text-ink-3">{meta}</div>}
        {actions !== undefined && <div className="ml-auto flex items-center gap-2">{actions}</div>}
      </header>
      {children}
    </section>
  );
}
