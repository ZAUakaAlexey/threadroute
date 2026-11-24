import { useId, type ReactNode } from 'react';

import type { HelpId } from '../lib/help-texts.ts';
import { HelpHint } from './help-hint.tsx';

interface PanelProps {
  readonly title: string;
  /** A help text about the whole panel, next to the title. */
  readonly hint?: HelpId;
  readonly meta?: ReactNode;
  readonly actions?: ReactNode;
  readonly children: ReactNode;
  readonly className?: string;
}

/** A titled section of a screen. */
export function Panel({ title, hint, meta, actions, children, className = '' }: PanelProps) {
  const headingId = useId();
  return (
    <section aria-labelledby={headingId} className={`min-w-0 rounded-lg border border-line bg-panel ${className}`}>
      <header className="flex min-h-11 shrink-0 flex-wrap items-center gap-x-3 gap-y-1 border-b border-line px-4 py-2">
        {/* The hint is next to the heading, not in it, so the heading keeps its name. */}
        <div className="flex items-center gap-1.5">
          <h2 id={headingId} className="eyebrow">
            {title}
          </h2>
          {hint !== undefined && <HelpHint id={hint} label={title} />}
        </div>
        {meta !== undefined && <div className="text-xs text-ink-3">{meta}</div>}
        {actions !== undefined && <div className="ml-auto flex items-center gap-2">{actions}</div>}
      </header>
      {children}
    </section>
  );
}

interface LabelWithHintProps {
  readonly label: string;
  readonly hint: HelpId;
}

/** A column header or a short label with its hint. */
export function LabelWithHint({ label, hint }: LabelWithHintProps) {
  return (
    <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
      {label}
      <HelpHint id={hint} label={label} />
    </span>
  );
}
