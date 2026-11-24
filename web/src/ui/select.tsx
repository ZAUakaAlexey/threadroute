import type { SelectHTMLAttributes } from 'react';

import { ChevronDownIcon } from './icons.tsx';

interface SelectProps extends SelectHTMLAttributes<HTMLSelectElement> {
  /** A fixed width class, so the control does not change size with the value. */
  readonly width: string;
}

/**
 * A native select in the style of the small controls (like the "Change
 * languages" button): 28px high, 12px text, our own chevron. The focus ring
 * covers the border, so there is one frame only.
 */
export function Select({ width, className = '', ...props }: SelectProps) {
  return (
    <span className={`relative inline-flex ${width} ${className}`}>
      <select
        className="h-7 w-full min-w-0 cursor-pointer appearance-none truncate rounded-md border border-line-strong bg-panel pr-7 pl-2 text-xs text-ink-2 hover:bg-sunken hover:text-ink focus-visible:outline-offset-[-1px]"
        {...props}
      />
      <ChevronDownIcon className="pointer-events-none absolute top-1/2 right-2 size-3.5 -translate-y-1/2 text-ink-3" />
    </span>
  );
}
