import * as RadixTooltip from '@radix-ui/react-tooltip';
import type { ReactElement, ReactNode } from 'react';

interface TooltipProps {
  readonly content: ReactNode;
  /** A focusable element, so keyboard users get the tooltip too. */
  readonly children: ReactElement;
}

export function Tooltip({ content, children }: TooltipProps) {
  return (
    <RadixTooltip.Root delayDuration={250}>
      <RadixTooltip.Trigger asChild>{children}</RadixTooltip.Trigger>
      <RadixTooltip.Portal>
        <RadixTooltip.Content
          sideOffset={6}
          className="z-50 max-w-72 rounded-md border border-line-strong bg-panel px-3 py-2 text-xs leading-relaxed text-ink-2 shadow-lg"
        >
          {content}
          <RadixTooltip.Arrow className="fill-line-strong" />
        </RadixTooltip.Content>
      </RadixTooltip.Portal>
    </RadixTooltip.Root>
  );
}

export const TooltipProvider = RadixTooltip.Provider;
