// A small "i" button next to a label that opens a short help text (a
// toggletip). It is a Radix Popover: the text is in a portal, so a panel
// that scrolls or clips does not cut it, and Radix keeps it inside the
// window near the edges.
//
// - Mouse: hovering shows the text and moving away hides it; a click pins it
//   open until a click outside, Escape, or another click on the button.
// - Keyboard: Enter or Space opens it and moves the focus into the text, so a
//   screen reader reads its name and the text. Escape or Tab closes it and
//   puts the focus back on the button.
// - Touch: a tap opens it, a tap outside closes it.

import * as Popover from '@radix-ui/react-popover';
import { useId, useRef, useState, type KeyboardEvent, type MouseEvent, type PointerEvent, type ReactNode } from 'react';

import type { HelpId } from '../lib/help-texts.ts';
import { useHelpText } from './help-values.ts';
import { InfoIcon } from './icons.tsx';

const OPEN_DELAY_MS = 250;
const CLOSE_DELAY_MS = 150;

type Mode = 'hover' | 'pinned';

interface HelpHintProps {
  readonly id: HelpId;
  /** What the hint explains. The button is named "About: <label>". */
  readonly label: string;
  /** Visible content of a badge-style button. Without it the button is an "i" icon. */
  readonly children?: ReactNode;
  readonly className?: string;
}

const ICON_BUTTON =
  // 24px target, with negative margins so the button takes no more room in
  // the line than the 14px icon.
  'relative -mx-1 -my-1.5 inline-flex size-6 shrink-0 items-center justify-center rounded-full align-middle text-ink-3 hover:text-accent data-[state=open]:text-accent';
const BADGE_BUTTON =
  'inline-flex items-center gap-1 rounded-full border border-line-strong px-2 py-0.5 font-mono text-[11px] text-ink-2 hover:text-ink data-[state=open]:text-ink';

export function HelpHint({ id, label, children, className = '' }: HelpHintProps) {
  const text = useHelpText(id);
  const textId = useId();
  const [mode, setMode] = useState<Mode | null>(null);
  // How the hint was opened, also while it closes: a hover never moves the
  // focus, a click or a key does.
  const openedBy = useRef<Mode>('pinned');
  const openTimer = useRef<number | undefined>(undefined);
  const closeTimer = useRef<number | undefined>(undefined);
  const name = `About: ${label}`;

  function clearTimers() {
    window.clearTimeout(openTimer.current);
    window.clearTimeout(closeTimer.current);
  }

  function handleOpenChange(isOpen: boolean) {
    // Radix asks to close on Escape, a click outside or a focus outside.
    if (!isOpen) {
      clearTimers();
      setMode(null);
    }
  }

  function handleClick(event: MouseEvent<HTMLButtonElement>) {
    // The open state is ours: keep the trigger from toggling it as well.
    event.preventDefault();
    clearTimers();
    if (mode === 'pinned') {
      setMode(null);
      return;
    }
    openedBy.current = mode ?? 'pinned';
    setMode('pinned');
  }

  function handlePointerEnter(event: PointerEvent) {
    if (event.pointerType === 'touch') return;
    window.clearTimeout(closeTimer.current);
    if (mode !== null) return;
    openTimer.current = window.setTimeout(() => {
      openedBy.current = 'hover';
      setMode('hover');
    }, OPEN_DELAY_MS);
  }

  function handlePointerLeave(event: PointerEvent) {
    if (event.pointerType === 'touch') return;
    window.clearTimeout(openTimer.current);
    if (mode !== 'hover') return;
    closeTimer.current = window.setTimeout(() => setMode(null), CLOSE_DELAY_MS);
  }

  function handleOpenAutoFocus(event: Event) {
    if (openedBy.current === 'hover') event.preventDefault();
  }

  function handleCloseAutoFocus(event: Event) {
    // Radix gives the focus back to the button a moment after the close. A
    // hover never took the focus, and when the focus already went somewhere
    // else (another hint, a field), it stays there.
    const active = document.activeElement;
    if (openedBy.current === 'hover' || (active !== null && active !== document.body)) event.preventDefault();
  }

  function handleContentKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    // The text sits at the end of the page. Tab would leave it for the end
    // of the document, so it closes the hint and the focus goes back to the
    // button instead.
    if (event.key === 'Tab') {
      event.preventDefault();
      setMode(null);
    }
  }

  return (
    <Popover.Root open={mode !== null} onOpenChange={handleOpenChange}>
      <Popover.Trigger asChild>
        <button
          type="button"
          aria-label={name}
          aria-describedby={mode === null ? undefined : textId}
          onClick={handleClick}
          onPointerEnter={handlePointerEnter}
          onPointerLeave={handlePointerLeave}
          className={`${children === undefined ? ICON_BUTTON : BADGE_BUTTON} ${className}`}
        >
          {children}
          <InfoIcon className={children === undefined ? 'size-3.5' : 'size-3'} />
        </button>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content
          aria-label={name}
          aria-describedby={textId}
          side="bottom"
          align="center"
          sideOffset={6}
          collisionPadding={8}
          hideWhenDetached
          onOpenAutoFocus={handleOpenAutoFocus}
          onCloseAutoFocus={handleCloseAutoFocus}
          onKeyDown={handleContentKeyDown}
          onPointerEnter={handlePointerEnter}
          onPointerLeave={handlePointerLeave}
          className="z-50 w-max max-w-[min(20rem,calc(100vw-16px))] rounded-md border border-line-strong bg-panel px-3 py-2 text-left font-sans text-xs leading-relaxed font-normal tracking-normal text-ink-2 normal-case shadow-lg outline-none"
        >
          <p id={textId}>{text}</p>
          <Popover.Arrow className="fill-line-strong" />
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
