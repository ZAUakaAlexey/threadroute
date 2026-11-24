import type { HelpId } from '../lib/help-texts.ts';
import { useHelpText } from './help-values.ts';

interface ScreenTitleProps {
  readonly eyebrow: string;
  readonly title: string;
  /** The help text that says what the screen shows. */
  readonly description: HelpId;
}

/** The title of a live screen with one or two sentences about it. */
export function ScreenTitle({ eyebrow, title, description }: ScreenTitleProps) {
  const text = useHelpText(description);
  return (
    <div className="flex flex-col gap-1">
      <p className="eyebrow">{eyebrow}</p>
      <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
      <p className="max-w-3xl text-sm leading-relaxed text-ink-2">{text}</p>
    </div>
  );
}
