import type { ButtonHTMLAttributes } from 'react';

type Tone = 'primary' | 'secondary' | 'quiet' | 'danger';

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  readonly tone?: Tone;
  readonly size?: 'sm' | 'md';
}

// Disabled buttons get their own colours, not a lower opacity: a light
// accent at half opacity on the dark panel still looks like an active
// primary button. Hover acts on enabled buttons only.
const TONES: Readonly<Record<Tone, string>> = {
  primary: 'bg-accent text-accent-ink border-accent enabled:hover:brightness-110 disabled:bg-sunken disabled:text-ink-off disabled:border-line',
  secondary: 'bg-panel text-ink border-line-strong enabled:hover:bg-sunken disabled:text-ink-off disabled:border-line',
  quiet: 'bg-transparent text-ink-2 border-transparent enabled:hover:bg-sunken enabled:hover:text-ink disabled:text-ink-off',
  danger: 'bg-panel text-late border-line-strong enabled:hover:bg-late-soft disabled:text-ink-off disabled:border-line',
};

const SIZES = { sm: 'h-8 px-3 text-[13px]', md: 'h-10 px-4 text-sm' } as const;

export function Button({ tone = 'secondary', size = 'md', className = '', type = 'button', ...props }: ButtonProps) {
  return (
    <button
      type={type}
      className={`inline-flex items-center justify-center gap-2 rounded-md border font-medium transition-[background-color,filter] disabled:cursor-not-allowed ${TONES[tone]} ${SIZES[size]} ${className}`}
      {...props}
    />
  );
}
