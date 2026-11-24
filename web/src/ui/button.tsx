import type { ButtonHTMLAttributes } from 'react';

type Tone = 'primary' | 'secondary' | 'quiet' | 'danger';

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  readonly tone?: Tone;
  readonly size?: 'sm' | 'md';
}

const TONES: Readonly<Record<Tone, string>> = {
  primary: 'bg-accent text-accent-ink border-accent hover:brightness-110',
  secondary: 'bg-panel text-ink border-line-strong hover:bg-sunken',
  quiet: 'bg-transparent text-ink-2 border-transparent hover:bg-sunken hover:text-ink',
  danger: 'bg-panel text-late border-line-strong hover:bg-late-soft',
};

const SIZES = { sm: 'h-8 px-3 text-[13px]', md: 'h-10 px-4 text-sm' } as const;

export function Button({ tone = 'secondary', size = 'md', className = '', type = 'button', ...props }: ButtonProps) {
  return (
    <button
      type={type}
      className={`inline-flex items-center justify-center gap-2 rounded-md border font-medium transition-[background-color,filter] disabled:cursor-not-allowed disabled:opacity-50 ${TONES[tone]} ${SIZES[size]} ${className}`}
      {...props}
    />
  );
}
