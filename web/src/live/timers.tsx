// Timers that tick in the browser from server times (decision 0013). They
// read the shared clock tick and show model time. While the data is stale
// they stand still. They have role "timer", which screen readers do not
// announce on every change.

import { formatDuration, isoDuration, spanUs, spokenDuration } from '../lib/format.ts';
import { ShortDuration } from '../ui/short-duration.tsx';
import { useElapsedUs, useLive, useRemainingUs } from './hooks.ts';
import { selectSpeedFactor } from './selectors.ts';

type TimerTone = 'neutral' | 'wait' | 'late' | 'done' | 'accent';

const TONES: Readonly<Record<TimerTone, string>> = {
  neutral: 'text-ink',
  wait: 'text-wait',
  late: 'text-late',
  done: 'text-done',
  accent: 'text-accent',
};

interface TimerProps {
  readonly label: string;
  readonly className?: string;
}

interface ElapsedProps extends TimerProps {
  readonly sinceUs: number;
  /** "clock" shows 1:05, "short" shows 65 s or 12 min. */
  readonly format?: 'clock' | 'short';
}

export function Elapsed({ sinceUs, label, className = '', format = 'clock' }: ElapsedProps) {
  const elapsed = useElapsedUs(sinceUs);
  const spoken = `${label}: ${spokenDuration(elapsed)}`;
  if (format === 'short') {
    // Only the number is mono, so the space before the unit has its normal width.
    return (
      <span role="timer" aria-label={spoken} className={`whitespace-nowrap ${className}`}>
        <ShortDuration us={elapsed} />
      </span>
    );
  }
  return (
    <span role="timer" aria-label={spoken} className={`font-mono tnum ${className}`}>
      {formatDuration(elapsed)}
    </span>
  );
}

interface FixedDurationProps {
  readonly fromUs: number;
  readonly toUs: number;
  readonly className?: string;
}

/**
 * A fixed duration between two server times, in model time, never below 0.
 * It does not tick, so it is a time element and not a timer.
 */
export function FixedDuration({ fromUs, toUs, className = '' }: FixedDurationProps) {
  const speed = useLive(selectSpeedFactor);
  const us = spanUs(fromUs, toUs, speed);
  return (
    <time dateTime={isoDuration(us)} className={`font-mono tnum ${className}`}>
      {formatDuration(us)}
    </time>
  );
}

interface CountdownProps extends TimerProps {
  readonly deadlineUs: number;
  /** Below this much model time left, the timer turns amber. */
  readonly warnUs?: number;
}

export function Countdown({ deadlineUs, label, warnUs = 30_000_000, className = '' }: CountdownProps) {
  const left = useRemainingUs(deadlineUs);
  const tone: TimerTone = left <= 0 ? 'late' : left <= warnUs ? 'wait' : 'neutral';
  const spoken = left <= 0 ? `${label}: passed` : `${label}: ${spokenDuration(left)} left`;
  return (
    <span role="timer" aria-label={spoken} className={`font-mono tnum ${TONES[tone]} ${className}`}>
      {formatDuration(Math.max(0, left))}
    </span>
  );
}
