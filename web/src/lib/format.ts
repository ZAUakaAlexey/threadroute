// Number and time formats for the screens. Durations are shown as model
// time (machine time * speed factor, decision 0013).

const US_PER_SECOND = 1_000_000;

/** "0:07", "12:30", "1:02:03", "3d 04h". Negative values get a minus sign. */
export function formatDuration(us: number): string {
  const sign = us < 0 ? '-' : '';
  const totalSeconds = Math.floor(Math.abs(us) / US_PER_SECOND);
  const days = Math.floor(totalSeconds / 86_400);
  const hours = Math.floor((totalSeconds % 86_400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (days > 0) return `${sign}${days}d ${pad(hours)}h`;
  if (hours > 0) return `${sign}${hours}:${pad(minutes)}:${pad(seconds)}`;
  return `${sign}${minutes}:${pad(seconds)}`;
}

/** Words for screen readers: "1 minute 5 seconds". */
export function spokenDuration(us: number): string {
  const totalSeconds = Math.floor(Math.abs(us) / US_PER_SECOND);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts = [
    hours > 0 ? plural(hours, 'hour') : '',
    minutes > 0 ? plural(minutes, 'minute') : '',
    hours === 0 ? plural(seconds, 'second') : '',
  ].filter((p) => p !== '');
  return parts.join(' ');
}

export interface DurationParts {
  readonly value: string;
  readonly unit: 's' | 'min' | 'h';
}

/**
 * The number and the unit of a short duration, apart. The screens show the
 * number in the mono font and the unit in the text font: a space inside a
 * mono element is as wide as a digit and looks like two spaces.
 */
export function shortDurationParts(us: number): DurationParts {
  const seconds = us / US_PER_SECOND;
  if (seconds < 90) return { value: Math.round(seconds).toString(), unit: 's' };
  if (seconds < 5400) return { value: trim(seconds / 60), unit: 'min' };
  return { value: trim(seconds / 3600), unit: 'h' };
}

/** Short duration for totals: "8 s", "41 s", "2.5 min", "1.2 h". */
export function formatShortDuration(us: number): string {
  const { value, unit } = shortDurationParts(us);
  return `${value} ${unit}`;
}

export function formatRate(perSecond: number): string {
  return perSecond >= 100 ? Math.round(perSecond).toString() : perSecond.toFixed(perSecond >= 10 ? 1 : 2);
}

export function formatCount(n: number): string {
  return new Intl.NumberFormat('en').format(n);
}

/** Clock time of a source time, "14:05:09", in the browser time zone. */
export function formatClock(sourceUs: number): string {
  return new Date(sourceUs / 1000).toLocaleTimeString('en-GB', { hour12: false });
}

export function percent(part: number, whole: number): number {
  return whole <= 0 ? 0 : Math.min(100, Math.round((part / whole) * 100));
}

function pad(n: number): string {
  return n.toString().padStart(2, '0');
}

/** The word in the singular for one and in the plural otherwise: "operator", "operators". */
export function pluralWord(n: number, word: string, pluralForm = `${word}s`): string {
  return n === 1 ? word : pluralForm;
}

function plural(n: number, word: string): string {
  return `${n} ${pluralWord(n, word)}`;
}

function trim(n: number): string {
  return n >= 10 ? Math.round(n).toString() : n.toFixed(1).replace(/\.0$/, '');
}

/** Open conversations of every state. */
export function totalOpen(counts: { readonly open: number; readonly waiting: number; readonly offered: number; readonly assigned: number; readonly resolved: number }): number {
  return counts.open + counts.waiting + counts.offered + counts.assigned + counts.resolved;
}
