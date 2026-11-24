import { shortDurationParts } from '../lib/format.ts';

interface ShortDurationProps {
  readonly us: number;
  /** Classes of the number, for example a color. */
  readonly className?: string;
}

/**
 * "4.2 h": the number in the mono font, the space and the unit in the text
 * font around it. With the space inside the mono element the gap is as wide
 * as a digit and reads like two spaces.
 */
export function ShortDuration({ us, className = '' }: ShortDurationProps) {
  const { value, unit } = shortDurationParts(us);
  return (
    <>
      <span className={`font-mono tnum ${className}`}>{value}</span> {unit}
    </>
  );
}
