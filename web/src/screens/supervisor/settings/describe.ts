// Words for the changes of the stand settings, for the list of changes and
// the marks of the trend strip.

import type { FieldChange, SettingsBlock, SettingsChange } from '../../../protocol/types.ts';
import { boundOf, fieldMeta, formatSetting } from './fields.ts';

/** "Load that forms a team: 20 to 30", or "set to 200" for a first value. */
export function describeField(f: FieldChange, block: SettingsBlock): string {
  const meta = fieldMeta(f.field);
  const bound = meta === undefined ? undefined : boundOf(block.bounds, meta.field);
  const label = meta?.label ?? f.field;
  const next = formatSetting(f.field, f.new, bound);
  if (f.old === undefined) return `${label}: set to ${next}`;
  return `${label}: ${formatSetting(f.field, f.old, bound)} to ${next}`;
}

/** What a change did, in one line, for the marks of the trend strip. */
export function describeChange(c: SettingsChange, block: SettingsBlock): string {
  return c.fields.map((f) => describeField(f, block)).join('; ');
}

/** The clock time, with the date when it is not today. */
export function formatChangeTime(atUs: number): string {
  const at = new Date(atUs / 1000);
  const time = at.toLocaleTimeString('en-GB', { hour12: false });
  const isToday = at.toDateString() === new Date().toDateString();
  return isToday ? time : `${at.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })} ${time}`;
}
