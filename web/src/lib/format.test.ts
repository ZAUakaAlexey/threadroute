import { formatShortDuration, pluralWord, shortDurationParts, spokenDuration } from './format.ts';

describe('pluralWord', () => {
  it('gives the singular for one and the plural for every other number', () => {
    expect(pluralWord(1, 'operator')).toBe('operator');
    expect(pluralWord(0, 'operator')).toBe('operators');
    expect(pluralWord(2, 'operator')).toBe('operators');
    expect(pluralWord(1, 'company', 'companies')).toBe('company');
    expect(pluralWord(60, 'company', 'companies')).toBe('companies');
  });

  it('keeps the spoken durations right', () => {
    expect(spokenDuration(61_000_000)).toBe('1 minute 1 second');
    expect(spokenDuration(120_000_000)).toBe('2 minutes 0 seconds');
  });
});

describe('short durations', () => {
  it('splits the number from the unit', () => {
    expect(shortDurationParts(41_000_000)).toEqual({ value: '41', unit: 's' });
    expect(shortDurationParts(62 * 60_000_000)).toEqual({ value: '62', unit: 'min' });
    expect(shortDurationParts(4.2 * 3_600_000_000)).toEqual({ value: '4.2', unit: 'h' });
  });

  it('writes one normal space between the number and the unit', () => {
    expect(formatShortDuration(8_000_000)).toBe('8 s');
    expect(formatShortDuration(150_000_000)).toBe('2.5 min');
    expect(formatShortDuration(4.2 * 3_600_000_000)).toBe('4.2 h');
  });
});
