import { pluralWord, spokenDuration } from './format.ts';

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
