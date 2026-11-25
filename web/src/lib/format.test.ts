import {
  formatDuration,
  formatShortDuration,
  isoDuration,
  loadBarWidths,
  pluralWord,
  shortDurationParts,
  spanUs,
  splitLoad,
  spokenDuration,
  spokenLoad,
} from './format.ts';

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

describe('load split', () => {
  it('has no offered part without offers', () => {
    expect(splitLoad(3, 0)).toEqual({ accepted: 3, offered: 0 });
    expect(loadBarWidths(3, 4, 0)).toEqual({ accepted: 75, offered: 0 });
    expect(spokenLoad(3, 4, 0)).toBe('3 of 4');
  });

  it('counts the offers inside the load', () => {
    expect(splitLoad(4, 2)).toEqual({ accepted: 2, offered: 2 });
    expect(loadBarWidths(4, 4, 2)).toEqual({ accepted: 50, offered: 50 });
    expect(spokenLoad(4, 4, 2)).toBe('4 of 4, 2 of them offered');
  });

  it('shows only the offered part when every slot in use is an offer', () => {
    expect(splitLoad(2, 2)).toEqual({ accepted: 0, offered: 2 });
    expect(loadBarWidths(2, 4, 2)).toEqual({ accepted: 0, offered: 50 });
    expect(spokenLoad(2, 4, 2)).toBe('2 of 4, 2 of them offered');
  });

  it('treats more offers than load as the whole load', () => {
    expect(splitLoad(1, 3)).toEqual({ accepted: 0, offered: 1 });
    expect(loadBarWidths(1, 4, 3)).toEqual({ accepted: 0, offered: 25 });
    expect(spokenLoad(1, 4, 3)).toBe('1 of 4, 1 of them offered');
  });

  it('never gives a negative part or more than the whole bar', () => {
    expect(splitLoad(0, 2)).toEqual({ accepted: 0, offered: 0 });
    expect(splitLoad(2, -1)).toEqual({ accepted: 2, offered: 0 });
    expect(loadBarWidths(6, 4, 2)).toEqual({ accepted: 100, offered: 0 });
    expect(loadBarWidths(5, 4, 3)).toEqual({ accepted: 50, offered: 50 });
    expect(loadBarWidths(2, 0, 1)).toEqual({ accepted: 0, offered: 0 });
  });
});

describe('fixed spans', () => {
  it('gives the model time between two source times', () => {
    expect(spanUs(1_000_000, 136_000_000, 1)).toBe(135_000_000);
    expect(spanUs(1_000_000, 28_000_000, 5)).toBe(135_000_000);
    expect(formatDuration(spanUs(1_000_000, 136_000_000, 1))).toBe('2:15');
  });

  it('is never below 0 when the end is before the start', () => {
    expect(spanUs(10_000_000, 4_000_000, 1)).toBe(0);
  });

  it('writes an HTML duration string', () => {
    expect(isoDuration(135_400_000)).toBe('PT135S');
    expect(isoDuration(-5)).toBe('PT0S');
  });
});
