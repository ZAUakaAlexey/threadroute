// @vitest-environment node
// The rules of the settings form, without React: edits over the live set,
// checks from the bounds of the gateway, and the answers.

import { readSettingsFixture } from '../../../test/fixtures.ts';
import type { Result, SettingsBlock } from '../../../protocol/types.ts';
import { INITIAL_FORM, answerText, deriveForm, formReducer, retrySecondsLeft, type FormAction, type FormState } from './form.ts';
import { formatSetting, parseInput, toInput } from './fields.ts';

const welcome = readSettingsFixture('server_welcome_supervisor_settings_full_may_change.json') as { settings: SettingsBlock };
const block: SettingsBlock = welcome.settings;
const firstStart = (readSettingsFixture('server_settings_first_start_demo.json') as { settings: SettingsBlock }).settings;

function run(...actions: FormAction[]): FormState {
  return actions.reduce(formReducer, INITIAL_FORM);
}

function edit(field: Parameters<typeof toInput>[0], text: string, version = block.version, isClean = false): FormAction {
  return { type: 'edit', field, text, version, isClean };
}

function field(state: FormState, name: string, b: SettingsBlock = block) {
  const f = deriveForm(state, b).fields.find((x) => x.meta.field === name);
  if (f === undefined) throw new Error(`no field ${name}`);
  return f;
}

function result(partial: Partial<Result>): Result {
  return { type: 'result', request_id: 'r1', outcome: 'rejected', ...partial };
}

describe('settings form, values', () => {
  it('shows durations in their unit, shares as a percentage, and sends microseconds and fractions', () => {
    expect(field(INITIAL_FORM, 'teams.release_after_us').text).toBe('15');
    expect(field(INITIAL_FORM, 'sim.handling_median_us').text).toBe('45');
    expect(field(INITIAL_FORM, 'teams.max_share').text).toBe('50');
    expect(field(INITIAL_FORM, 'sim.missed_offer_share').text).toBe('3');
    const s = run(edit('teams.release_after_us', '20'), edit('teams.max_share', '30'), edit('sim.handling_median_us', '7.5'));
    const view = deriveForm(s, block);
    expect(view.next?.teams).toMatchObject({ release_after_us: 1_200_000_000, max_share: 0.3 });
    expect(view.next?.sim?.handling_median_us).toBe(7_500_000);
    expect(view.editedCount).toBe(3);
    expect(view.canApply).toBe(true);
  });

  it('formats values for people', () => {
    const bound = block.bounds.teams.release_after_us;
    expect(formatSetting('teams.release_after_us', 5_400_000_000, bound)).toBe('1 h 30 min');
    expect(formatSetting('sim.handling_median_us', 45_000_000, block.bounds.sim.handling_median_us)).toBe('45 s');
    expect(formatSetting('teams.max_share', 0.05, block.bounds.teams.max_share)).toBe('5%');
  });

  it('checks each value against its bound from the gateway', () => {
    const b = block.bounds.teams.form_load;
    expect(parseInput('teams.form_load', b, '501')).toEqual({ ok: false, error: 'Use a value from 2 to 500.' });
    expect(parseInput('teams.form_load', b, '2.5')).toEqual({ ok: false, error: 'Enter a whole number.' });
    expect(parseInput('teams.form_load', b, '')).toMatchObject({ ok: false });
    expect(parseInput('teams.form_load', b, '-3')).toMatchObject({ ok: false });
    expect(parseInput('teams.max_share', block.bounds.teams.max_share, '4')).toEqual({ ok: false, error: 'Use a value from 5% to 100%.' });
    // A changed bound in the block changes the check: nothing is fixed in the console.
    expect(parseInput('teams.form_load', { ...b, max: 40 }, '41')).toMatchObject({ ok: false });
  });

  it('checks the rules between fields, with the values of the form', () => {
    const below = run(edit('teams.release_load', '30'));
    expect(field(below, 'teams.release_load').error).toBe('Must be below the load that forms a team (30).');
    expect(deriveForm(below, block).canApply).toBe(false);
    // Raising the form load in the same set makes it valid.
    const fixed = formReducer(below, edit('teams.form_load', '31'));
    expect(field(fixed, 'teams.release_load').error).toBeNull();
    expect(deriveForm(fixed, block).canApply).toBe(true);

    const notAbove = run(edit('teams.min_size', '21'));
    expect(field(notAbove, 'teams.min_size').error).toBe('Must not be above the max team size (20).');
  });

  it('counts a field typed back to its value as not changed', () => {
    const s = run(edit('teams.form_load', '31'), edit('teams.form_load', '30'));
    expect(deriveForm(s, block)).toMatchObject({ editedCount: 0, canApply: false });
  });

  it('has no sim fields before a simulator reported, and sends no sim part', () => {
    const view = deriveForm(run(edit('teams.max_share', '30', 0)), firstStart);
    expect(view.hasSim).toBe(false);
    expect(view.fields.some((f) => f.meta.field.startsWith('sim.'))).toBe(false);
    expect(view.next).toEqual({ teams: { ...firstStart.current.teams, max_share: 0.3 } });
  });

  it('fills the defaults on reset and applies them as one change', () => {
    const view = deriveForm(run({ type: 'reset_to_defaults', block }), block);
    expect(view.next).toEqual(block.defaults);
    expect(view.editedCount).toBe(2);
    expect(view.canApply).toBe(true);
  });

  it('sends the exact defaults, also when their text is rounded', () => {
    const odd: SettingsBlock = { ...block, defaults: { ...block.defaults, teams: { ...block.defaults.teams, release_after_us: 100_000_000 } } };
    const s = run({ type: 'reset_to_defaults', block: odd });
    expect(field(s, 'teams.release_after_us', odd).text).toBe('1.667');
    expect(deriveForm(s, odd).next).toEqual(odd.defaults);
    // A field typed by the user takes the typed value again.
    const typed = formReducer(s, edit('teams.release_after_us', '2'));
    expect(deriveForm(typed, odd).next?.teams.release_after_us).toBe(120_000_000);
  });

  it('is read-only without the right', () => {
    const view = deriveForm(run(edit('teams.form_load', '31')), { ...block, may_change: false });
    expect(view.canApply).toBe(false);
    expect(view.canReset).toBe(false);
  });
});

describe('settings form, a change by someone else', () => {
  const newer: SettingsBlock = { ...block, version: 4, current: { ...block.current, teams: { ...block.current.teams, max_size: 25, form_load: 40 } } };

  it('follows the live values in fields that were not edited', () => {
    expect(field(INITIAL_FORM, 'teams.max_size', newer).text).toBe('25');
    expect(deriveForm(INITIAL_FORM, newer).isOutdated).toBe(false);
  });

  it('keeps the edits, marks them outdated and blocks Apply until the user decides', () => {
    const s = run(edit('teams.min_size', '3'));
    const view = deriveForm(s, newer);
    expect(view.isOutdated).toBe(true);
    expect(view.canApply).toBe(false);
    expect(field(s, 'teams.min_size', newer).text).toBe('3');

    const kept = formReducer(s, { type: 'keep_edits', version: newer.version });
    const next = deriveForm(kept, newer);
    expect(next.isOutdated).toBe(false);
    expect(next.canApply).toBe(true);
    // The other person's values stay; only the own edit is laid over them.
    expect(next.next?.teams).toMatchObject({ min_size: 3, max_size: 25, form_load: 40 });

    expect(formReducer(s, { type: 'discard' }).drafts).toEqual({});
  });

  it('starts a new base when the edit begins on a clean form, after an edit typed back', () => {
    const typedBack = run(edit('teams.form_load', '31'), edit('teams.form_load', '30'));
    expect(typedBack.baseVersion).toBe(3);
    // Someone else changes the set; the next keystroke starts on version 4.
    const next = formReducer(typedBack, edit('teams.min_size', '3', newer.version, true));
    expect(deriveForm(next, newer).isOutdated).toBe(false);
  });

  it('does not show the outdated state while the own change is on its way', () => {
    const s = run(edit('teams.form_load', '40'), { type: 'submit' });
    expect(deriveForm(s, newer).isOutdated).toBe(false);
  });
});

describe('settings form, answers', () => {
  const sent = run(edit('teams.form_load', '31'), { type: 'submit' });

  it('done: clears the edits and says which version runs', () => {
    const s = formReducer(sent, { type: 'answered', result: result({ outcome: 'done' }), version: 4, nowMs: 0 });
    expect(s.drafts).toEqual({});
    expect(s.isPending).toBe(false);
    expect(answerText(s.answer ?? { kind: 'unchanged' }, 0)).toBe('Applied. The stand now runs version 4 of the settings.');
  });

  it('conflict: keeps the edits on their old base', () => {
    const s = formReducer(sent, { type: 'answered', result: result({ code: 'settings_conflict' }), version: 4, nowMs: 0 });
    expect(s.drafts).toEqual({ 'teams.form_load': '31' });
    expect(s.answer).toEqual({ kind: 'conflict' });
    const newer = { ...block, version: 4 };
    expect(deriveForm(s, newer).isOutdated).toBe(true);
  });

  it('too soon: waits for retry_after_ms, then allows Apply again', () => {
    const s = formReducer(sent, { type: 'answered', result: result({ code: 'settings_too_soon', retry_after_ms: 6800 }), version: 3, nowMs: 1000 });
    expect(s.retryAtMs).toBe(7800);
    expect(s.retryWaitMs).toBe(6800);
    expect(deriveForm(s, block)).toMatchObject({ isWaiting: true, canApply: false });
    expect(answerText({ kind: 'too_soon' }, 7)).toContain('in 7 seconds');
    const ready = formReducer(s, { type: 'retry_ready' });
    expect(deriveForm(ready, block)).toMatchObject({ isWaiting: false, canApply: true });
    expect(ready.answer).toBeNull();
    expect(ready.retryWaitMs).toBeNull();
  });

  it.each([
    // [wait asked for, time since the answer, seconds shown]
    [10_000, 0, 10],
    // A clock that ticked a little before the answer sees more than the wait.
    [10_000, -200, 10],
    [9_800, 0, 10],
    [9_800, 800, 9],
    [9_800, 8_800, 1],
    [9_800, 9_799, 1],
    [9_800, 9_800, 0],
    [9_800, 12_000, 0],
    [300, -100, 1],
  ])('too soon: a wait of %i ms, %i ms later, shows %i seconds', (waitMs, sinceMs, seconds) => {
    expect(retrySecondsLeft(1_000 + waitMs, waitMs, 1_000 + sinceMs)).toBe(seconds);
  });

  it('invalid: shows the reasons of the gateway next to their fields until the next edit', () => {
    const s = formReducer(sent, {
      type: 'answered',
      result: result({ code: 'invalid_request', fields: [{ field: 'teams.form_load', reason: '31 is not allowed here' }, { field: 'sim', reason: 'is missing' }] }),
      version: 3,
      nowMs: 0,
    });
    expect(field(s, 'teams.form_load').error).toBe('31 is not allowed here');
    expect(deriveForm(s, block)).toMatchObject({ simError: 'is missing', canApply: false });
    expect(field(formReducer(s, edit('teams.form_load', '32')), 'teams.form_load').error).toBeNull();
  });

  it.each([
    ['not_allowed', 'may not change settings'],
    ['settings_off', 'does not offer settings'],
    ['unavailable', 'router is busy'],
    ['connection_lost', 'connection was lost'],
  ])('%s: says what happened', (code, text) => {
    const s = formReducer(sent, { type: 'answered', result: result({ code }), version: 3, nowMs: 0 });
    expect(answerText(s.answer ?? { kind: 'unchanged' }, 0)).toContain(text);
    expect(s.drafts).toEqual({ 'teams.form_load': '31' });
  });
});
