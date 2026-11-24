// The settings panel and the trend strip on the supervisor board, against
// an in-memory gateway connection: with an older gateway, read only, and
// a change with each kind of answer.

import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { readFixture, readSettingsFixture } from '../../test/fixtures.ts';
import { renderLive, type Harness } from '../../test/live-harness.tsx';
import { SupervisorBoard } from './supervisor-screen.tsx';

type Json = Record<string, unknown>;

function welcome(file: string): Json {
  return { ...(readSettingsFixture(file) as Json), server_time_us: Date.now() * 1000 };
}

function start(file: string): Harness {
  const h = renderLive('supervisor', <SupervisorBoard />, welcome(file));
  h.push(readFixture('server_state_supervisor_snapshot_full.json'));
  return h;
}

async function openPanel() {
  await userEvent.click(screen.getByRole('button', { name: 'Stand settings' }));
}

function settingsOf(file: string): Json {
  return (readSettingsFixture(file) as { settings: Json }).settings;
}

/** The block of the full example with other values and a new version. */
function newerBlock(version: number, teams: Json): Json {
  const block = settingsOf('server_welcome_supervisor_settings_full_may_change.json') as { current: { teams: Json } } & Json;
  return { ...block, version, current: { ...block.current, teams: { ...block.current.teams, ...teams } } };
}

function sentChanges(h: Harness): Json[] {
  return h.socket().sent.filter((m) => (m as Json).type === 'change_settings') as Json[];
}

describe('an older gateway without stand settings', () => {
  it('shows no settings panel and no trend strip, and never sends change_settings', async () => {
    const h = renderLive('supervisor', <SupervisorBoard />);
    h.push(readFixture('server_state_supervisor_snapshot_full.json'));
    expect(screen.getByText('Messages per second')).toBeInTheDocument();
    expect(screen.queryByRole('region', { name: 'Last hour' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Stand settings' })).not.toBeInTheDocument();
    // Messages that only a newer gateway sends change nothing.
    h.push(readSettingsFixture('server_settings_update_demo.json'));
    h.push(readSettingsFixture('server_trend_demo.json'));
    expect(screen.queryByRole('region', { name: 'Last hour' })).not.toBeInTheDocument();
    expect(screen.queryByRole('region', { name: 'Stand settings' })).not.toBeInTheDocument();

    const result = await h.session.changeSettings(0, { teams: { form_load: 20, release_load: 8, release_after_us: 900_000_000, min_size: 2, max_size: 20, max_share: 0.5 } });
    expect(result).toMatchObject({ outcome: 'rejected', code: 'not_allowed' });
    expect(sentChanges(h)).toEqual([]);
    expect(h.socket().closedWith).toBeNull();
  });
});

describe('read only, at the demo level without the right', () => {
  it('shows the values, the sentence about access and no "by" label', async () => {
    start('server_welcome_supervisor_settings_demo.json');
    await openPanel();
    const panel = screen.getByRole('region', { name: 'Stand settings' });
    expect(within(panel).queryAllByRole('textbox')).toEqual([]);
    expect(within(panel).queryByRole('button', { name: 'Apply' })).not.toBeInTheDocument();
    expect(panel).toHaveTextContent('Changing them needs a site account with access');
    expect(within(panel).getByRole('group', { name: 'Dedicated teams' })).toHaveTextContent('Load that forms a team');
    expect(within(panel).getByRole('list', { name: 'Last changes of the stand settings' })).toHaveTextContent('Load that forms a team: 20 to 30');
    expect(panel).not.toHaveTextContent(/\bby\b/);
  });

  it('updates the values live when someone else changes them', async () => {
    const h = start('server_welcome_supervisor_settings_demo.json');
    await openPanel();
    h.push(readSettingsFixture('server_settings_update_demo.json'));
    expect(screen.getByRole('region', { name: 'Stand settings' })).toHaveTextContent('version 4');
  });

  it('says "not available" for the sim part before a simulator reported', async () => {
    const h = start('server_welcome_supervisor_settings_demo.json');
    h.push(readSettingsFixture('server_settings_first_start_demo.json'));
    await openPanel();
    expect(screen.getByRole('group', { name: 'Simulated operators' })).toHaveTextContent('Not available');
  });
});

describe('a change, with the right', () => {
  async function editFormLoad(h: Harness, text: string) {
    await openPanel();
    const input = screen.getByRole('textbox', { name: 'Load that forms a team' });
    await userEvent.clear(input);
    await userEvent.type(input, text);
    return { input, apply: screen.getByRole('button', { name: 'Apply' }), h };
  }

  it('sends the whole set with its base version and shows the applied set', async () => {
    const h = start('server_welcome_supervisor_settings_full_may_change.json');
    const { apply, input } = await editFormLoad(h, '31');
    expect(apply).toBeEnabled();
    await userEvent.click(apply);
    const sent = sentChanges(h).at(-1) as { base_version: number; settings: { teams: Json; sim?: Json }; request_id: string };
    expect(sent.base_version).toBe(3);
    expect(sent.settings.teams.form_load).toBe(31);
    expect(sent.settings.sim?.capacity).toBe(6);
    h.push({ type: 'settings', settings: newerBlock(4, { form_load: 31 }) });
    h.push({ type: 'result', request_id: sent.request_id, outcome: 'done' });
    expect(await screen.findByText('Applied. The stand now runs version 4 of the settings.')).toBeInTheDocument();
    expect(input).toHaveValue('31');
    expect(screen.queryByRole('alert', { name: /someone else/i })).not.toBeInTheDocument();
    expect(screen.queryByText(/Someone else changed/)).not.toBeInTheDocument();
    expect(apply).toBeDisabled();
  });

  it('refuses an invalid value on the client and names the rule', async () => {
    const h = start('server_welcome_supervisor_settings_full_may_change.json');
    const { apply, input } = await editFormLoad(h, '600');
    expect(input).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Use a value from 2 to 500.')).toBeInTheDocument();
    expect(apply).toBeDisabled();
  });

  it('keeps the edits after a conflict and lets the user decide', async () => {
    const h = start('server_welcome_supervisor_settings_full_may_change.json');
    const { apply, input } = await editFormLoad(h, '31');
    await userEvent.click(apply);
    const sent = sentChanges(h).at(-1) as { request_id: string };
    h.push({ type: 'settings', settings: newerBlock(4, { max_size: 25 }) });
    h.push({ type: 'result', request_id: sent.request_id, outcome: 'rejected', code: 'settings_conflict', message: 'base version' });
    expect(await screen.findByText(/Someone else changed the settings first/)).toBeInTheDocument();
    expect(screen.getByText(/now version 4/)).toBeInTheDocument();
    expect(input).toHaveValue('31');
    expect(screen.getByRole('textbox', { name: 'Team size, max' })).toHaveValue('25');
    expect(apply).toBeDisabled();
    await userEvent.click(screen.getByRole('button', { name: 'Keep my edits' }));
    expect(apply).toBeEnabled();
    await userEvent.click(apply);
    const again = sentChanges(h).at(-1) as { base_version: number; settings: { teams: Json } };
    expect(again.base_version).toBe(4);
    expect(again.settings.teams).toMatchObject({ form_load: 31, max_size: 25 });
  });

  it('marks own edits as outdated when another change arrives, without overwriting them', async () => {
    const h = start('server_welcome_supervisor_settings_full_may_change.json');
    const { input } = await editFormLoad(h, '31');
    h.push({ type: 'settings', settings: newerBlock(4, { form_load: 40 }) });
    expect(screen.getByText(/now version 4/)).toBeInTheDocument();
    expect(input).toHaveValue('31');
    await userEvent.click(screen.getByRole('button', { name: 'Discard my edits' }));
    expect(input).toHaveValue('40');
  });

  it('waits after "too soon" and allows Apply again after retry_after_ms', async () => {
    const h = start('server_welcome_supervisor_settings_full_may_change.json');
    const { apply } = await editFormLoad(h, '31');
    await userEvent.click(apply);
    const sent = sentChanges(h).at(-1) as { request_id: string };
    h.push({ type: 'result', request_id: sent.request_id, outcome: 'rejected', code: 'settings_too_soon', message: 'one change per 10 seconds', retry_after_ms: 300 });
    expect(await screen.findByText(/You can apply again in 1 second/)).toBeInTheDocument();
    expect(apply).toBeDisabled();
    await waitFor(() => expect(apply).toBeEnabled());
  });

  describe('the "too soon" countdown, with fake timers', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });
    afterEach(() => {
      vi.useRealTimers();
    });

    // fireEvent, not userEvent: the async wrapper of userEvent waits on a
    // real timer, which never fires while the timers are fake.
    async function tooSoon(retryAfterMs: number) {
      const h = start('server_welcome_supervisor_settings_full_may_change.json');
      fireEvent.click(screen.getByRole('button', { name: 'Stand settings' }));
      fireEvent.change(screen.getByRole('textbox', { name: 'Load that forms a team' }), { target: { value: '31' } });
      const apply = screen.getByRole('button', { name: 'Apply' });
      fireEvent.click(apply);
      const sent = sentChanges(h).at(-1) as { request_id: string };
      // The answer comes between two ticks of the shared clock.
      act(() => {
        vi.advanceTimersByTime(100);
      });
      h.push({ type: 'result', request_id: sent.request_id, outcome: 'rejected', code: 'settings_too_soon', message: 'one change per 10 seconds', retry_after_ms: retryAfterMs });
      // The panel handles the answer after the promise of the request settles.
      await act(() => Promise.resolve());
      return apply;
    }

    function answer(): HTMLElement {
      return within(screen.getByRole('region', { name: 'Stand settings' })).getByRole('status');
    }

    function shownSeconds(): number | null {
      const match = /in (\d+) seconds?\./.exec(answer().textContent);
      return match === null ? null : Number(match[1]);
    }

    function advance(ms: number) {
      act(() => {
        vi.advanceTimersByTime(ms);
      });
    }

    it('starts at the limit, never above it, and ends at 1 before Apply comes back', async () => {
      const apply = await tooSoon(10_000);
      expect(shownSeconds()).toBe(10);
      const seen: number[] = [];
      for (let t = 0; t < 9_750; t += 250) {
        advance(250);
        const s = shownSeconds();
        if (s !== null) seen.push(s);
      }
      expect(Math.max(...seen)).toBeLessThanOrEqual(10);
      expect(seen.at(-1)).toBe(1);
      expect(apply).toBeDisabled();
      advance(300);
      expect(apply).toBeEnabled();
      expect(answer()).toBeEmptyDOMElement();
    });

    it('shows 10 for a wait of 9,800 ms', async () => {
      const apply = await tooSoon(9_800);
      expect(shownSeconds()).toBe(10);
      // The shared clock ticks every 250 ms, so the text may be that much late.
      advance(9_300);
      expect(shownSeconds()).toBe(1);
      expect(apply).toBeDisabled();
      advance(600);
      expect(apply).toBeEnabled();
    });
  });

  it('shows the reasons of the gateway next to their fields', async () => {
    const h = start('server_welcome_supervisor_settings_full_may_change.json');
    const { apply } = await editFormLoad(h, '31');
    await userEvent.click(apply);
    const sent = sentChanges(h).at(-1) as { request_id: string };
    h.push({ ...(readSettingsFixture('server_result_settings_invalid.json') as Json), request_id: sent.request_id });
    expect(await screen.findByText('25 is not between 1 and 20')).toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: 'Capacity of an operator' })).toHaveAttribute('aria-invalid', 'true');
  });

  it('shows "by" at the full level when the server sends it', async () => {
    start('server_welcome_supervisor_settings_full_may_change.json');
    await openPanel();
    expect(screen.getByRole('list', { name: 'Last changes of the stand settings' })).toHaveTextContent('by guest');
  });

  it('never shows "by" at the demo level, also when the server sends it to a connection with the right', async () => {
    const h = start('server_welcome_supervisor_settings_demo.json');
    h.push(readSettingsFixture('server_settings_update_may_change_demo.json'));
    await openPanel();
    expect(screen.getByRole('textbox', { name: 'Load that forms a team' })).toBeInTheDocument();
    expect(screen.getByRole('list', { name: 'Last changes of the stand settings' })).not.toHaveTextContent(/\bby\b/);
  });
});

describe('trend strip', () => {
  it('shows the current values and a numbered mark for each change in the hour', () => {
    start('server_welcome_supervisor_settings_full_may_change.json');
    const strip = screen.getByRole('region', { name: 'Last hour' });
    expect(within(strip).getByRole('img', { name: /^Waiting over the last hour: now 134/ })).toBeInTheDocument();
    const marks = within(strip).getByRole('list', { name: 'Settings changes in this hour' });
    expect(within(marks).getAllByRole('listitem')).toHaveLength(3);
    expect(marks).toHaveTextContent('Load that forms a team: 20 to 30');
  });

  it('numbers changes made in the same microsecond in the order of their versions', () => {
    const h = start('server_welcome_supervisor_settings_full_may_change.json');
    const block = settingsOf('server_welcome_supervisor_settings_full_may_change.json') as { changes: Json[] } & Json;
    const strip = screen.getByRole('region', { name: 'Last hour' });
    const newest = (block.changes[0] as { changed_at_us: number }).changed_at_us;
    const same = (version: number, from: number, to: number): Json => ({ changed_at_us: newest + 1, version, fields: [{ field: 'teams.max_size', old: from, new: to }] });
    // Newest first, as the gateway sends them.
    h.push({ type: 'settings', settings: { ...block, version: 5, changes: [same(5, 19, 18), same(4, 20, 19), ...block.changes] } });
    const items = within(within(strip).getByRole('list', { name: 'Settings changes in this hour' })).getAllByRole('listitem');
    expect(items.at(-2)).toHaveTextContent('Team size, max: 20 to 19');
    expect(items.at(-1)).toHaveTextContent('Team size, max: 19 to 18');
  });

  it('adds the points of trend messages', () => {
    const h = start('server_welcome_supervisor_settings_full_may_change.json');
    h.push(readSettingsFixture('server_trend_demo.json'));
    expect(screen.getByRole('img', { name: /^Waiting over the last hour: now 141/ })).toBeInTheDocument();
  });
});
