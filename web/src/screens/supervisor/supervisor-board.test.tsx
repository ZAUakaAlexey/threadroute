import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { readFixture } from '../../test/fixtures.ts';
import { renderLive } from '../../test/live-harness.tsx';
import { SupervisorBoard } from './supervisor-screen.tsx';

const ALICE_BOB = 'did:plc:testalice0000000000000000|did:plc:testbob000000000000000000';

/** The rows of the teams list, without the member lists inside them. */
function teamRows(list: HTMLElement): HTMLElement[] {
  return within(list)
    .getAllByRole('listitem')
    .filter((li) => li.parentElement === list);
}

function start() {
  const h = renderLive('supervisor', <SupervisorBoard />);
  h.push(readFixture('server_state_supervisor_snapshot_full.json'));
  return h;
}

describe('supervisor board', () => {
  it('shows the flow totals', () => {
    start();
    // The label sits in the term with its help button; the value is the next element.
    expect(screen.getByText('Messages per second').closest('dt')?.nextElementSibling).toHaveTextContent('3.50');
    expect(screen.getByText('Operators online').closest('dt')?.nextElementSibling).toHaveTextContent('12');
  });

  it('lists operators in a table with kind and load', () => {
    start();
    const table = screen.getByRole('table', { name: 'Operators' });
    const rows = within(table).getAllByRole('row');
    expect(rows).toHaveLength(4);
    expect(within(table).getByText('web-test0001').closest('[role="row"]')).toHaveTextContent('human');
    expect(within(table).getByRole('meter', { name: 'Load of sim-op-0001: 3 of 4' })).toHaveAttribute('aria-valuenow', '3');
    expect(within(table).getByText('sim-op-0001').closest('[role="row"]')).toHaveTextContent(/3\/4(?! \()/);
  });

  it('shows the offers as a part of the load, in brackets and in the label', () => {
    start();
    const table = screen.getByRole('table', { name: 'Operators' });
    const row = within(table).getByText('web-test0001').closest('[role="row"]');
    expect(row).toHaveTextContent('2/2 (1 offered)');
    expect(row).not.toHaveTextContent('+1 offered');
    expect(within(table).getByRole('meter', { name: 'Load of web-test0001: 2 of 2, 1 of them offered' })).toHaveAttribute('aria-valuenow', '2');
  });

  it('filters conversations by state and language', async () => {
    start();
    const table = screen.getByRole('table', { name: 'Conversations' });
    expect(within(table).getAllByRole('row')).toHaveLength(3);
    await userEvent.selectOptions(screen.getByLabelText('Filter by state'), 'waiting');
    expect(within(table).getAllByRole('row')).toHaveLength(2);
    await userEvent.selectOptions(screen.getByLabelText('Filter by language'), 'en');
    expect(within(table).getAllByRole('row')).toHaveLength(2);
    await userEvent.selectOptions(screen.getByLabelText('Filter by state'), 'resolved');
    expect(screen.getByText('No conversation matches the filters.')).toBeInTheDocument();
  });

  it('opens the messages of a conversation in a dialog and stops watching on close', async () => {
    const h = start();
    await userEvent.click(screen.getAllByRole('button', { name: /view messages/i })[0]!);
    expect(h.socket().lastSent()).toMatchObject({ type: 'watch', conversation_id: ALICE_BOB });
    h.push(readFixture('server_messages_snapshot_full.json'));
    const dialog = screen.getByRole('dialog');
    expect(within(dialog).getByText('synthetic customer question one')).toBeInTheDocument();

    h.push(readFixture('server_message_removed_full.json'));
    expect(within(dialog).queryByText('synthetic customer question one')).not.toBeInTheDocument();
    expect(within(dialog).getByText('Message removed: the author deleted it.')).toBeInTheDocument();

    await userEvent.click(within(dialog).getByRole('button', { name: 'Close' }));
    expect(h.socket().lastSent()).toMatchObject({ type: 'unwatch', conversation_id: ALICE_BOB });
  });

  it('says operator and conversation in the singular for one and in the plural otherwise', () => {
    const h = start();
    const list = screen.getByRole('list', { name: 'Dedicated teams' });
    expect(teamRows(list)).toHaveLength(1);
    expect(teamRows(list)[0]).toHaveTextContent('2 operators, 21 active conversations');
    h.push({
      type: 'state',
      seq: 2,
      snapshot: false,
      server_time_us: Date.now() * 1000,
      teams: {
        set: [
          { id: 'did:plc:testalice0000000000000000', members: ['sim-op-0001'], active_load: 1, formed_at_us: Date.now() * 1000 },
          { id: 'did:plc:testbob000000000000000000', members: [], active_load: 0, formed_at_us: Date.now() * 1000 },
        ],
      },
    });
    const [one, none] = teamRows(list);
    expect(one).toHaveTextContent('1 operator, 1 active conversation');
    expect(one).not.toHaveTextContent('operators');
    expect(none).toHaveTextContent('0 operators, 0 active conversations');
    expect(screen.getByText('2 companies')).toBeInTheDocument();
  });

  it('puts the teams list in a focusable region with a name', () => {
    start();
    const region = screen.getByRole('group', { name: 'Dedicated teams, scrollable' });
    expect(region).toHaveAttribute('tabindex', '0');
    expect(within(region).getByRole('list', { name: 'Dedicated teams' })).toBeInTheDocument();
  });

  it('shows a removed team as gone after an update', () => {
    const h = start();
    expect(screen.getByText('1 company')).toBeInTheDocument();
    h.push({ type: 'state', seq: 2, snapshot: false, server_time_us: Date.now() * 1000, teams: { remove: ['did:plc:testalice0000000000000000'] } });
    expect(screen.getByText('No company has a dedicated team right now.')).toBeInTheDocument();
  });

  it('shows the age of a team with one normal space before the unit', () => {
    const h = start();
    // The welcome of the fixture has speed factor 50: 4.2 h of model time
    // are 302.4 s of machine time.
    const formed = (Date.now() - (4.2 * 3600 * 1000) / 50) * 1000;
    h.push({
      type: 'state',
      seq: 2,
      snapshot: false,
      server_time_us: Date.now() * 1000,
      teams: { set: [{ id: 'did:plc:testalice0000000000000000', members: ['sim-op-0001'], active_load: 1, formed_at_us: formed }] },
    });
    const timer = screen.getByRole('timer', { name: /^Team age/ });
    expect(timer.textContent).toBe('4.2 h');
    // Only the number is in the mono font; the space and the unit are not.
    const mono = timer.querySelector('.font-mono');
    expect(mono?.textContent).toBe('4.2');
    expect(timer.closest('span:not([role])')?.textContent).toBe('formed 4.2 h ago');
  });

  it('bounds the member list of a large team and opens the full list on request', async () => {
    const h = start();
    const members = Array.from({ length: 20 }, (_, i) => `sim-op-${(i + 1).toString().padStart(4, '0')}`);
    h.push({
      type: 'state',
      seq: 2,
      snapshot: false,
      server_time_us: Date.now() * 1000,
      teams: { set: [{ id: 'did:plc:testalice0000000000000000', members, active_load: 191, formed_at_us: Date.now() * 1000 }] },
    });
    const list = screen.getByRole('list', { name: 'Dedicated teams' });
    expect(teamRows(list)[0]).toHaveTextContent('20 operators, 191 active conversations');
    const memberList = screen.getByRole('list', { name: 'Members of did:plc:testalice0000000000000000' });
    const more = within(memberList).getByRole('button', { name: '+15 more members of did:plc:testalice0000000000000000' });
    expect(more).toHaveAttribute('aria-expanded', 'false');
    expect(within(memberList).queryByText('sim-op-0006')).not.toBeInTheDocument();

    await userEvent.click(more);
    expect(within(memberList).getByText('sim-op-0020')).toBeInTheDocument();
    const fewer = within(memberList).getByRole('button', { name: /^show fewer/ });
    expect(fewer).toHaveAttribute('aria-expanded', 'true');
    await userEvent.click(fewer);
    expect(within(memberList).queryByText('sim-op-0020')).not.toBeInTheDocument();
  });
});
