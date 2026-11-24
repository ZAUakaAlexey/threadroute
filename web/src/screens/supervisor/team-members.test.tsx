import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { TeamMembers } from './team-members.tsx';

const ids = (n: number) => Array.from({ length: n }, (_, i) => `sim-op-${(i + 1).toString().padStart(4, '0')}`);

function memberItems(): string[] {
  const list = screen.getByRole('list', { name: 'Members of ps-team' });
  return within(list)
    .getAllByRole('listitem')
    .filter((li) => li.querySelector('button') === null)
    .map((li) => li.textContent);
}

describe('team members', () => {
  it.each([1, 5, 6])('shows every member of a team of %i, with no button', (n) => {
    render(<TeamMembers team="ps-team" members={ids(n)} />);
    expect(memberItems()).toEqual(ids(n));
    expect(screen.queryByRole('button')).not.toBeInTheDocument();
  });

  it('shows five members and "+N more" for a large team', async () => {
    render(<TeamMembers team="ps-team" members={ids(20)} />);
    expect(memberItems()).toEqual(ids(5));
    const more = screen.getByRole('button', { name: '+15 more members of ps-team' });
    expect(more).toHaveTextContent('+15 more');
    expect(more).toHaveAttribute('aria-expanded', 'false');

    await userEvent.click(more);
    expect(memberItems()).toEqual(ids(20));
    expect(screen.getByRole('button', { name: 'show fewer members of ps-team' })).toHaveAttribute('aria-expanded', 'true');
  });

  it('says "+2 more" for a team of seven', () => {
    render(<TeamMembers team="ps-team" members={ids(7)} />);
    expect(memberItems()).toEqual(ids(5));
    expect(screen.getByRole('button')).toHaveTextContent('+2 more');
  });
});
