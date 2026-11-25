import { act, screen } from '@testing-library/react';

import type { OperatorConversation } from '../../protocol/types.ts';
import { readFixture } from '../../test/fixtures.ts';
import { nowUs, renderLive } from '../../test/live-harness.tsx';
import { WaitTimesBlocks, WaitTimesInline } from './wait-times.tsx';

const BASE = {
  id: 'ps-k2m3n4p5q6r7',
  company: 'ps-abcdefgh2345',
  customer: 'ps-zyxwvuts7654',
  lang: 'en',
  session: 1,
  offer_id: 'of-101',
  first_response_missed: false,
  replied: false,
  priority: 0,
  customer_messages: 2,
  company_messages: 0,
} as const;

/** A held conversation without accepted_at_us: an offer, or a row from an old gateway. */
function notAccepted(): OperatorConversation {
  return { ...BASE, state: 'offered', accept_by_us: nowUs(15), queued_at_us: nowUs(-10) };
}

/** An accepted conversation: 2:15 in the queue, 5:00 in work. */
function conversation(extra: Partial<OperatorConversation> = {}): OperatorConversation {
  return { ...BASE, state: 'assigned', accepted_at_us: nowUs(-300), queued_at_us: nowUs(-435), ...extra };
}

/** The term of a header block and the value next to it. */
function blockValue(term: string): Element | null | undefined {
  return screen.getByText(term, { selector: 'dt' }).nextElementSibling;
}

describe('wait times of a held conversation', () => {
  // Fake time: the clock and the server times in the data do not drift apart during a test.
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('shows a fixed Waited and a running In work after the accept', () => {
    renderLive(
      'operator',
      <dl>
        <WaitTimesBlocks conversation={conversation()} />
      </dl>,
    );
    expect(blockValue('Waited')).toHaveTextContent(/^2:15$/);
    expect(blockValue('Waited')?.querySelector('time')).toHaveAttribute('datetime', 'PT135S');
    const inWork = screen.getByRole('timer', { name: /^time in work: 5 minutes/i });
    expect(inWork).toHaveTextContent(/^5:00$/);
    expect(screen.queryByText('Waiting', { selector: 'dt' })).not.toBeInTheDocument();
    expect(screen.queryByRole('timer', { name: /waiting time/i })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'About: In work' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'About: Waited' })).toBeInTheDocument();

    act(() => {
      vi.advanceTimersByTime(10_000);
    });
    expect(blockValue('Waited')).toHaveTextContent(/^2:15$/);
    expect(inWork).toHaveTextContent(/^5:10$/);
  });

  it('keeps the running Waiting clock without accepted_at_us', () => {
    renderLive(
      'operator',
      <dl>
        <WaitTimesBlocks conversation={notAccepted()} />
      </dl>,
    );
    expect(screen.getByRole('timer', { name: /^waiting time: 10 seconds$/i })).toHaveTextContent(/^0:10$/);
    expect(screen.queryByText('Waited', { selector: 'dt' })).not.toBeInTheDocument();
    expect(screen.queryByRole('timer', { name: /time in work/i })).not.toBeInTheDocument();
  });

  it('shows Waited as 0:00 when the accept is before the queue time', () => {
    renderLive(
      'operator',
      <dl>
        <WaitTimesBlocks conversation={conversation({ queued_at_us: nowUs(-60), accepted_at_us: nowUs(-90) })} />
      </dl>,
    );
    expect(blockValue('Waited')).toHaveTextContent(/^0:00$/);
    expect(screen.getByRole('timer', { name: /time in work/i })).toHaveTextContent(/^1:30$/);
  });

  it('counts Waited in model time', () => {
    const welcome = { ...(readFixture('server_welcome_operator_demo.json') as object), server_time_us: nowUs(), speed_factor: 5 };
    renderLive(
      'operator',
      <dl>
        <WaitTimesBlocks conversation={conversation({ queued_at_us: nowUs(-60), accepted_at_us: nowUs(-33) })} />
      </dl>,
      welcome,
    );
    expect(blockValue('Waited')).toHaveTextContent(/^2:15$/);
  });

  it('writes the row line as "waited, in work" after the accept', () => {
    renderLive('operator', <WaitTimesInline conversation={conversation()} />);
    expect(screen.getByText(/^waited/)).toHaveTextContent(/^waited 2:15, in work 5:00$/);
  });

  it('writes the row line as "waiting" without accepted_at_us', () => {
    renderLive('operator', <WaitTimesInline conversation={notAccepted()} />);
    expect(screen.getByText(/^waiting/)).toHaveTextContent(/^waiting 0:10$/);
    expect(screen.queryByText(/waited|in work/)).not.toBeInTheDocument();
  });
});
