import { act, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { nowUs, renderLive, type Harness } from '../../test/live-harness.tsx';
import { OperatorDesk } from './operator-screen.tsx';

const ME = { id: 'web-test0001', kind: 'human', languages: ['en', 'und'], capacity: 2, presence: 'online', load: 2, offered: 1 };
const HELD_ID = 'ps-k2m3n4p5q6r7';

function held(extra: Record<string, unknown> = {}) {
  return {
    id: HELD_ID,
    company: 'ps-abcdefgh2345',
    customer: 'ps-zyxwvuts7654',
    lang: 'en',
    session: 1,
    state: 'assigned',
    offer_id: 'of-101',
    accepted_at_us: nowUs(-30),
    queued_at_us: nowUs(-45),
    first_response_due_us: nowUs(75),
    first_response_missed: false,
    replied: false,
    priority: 0,
    customer_messages: 2,
    company_messages: 0,
    ...extra,
  };
}

const OFFER = {
  id: 'ps-c2d3e4f5g6h7',
  company: 'ps-abcdefgh2345',
  customer: 'ps-mnopqrst2233',
  lang: 'de',
  session: 1,
  state: 'offered',
  offer_id: 'of-102',
  accept_by_us: nowUs(15),
  queued_at_us: nowUs(-10),
  first_response_missed: false,
  replied: false,
  priority: 0,
  customer_messages: 1,
  company_messages: 0,
};

function start(): Harness {
  const h = renderLive('operator', <OperatorDesk />);
  h.push({ type: 'state', seq: 1, snapshot: true, server_time_us: nowUs(), me: ME, my_conversations: { set: [held(), OFFER] } });
  return h;
}

function sentOfType(h: Harness, type: string): Record<string, unknown>[] {
  return h.socket().sent.filter((m) => (m as { type: string }).type === type) as Record<string, unknown>[];
}

describe('operator desk', () => {
  it('shows the offer with a countdown and sends accept with its offer ID', async () => {
    const h = start();
    const offer = screen.getByRole('article', { name: /new conversation/i });
    expect(within(offer).getByRole('timer', { name: /time to accept/i })).toHaveTextContent(/^0:1[45]$/);
    await userEvent.click(within(offer).getByRole('button', { name: 'Accept' }));
    expect(sentOfType(h, 'action').at(-1)).toMatchObject({ action: 'accept', offer_id: 'of-102' });
  });

  it('shows a rejected accept as a message', async () => {
    const h = start();
    await userEvent.click(screen.getByRole('button', { name: 'Accept' }));
    const request = sentOfType(h, 'action').at(-1);
    h.push({ type: 'result', request_id: request?.request_id, outcome: 'rejected', code: 'offer_expired', message: 'x' });
    expect(await screen.findByRole('alert')).toHaveTextContent('The offer expired before it was accepted.');
  });

  it('opens the held conversation, watches it, and shows placeholders at the demo level', () => {
    const h = start();
    expect(sentOfType(h, 'watch').at(-1)).toMatchObject({ conversation_id: HELD_ID });
    h.push({
      type: 'messages',
      conversation_id: HELD_ID,
      snapshot: true,
      messages: [{ id: 'ps-msgaaaa22222', direction: 'customer', source_us: nowUs(-40), length: 31, edited: false }],
    });
    expect(screen.getByRole('list', { name: 'Messages' })).toHaveTextContent('Text hidden at the demo level');
    expect(screen.getByRole('list', { name: 'Messages' })).toHaveTextContent('31 characters');
  });

  it('shows Waited and In work for the accepted conversation, in the header and the held row', () => {
    start();
    const header = screen.getByText('Waited', { selector: 'dt' }).closest('dl');
    expect(header).toHaveTextContent(/Waited.*0:1[45]/);
    expect(header).toHaveTextContent(/In work.*0:(29|3[01])/);
    expect(header).not.toHaveTextContent('Waiting');
    const row = within(screen.getByRole('list', { name: 'Conversations you hold' })).getAllByRole('listitem')[0];
    expect(row).toHaveTextContent(/waited 0:1[45], in work 0:(29|3[01])/);
  });

  it('replaces a removed message with a placeholder', () => {
    const h = start();
    h.push({
      type: 'messages',
      conversation_id: HELD_ID,
      snapshot: true,
      messages: [{ id: 'ps-msgaaaa22222', direction: 'customer', source_us: nowUs(-40), length: 31, edited: false }],
    });
    h.push({ type: 'message_removed', conversation_id: HELD_ID, message_id: 'ps-msgaaaa22222', reason: 'deleted' });
    expect(screen.getByRole('list', { name: 'Messages' })).toHaveTextContent('Message removed: the author deleted it.');
  });

  it('marks replied and resolves with the offer ID of the assignment', async () => {
    const h = start();
    await userEvent.click(screen.getByRole('button', { name: 'Mark replied' }));
    await userEvent.click(screen.getByRole('button', { name: 'Resolve' }));
    const actions = sentOfType(h, 'action').map((a) => [a.action, a.offer_id]);
    expect(actions).toEqual([
      ['reply', 'of-101'],
      ['resolve', 'of-101'],
    ]);
  });

  it('shows the replied state from the server', () => {
    const h = start();
    h.push({ type: 'state', seq: 2, snapshot: false, server_time_us: nowUs(), my_conversations: { set: [held({ replied: true })] } });
    expect(screen.getByRole('button', { name: 'Replied' })).toBeDisabled();
  });

  it('switches online with the presence action', async () => {
    const h = start();
    const toggle = screen.getByRole('switch', { name: 'Online' });
    expect(toggle).toBeChecked();
    await userEvent.click(toggle);
    expect(sentOfType(h, 'action').at(-1)).toMatchObject({ action: 'offline' });
  });

  it('shows a stale banner and stops the timers', () => {
    vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] });
    const h = start();
    h.push({ type: 'status', stale: true, reason: 'source_unavailable', lag_us: 12_500_000 });
    expect(screen.getByRole('status')).toHaveTextContent('The data is stale.');
    const timer = screen.getByRole('timer', { name: /time to accept/i });
    const before = timer.textContent;
    act(() => {
      vi.advanceTimersByTime(5000);
    });
    expect(timer.textContent).toBe(before);
    vi.useRealTimers();
  });

  it('shows the lost connection', () => {
    const h = start();
    act(() => {
      h.socket().serverClose(1006);
    });
    expect(screen.getByRole('status')).toHaveTextContent('The connection to the gateway was lost.');
    expect(screen.getByRole('button', { name: 'Try now' })).toBeEnabled();
  });
});
