import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';

import { readFixture } from '../../test/fixtures.ts';
import { PublicScreen } from './public-screen.tsx';

function renderPage(response: () => Promise<Response>) {
  vi.stubGlobal('fetch', vi.fn(response));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <PublicScreen />
    </QueryClientProvider>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('public page', () => {
  it('shows totals and queues from the snapshot, and links to the operator role', async () => {
    renderPage(() => Promise.resolve(Response.json(readFixture('public_snapshot.json'))));
    expect(await screen.findByText('Messages per second')).toBeInTheDocument();
    expect(screen.getByRole('rowheader', { name: /english/i })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /try the operator role/i })).toHaveAttribute('href', '#/operator');
    expect(screen.getByRole('link', { name: /how data is handled/i })).toBeInTheDocument();
    expect(document.body.textContent).not.toMatch(/did:|ps-[a-z2-7]{12}|sim-op|web-/);
  });

  it('never shows a field that is not in the protocol', async () => {
    const snapshot = { ...(readFixture('public_snapshot.json') as object), leaked_key: 'did:plc:shouldnotshow' };
    renderPage(() => Promise.resolve(Response.json(snapshot)));
    await screen.findByText('Messages per second');
    expect(document.body.textContent).not.toContain('shouldnotshow');
  });

  it('says so when the gateway reports stale data', async () => {
    renderPage(() => Promise.resolve(Response.json({ ...(readFixture('public_snapshot.json') as object), stale: true })));
    expect(await screen.findByText(/reports stale data/)).toBeInTheDocument();
  });

  it('says so when the numbers cannot be loaded', async () => {
    renderPage(() => Promise.resolve(new Response('', { status: 503 })));
    // The query tries once more after a second before it gives up.
    expect(await screen.findByText(/not available right now/, undefined, { timeout: 4000 })).toBeInTheDocument();
  });
});
