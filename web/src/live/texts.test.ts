// @vitest-environment node

import { resultMessage } from './result-messages.ts';
import { staleDetail } from './stale-text.ts';

describe('resultMessage, stale_connection', () => {
  const stale = (message?: string) =>
    resultMessage({ type: 'result', request_id: 'r1', outcome: 'rejected', code: 'stale_connection', ...(message === undefined ? {} : { message }) });

  it('names the move to another connection', () => {
    expect(stale('accept: the operator moved to another connection')).toBe('This operator session moved to another tab or window.');
  });

  it('names stale data and asks to try again later', () => {
    expect(stale('resolve: the gateway data is stale')).toMatch(/^The data is stale.*Try again when the data is current\.$/);
  });

  it('has a plain sentence for an unknown text', () => {
    expect(stale()).toBe('The gateway could not check the request. Reconnect and try again.');
  });
});

describe('staleDetail', () => {
  it('says how far behind with lagging and catching_up', () => {
    expect(staleDetail({ reason: 'lagging', lag_us: 12_000_000 })).toBe('The gateway is behind its data source. It is 12 s behind.');
    expect(staleDetail({ reason: 'catching_up', lag_us: 3_000_000 })).toContain('It is 3 s behind.');
  });

  it('says when the end of the data was last seen with source_unavailable', () => {
    expect(staleDetail({ reason: 'source_unavailable', lag_us: 4_000_000 })).toBe(
      'The gateway cannot read its data source. It last reached the end of its data 4 s ago.',
    );
  });

  it('gives the reason alone without lag_us', () => {
    expect(staleDetail({ reason: 'source_unavailable' })).toBe('The gateway cannot read its data source.');
  });
});
