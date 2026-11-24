// What the user must know about the connection: the access level, whether
// the data is current, and what happens after a loss.

import { formatShortDuration } from '../lib/format.ts';
import { ROUTES } from '../app/route.ts';
import { Button } from '../ui/button.tsx';
import { AlertIcon, CloseIcon, InfoIcon } from '../ui/icons.tsx';
import { Tooltip } from '../ui/tooltip.tsx';
import { useLive, useSession } from './hooks.ts';
import { isLive } from './store.ts';

const STALE_REASONS = {
  catching_up: 'The gateway is still reading the event history.',
  source_unavailable: 'The gateway cannot read its data source.',
  lagging: 'The gateway is behind its data source.',
} as const;

/** Level and connection, for the header. */
export function LiveStatus() {
  const level = useLive((s) => s.welcome?.level ?? null);
  const isCurrent = useLive(isLive);
  const phase = useLive((s) => s.connection.phase);
  const speed = useLive((s) => s.welcome?.speed_factor ?? 1);
  const connectionLabel = phase === 'open' ? (isCurrent ? 'Live' : 'Stale') : phase === 'connecting' ? 'Connecting' : 'Offline';
  const dot = phase === 'open' && isCurrent ? 'bg-done' : phase === 'connecting' ? 'bg-ink-3' : 'bg-wait';

  return (
    <>
      {speed !== 1 && (
        <span className="rounded-full border border-line-strong px-2 py-0.5 font-mono text-[11px] text-ink-2">
          model time x{speed}
        </span>
      )}
      {level !== null && (
        <Tooltip
          content={
            level === 'demo'
              ? 'Demo level: no post text and no account IDs. Accounts and messages appear under daily pseudonyms.'
              : 'Full level: post text and account IDs are visible to you. They stay in memory only.'
          }
        >
          <button
            type="button"
            className="rounded-full border border-line-strong px-2 py-0.5 font-mono text-[11px] text-ink-2 hover:text-ink"
          >
            {level === 'demo' ? 'Demo level' : 'Full level'}
          </button>
        </Tooltip>
      )}
      <span className="flex items-center gap-1.5 text-xs text-ink-2">
        <span aria-hidden="true" className={`size-2 rounded-full ${dot}`} />
        {connectionLabel}
      </span>
    </>
  );
}

/** A banner while the data is not current; timers stop meanwhile. */
export function ConnectionBanner() {
  const session = useSession();
  const connection = useLive((s) => s.connection);
  const status = useLive((s) => s.status);
  const hasData = useLive((s) => s.hasSnapshot);

  function handleReconnect() {
    session.connect();
  }

  function handleReload() {
    window.location.reload();
  }

  const view = (() => {
    if (connection.phase === 'stopped') {
      const code = connection.closeCode;
      return {
        tone: 'late' as const,
        title: connection.close?.message ?? 'Disconnected.',
        detail: 'Live updates are stopped. Timers are stopped.',
        action:
          code === 4426 ? (
            <Button size="sm" tone="primary" onClick={handleReload}>
              Reload the page
            </Button>
          ) : (
            <Button size="sm" onClick={handleReconnect}>
              {code === 4409 ? 'Use this tab again' : 'Connect again'}
            </Button>
          ),
      };
    }
    if (connection.phase === 'waiting') {
      return {
        tone: 'wait' as const,
        title: connection.close?.message ?? 'The connection to the gateway was lost.',
        detail: `Reconnecting (try ${connection.attempt}). The data below is from before the loss, and timers are stopped.`,
        action: (
          <Button size="sm" onClick={handleReconnect}>
            Try now
          </Button>
        ),
        extra:
          connection.closeCode === 4503 ? (
            <a className="text-accent underline underline-offset-2" href={ROUTES.supervisor}>
              Open the supervisor view
            </a>
          ) : null,
      };
    }
    if (connection.phase === 'connecting' && hasData) {
      return { tone: 'wait' as const, title: 'Reconnecting to the gateway.', detail: 'Timers are stopped until the new data arrives.' };
    }
    if (connection.phase === 'open' && status?.stale === true) {
      const lag = status.lag_us === undefined ? '' : ` It is ${formatShortDuration(status.lag_us)} behind.`;
      return {
        tone: 'wait' as const,
        title: 'The data is stale.',
        detail: `${status.reason === undefined ? '' : STALE_REASONS[status.reason]}${lag} Timers are stopped.`,
      };
    }
    return null;
  })();

  return (
    <div role="status" aria-live="polite" className="empty:hidden">
      {view !== null && (
        <div
          className={`flex flex-wrap items-center gap-x-4 gap-y-2 rounded-lg border px-4 py-3 text-sm ${
            view.tone === 'late' ? 'border-late/40 bg-late-soft' : 'border-wait/40 bg-wait-soft'
          }`}
        >
          <AlertIcon className={`size-4 shrink-0 ${view.tone === 'late' ? 'text-late' : 'text-wait'}`} />
          <p className="min-w-0 flex-1">
            <strong className="font-semibold">{view.title}</strong> <span className="text-ink-2">{view.detail}</span>{' '}
            {'extra' in view ? view.extra : null}
          </p>
          {'action' in view ? view.action : null}
        </div>
      )}
    </div>
  );
}

/** The answer to the last rejected request, until dismissed or replaced. */
export function NoticeLine() {
  const notice = useLive((s) => s.notice);
  const clearNotice = useLive((s) => s.clearNotice);

  function handleDismiss() {
    clearNotice();
  }

  return (
    <div role="alert" className="empty:hidden">
      {notice !== null && (
        <div className="flex items-center gap-3 rounded-lg border border-line-strong bg-panel px-4 py-2.5 text-sm">
          <InfoIcon className={`size-4 shrink-0 ${notice.tone === 'error' ? 'text-late' : 'text-accent'}`} />
          <p className="flex-1">{notice.text}</p>
          <button
            type="button"
            onClick={handleDismiss}
            aria-label="Dismiss the message"
            className="rounded-md p-1 text-ink-3 hover:bg-sunken hover:text-ink"
          >
            <CloseIcon className="size-4" />
          </button>
        </div>
      )}
    </div>
  );
}
