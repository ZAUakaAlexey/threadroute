import { useId } from 'react';

import { formatShortDuration } from '../../lib/format.ts';
import { useLive, useSession } from '../../live/hooks.ts';
import { selectOffers } from '../../live/selectors.ts';
import { useRemainingUs } from '../../live/hooks.ts';
import { Countdown, Elapsed } from '../../live/timers.tsx';
import type { OperatorConversation } from '../../protocol/types.ts';
import { Button } from '../../ui/button.tsx';
import { languageName } from '../../ui/lang.ts';
import { Panel } from '../../ui/panel.tsx';

export function OffersPanel() {
  const offers = useLive(selectOffers);
  const acceptTimeout = useLive((s) => s.welcome?.accept_timeout_us ?? null);
  const isOnline = useLive((s) => s.me?.presence === 'online');

  return (
    <Panel title="Offers" meta={acceptTimeout === null ? undefined : `accept within ${formatShortDuration(acceptTimeout)}`}>
      {offers.length === 0 ? (
        <p className="px-4 py-6 text-sm text-ink-3">
          {isOnline
            ? 'No offer right now. The next one comes from the queues of your languages.'
            : 'You are offline. Switch on "Online" above to get offers.'}
        </p>
      ) : (
        <ul className="flex flex-col gap-3 p-3">
          {offers.map((offer) => (
            <li key={offer.id}>
              <OfferCard offer={offer} />
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}

function OfferCard({ offer }: { readonly offer: OperatorConversation }) {
  const session = useSession();
  const headingId = useId();
  const acceptTimeout = useLive((s) => s.welcome?.accept_timeout_us ?? 20_000_000);
  const isAccepting = useLive((s) => s.busy.has(`accept:${offer.offer_id}`));
  const isRefusing = useLive((s) => s.busy.has(`refuse:${offer.offer_id}`));
  const left = useRemainingUs(offer.accept_by_us ?? 0);
  const fuse = Math.max(0, Math.min(1, left / acceptTimeout));
  const isUrgent = left <= acceptTimeout / 4;

  function handleAccept() {
    void session.offerAction('accept', offer.offer_id);
  }

  function handleRefuse() {
    void session.offerAction('refuse', offer.offer_id);
  }

  return (
    <article
      aria-labelledby={headingId}
      className="overflow-hidden rounded-md border border-accent/50 bg-accent-soft/40 shadow-[inset_3px_0_0_var(--accent)]"
    >
      <div className="flex items-start justify-between gap-3 px-4 pt-3">
        <div className="min-w-0">
          <h3 id={headingId} className="flex items-center gap-2 text-sm font-semibold">
            New conversation
            <span className="rounded bg-panel px-1.5 py-0.5 font-mono text-[11px] font-normal text-ink-2">{offer.lang}</span>
          </h3>
          <p className="mt-0.5 truncate text-xs text-ink-3">
            {languageName(offer.lang)}, customer <span className="font-mono">{offer.customer}</span>
          </p>
        </div>
        <div className="text-right">
          <span className="eyebrow block">accept in</span>
          <Countdown deadlineUs={offer.accept_by_us ?? 0} label="Time to accept" warnUs={acceptTimeout / 4} className="text-2xl font-medium" />
        </div>
      </div>
      <dl className="mt-2 flex flex-wrap gap-x-4 gap-y-1 px-4 text-xs text-ink-2">
        <div className="flex gap-1">
          <dt>Waiting</dt>
          <dd>
            <Elapsed sinceUs={offer.queued_at_us} label="Waiting time" />
          </dd>
        </div>
        <div className="flex gap-1">
          <dt>Messages</dt>
          <dd className="font-mono tnum">{offer.customer_messages + offer.company_messages}</dd>
        </div>
        {offer.priority > 0 && (
          <div className="flex gap-1 text-late">
            <dt>Priority</dt>
            <dd className="font-mono tnum">{offer.priority}</dd>
          </div>
        )}
      </dl>
      <div className="mx-4 mt-3 h-1 overflow-hidden rounded-full bg-panel" aria-hidden="true">
        <div
          className={`h-full origin-left transition-transform duration-300 ease-linear ${isUrgent ? 'bg-late' : 'bg-accent'}`}
          style={{ transform: `scaleX(${fuse})` }}
        />
      </div>
      <div className="flex gap-2 p-3 pt-3">
        <Button tone="primary" className="flex-1" onClick={handleAccept} disabled={isAccepting || isRefusing} aria-describedby={headingId}>
          {isAccepting ? 'Accepting' : 'Accept'}
        </Button>
        <Button onClick={handleRefuse} disabled={isAccepting || isRefusing} aria-describedby={headingId}>
          Refuse
        </Button>
      </div>
    </article>
  );
}
