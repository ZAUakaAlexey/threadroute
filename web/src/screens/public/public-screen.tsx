import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';

import { ROUTES } from '../../app/route.ts';
import { Shell } from '../../app/shell.tsx';
import { loadConfig, type ConsoleConfig } from '../../config.ts';
import { formatCount, formatRate, formatShortDuration, totalOpen } from '../../lib/format.ts';
import type { PublicSnapshot } from '../../protocol/types.ts';
import { AlertIcon, ArrowRightIcon } from '../../ui/icons.tsx';
import { languageName } from '../../ui/lang.ts';
import { Panel } from '../../ui/panel.tsx';
import { Meter, Stat, StatGrid, StateBar } from '../../ui/stats.tsx';
import { publicSnapshotQuery } from './snapshot-query.ts';

export function PublicScreen() {
  const [config] = useState(() => loadConfig());
  const query = useQuery(publicSnapshotQuery(config.publicSnapshotUrl));
  const snapshot = query.data;
  const isFailing = query.isError || query.errorUpdatedAt > query.dataUpdatedAt;
  const isStale = isFailing || snapshot?.stale === true;

  return (
    <Shell route="public" status={<FreshnessBadge isStale={isStale} hasData={snapshot !== undefined} />}>
      <Intro />
      <div role="status" aria-live="polite" className="empty:hidden">
        {isStale && snapshot !== undefined && (
          <p className="flex items-center gap-3 rounded-lg border border-wait/40 bg-wait-soft px-4 py-3 text-sm">
            <AlertIcon className="size-4 shrink-0 text-wait" />
            {isFailing
              ? 'The numbers could not be updated. They show the last known state.'
              : 'The gateway reports stale data. The numbers may be behind.'}
          </p>
        )}
      </div>
      {snapshot === undefined ? (
        <p className="rounded-lg border border-line bg-panel px-4 py-8 text-sm text-ink-3">
          {query.isError ? 'The numbers are not available right now. The page tries again every two seconds.' : 'Loading the numbers.'}
        </p>
      ) : (
        <Numbers snapshot={snapshot} />
      )}
      <Footer config={config} />
    </Shell>
  );
}

function FreshnessBadge({ isStale, hasData }: { readonly isStale: boolean; readonly hasData: boolean }) {
  if (!hasData) return null;
  return (
    <span className="flex items-center gap-1.5 text-xs text-ink-2">
      <span aria-hidden="true" className={`size-2 rounded-full ${isStale ? 'bg-wait' : 'bg-done'}`} />
      {isStale ? 'Stale' : 'Updated every 2 s'}
    </span>
  );
}

function Intro() {
  return (
    <section className="grid gap-6 py-4 lg:grid-cols-[1.4fr_1fr] lg:items-end lg:py-8">
      <div className="flex flex-col gap-4">
        <p className="eyebrow">A live, simulated contact centre</p>
        <h1 className="max-w-3xl text-3xl leading-tight font-semibold tracking-tight text-balance sm:text-4xl">
          Public conversations, routed to operators in real time.
        </h1>
        <p className="max-w-2xl text-[15px] leading-relaxed text-ink-2">
          threadroute reads the public Bluesky post stream, groups posts into customer and company conversations, and
          routes them to simulated operators by language, with timers for every service target. This page shows totals
          only: no post text, no account names and no keys.
        </p>
      </div>
      <div className="flex flex-col gap-3 rounded-lg border border-line bg-panel p-4">
        <p className="text-sm text-ink-2">
          Take a seat between the simulated operators. Accept, mark replied and resolve real-time conversations, without
          signing in. Nothing is posted anywhere.
        </p>
        <div className="flex flex-wrap gap-2">
          <a
            href={ROUTES.operator}
            className="inline-flex h-10 items-center gap-2 rounded-md bg-accent px-4 text-sm font-medium text-accent-ink hover:brightness-110"
          >
            Try the operator role
            <ArrowRightIcon className="size-4" />
          </a>
          <a
            href={ROUTES.supervisor}
            className="inline-flex h-10 items-center rounded-md border border-line-strong bg-panel px-4 text-sm font-medium hover:bg-sunken"
          >
            Open the supervisor view
          </a>
        </div>
      </div>
    </section>
  );
}

function Numbers({ snapshot }: { readonly snapshot: PublicSnapshot }) {
  const t = snapshot.totals;
  return (
    <>
      <StatGrid label="Totals">
        <Stat label="Messages per second" value={formatRate(t.messages_per_second)} detail="last 10 seconds" />
        <Stat label="Open conversations" value={formatCount(totalOpen(t.conversations))} detail={`${formatCount(t.companies_active)} companies`} />
        <Stat label="Operators online" value={formatCount(t.operators_online)} detail={`${formatCount(t.humans_online)} of them people`} />
        <Stat
          label="Slots in use"
          value={formatCount(t.slots_used)}
          unit={`/ ${formatCount(t.slots_capacity)}`}
          detail={<Meter value={t.slots_used} max={Math.max(1, t.slots_capacity)} label="Slots in use against capacity" />}
        />
        <Stat label="Typical wait" value={formatShortDuration(t.wait_p50_us)} detail={`90 percent within ${formatShortDuration(t.wait_p90_us)}`} />
        <Stat
          label="Missed targets"
          value={formatCount(t.missed_assignment + t.missed_first_response)}
          detail="last hour"
          tone={t.missed_assignment + t.missed_first_response > 0 ? 'late' : 'done'}
        />
      </StatGrid>
      <div className="grid items-start gap-4 lg:grid-cols-[2fr_3fr]">
        <Panel title="Conversations by state">
          <div className="px-4 py-4">
            <StateBar counts={t.conversations} />
          </div>
        </Panel>
        <Panel title="Queues by language" meta={snapshot.speed_factor === 1 ? undefined : `model time x${snapshot.speed_factor}`}>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[420px] text-sm">
              <thead>
                <tr className="border-b border-line text-left">
                  <th scope="col" className="eyebrow px-4 py-2 font-normal">
                    Language
                  </th>
                  <th scope="col" className="eyebrow px-4 py-2 text-right font-normal">
                    Waiting
                  </th>
                  <th scope="col" className="eyebrow px-4 py-2 text-right font-normal">
                    Offered
                  </th>
                  <th scope="col" className="eyebrow px-4 py-2 text-right font-normal">
                    Oldest wait
                  </th>
                  <th scope="col" className="eyebrow px-4 py-2 text-right font-normal">
                    Missed
                  </th>
                </tr>
              </thead>
              <tbody>
                {snapshot.queues.map((q) => (
                  <tr key={q.lang} className="border-b border-line last:border-b-0">
                    <th scope="row" className="px-4 py-2 text-left font-normal">
                      <span className="mr-2 font-mono text-xs text-ink-3">{q.lang}</span>
                      {languageName(q.lang)}
                    </th>
                    <td className={`px-4 py-2 text-right font-mono tnum ${q.waiting > 0 ? 'text-wait' : 'text-ink-3'}`}>{formatCount(q.waiting)}</td>
                    <td className="px-4 py-2 text-right font-mono tnum">{formatCount(q.offered)}</td>
                    <td className="px-4 py-2 text-right font-mono tnum">{q.oldest_wait_us === 0 ? '-' : formatShortDuration(q.oldest_wait_us)}</td>
                    <td className={`px-4 py-2 text-right font-mono tnum ${q.missed_assignment > 0 ? 'font-medium text-late' : 'text-ink-3'}`}>
                      {formatCount(q.missed_assignment)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {snapshot.queues.length === 0 && <p className="px-4 py-3 text-sm text-ink-3">All queues are empty.</p>}
          </div>
        </Panel>
      </div>
    </>
  );
}

function Footer({ config }: { readonly config: ConsoleConfig }) {
  return (
    <footer className="mt-auto flex flex-col gap-2 border-t border-line pt-4 pb-2 text-xs leading-relaxed text-ink-3 sm:flex-row sm:justify-between">
      <p className="max-w-2xl">
        Data source: the public Bluesky post stream. The operators are simulated, and every waiting time describes the
        simulator, not real people. This is a personal, non-commercial project.
      </p>
      <p className="flex gap-4">
        <a className="underline underline-offset-2 hover:text-ink" href={config.privacyUrl}>
          How data is handled
        </a>
        <a className="underline underline-offset-2 hover:text-ink" href={config.sourceUrl}>
          Source code
        </a>
      </p>
    </footer>
  );
}
