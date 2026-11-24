import { Shell } from '../../app/shell.tsx';
import { formatCount, formatRate, formatShortDuration, shortDurationParts, totalOpen } from '../../lib/format.ts';
import { useLive } from '../../live/hooks.ts';
import { LiveSessionProvider } from '../../live/provider.tsx';
import { ConnectionBanner, LiveStatus, NoticeLine } from '../../live/status-bar.tsx';
import { HelpHint } from '../../ui/help-hint.tsx';
import { Panel } from '../../ui/panel.tsx';
import { ScreenTitle } from '../../ui/screen-title.tsx';
import { Meter, Stat, StatGrid, StateBar } from '../../ui/stats.tsx';
import { ConversationsPanel } from './conversations-panel.tsx';
import { OperatorsPanel } from './operators-panel.tsx';
import { QueuesPanel } from './queues-panel.tsx';
import { SettingsPanel } from './settings/settings-panel.tsx';
import { TeamsPanel } from './teams-panel.tsx';
import { TrendStrip } from './trend/trend-strip.tsx';

export function SupervisorScreen() {
  return (
    <LiveSessionProvider view="supervisor">
      <Shell route="supervisor" status={<LiveStatus />}>
        <SupervisorBoard />
      </Shell>
    </LiveSessionProvider>
  );
}

export function SupervisorBoard() {
  return (
    <>
      <ScreenTitle eyebrow="Supervisor" title="Floor overview" description="supervisor.description" />
      <ConnectionBanner />
      <NoticeLine />
      <FlowPanel />
      {/* Both render nothing for a gateway without stand settings and trends. */}
      <TrendStrip />
      <SettingsPanel />
      <div className="grid items-start gap-4 xl:grid-cols-[3fr_2fr]">
        <QueuesPanel />
        <TeamsPanel />
      </div>
      <OperatorsPanel />
      <ConversationsPanel />
    </>
  );
}

function FlowPanel() {
  const totals = useLive((s) => s.totals);
  if (totals === null) {
    return <p className="rounded-lg border border-line bg-panel px-4 py-6 text-sm text-ink-3">Waiting for the first numbers.</p>;
  }
  const missed = totals.missed_assignment + totals.missed_first_response;
  const medianWait = shortDurationParts(totals.wait_p50_us);
  return (
    <section aria-label="Flow" className="flex flex-col gap-3">
      <StatGrid label="Flow totals">
        <Stat label="Messages per second" hint="supervisor.totals.messages" value={formatRate(totals.messages_per_second)} detail="last 10 seconds" />
        <Stat label="Open conversations" hint="supervisor.totals.open" value={formatCount(totalOpen(totals.conversations))} detail={`${formatCount(totals.conversations.waiting)} waiting`} tone={totals.conversations.waiting > 0 ? 'wait' : 'neutral'} />
        <Stat label="Active companies" hint="supervisor.totals.companies" value={formatCount(totals.companies_active)} detail="with an open conversation" />
        <Stat label="Operators online" hint="supervisor.totals.operators" value={formatCount(totals.operators_online)} detail={`${formatCount(totals.humans_online)} human, the rest simulated`} />
        <Stat
          label="Slots in use"
          hint="supervisor.totals.slots"
          value={formatCount(totals.slots_used)}
          unit={`/ ${formatCount(totals.slots_capacity)}`}
          detail={<Meter value={totals.slots_used} max={Math.max(1, totals.slots_capacity)} label="Slots in use against capacity" />}
        />
        <Stat
          label="Wait, median"
          hint="supervisor.totals.wait"
          value={medianWait.value}
          unit={medianWait.unit}
          detail={`90th percentile ${formatShortDuration(totals.wait_p90_us)}`}
        />
      </StatGrid>
      <Panel
        title="Open conversations by state"
        meta={
          <span className="inline-flex items-center gap-1.5">
            <span className={missed > 0 ? 'text-late' : ''}>
              missed targets last hour: {formatCount(totals.missed_assignment)} assignment, {formatCount(totals.missed_first_response)} first response
            </span>
            <HelpHint id="supervisor.missed" label="Missed targets last hour" />
          </span>
        }
      >
        <div className="px-4 py-4">
          <StateBar counts={totals.conversations} />
        </div>
      </Panel>
    </section>
  );
}
