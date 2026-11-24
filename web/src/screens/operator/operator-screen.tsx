import { Shell } from '../../app/shell.tsx';
import { LiveSessionProvider } from '../../live/provider.tsx';
import { ConnectionBanner, LiveStatus, NoticeLine } from '../../live/status-bar.tsx';
import { InfoIcon } from '../../ui/icons.tsx';
import { ConversationPanel } from './conversation-panel.tsx';
import { HeldPanel } from './held-panel.tsx';
import { MePanel } from './me-panel.tsx';
import { OffersPanel } from './offers-panel.tsx';

export function OperatorScreen() {
  return (
    <LiveSessionProvider view="operator">
      <Shell route="operator" status={<LiveStatus />}>
        <OperatorDesk />
      </Shell>
    </LiveSessionProvider>
  );
}

export function OperatorDesk() {
  return (
    <>
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="eyebrow">Operator</p>
          <h1 className="text-2xl font-semibold tracking-tight">Your desk</h1>
        </div>
        <p className="flex max-w-2xl items-start gap-2 rounded-lg border border-line bg-panel px-3 py-2 text-xs leading-relaxed text-ink-2">
          <InfoIcon className="mt-0.5 size-4 shrink-0 text-accent" />
          <span>
            This is a practice desk. Nothing you do here is posted to Bluesky, and the other operators around you
            are simulated. Your actions only change the simulated routing.
          </span>
        </p>
      </div>
      <ConnectionBanner />
      <NoticeLine />
      <div className="grid items-start gap-4 lg:grid-cols-[minmax(320px,400px)_1fr]">
        <div className="flex flex-col gap-4">
          <MePanel />
          <OffersPanel />
          <HeldPanel />
        </div>
        <ConversationPanel />
      </div>
    </>
  );
}
