import { Shell } from '../../app/shell.tsx';
import { LiveSessionProvider } from '../../live/provider.tsx';
import { ConnectionBanner, LiveStatus, NoticeLine } from '../../live/status-bar.tsx';
import { ScreenTitle } from '../../ui/screen-title.tsx';
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
      <ScreenTitle eyebrow="Operator" title="Your desk" description="operator.description" />
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
