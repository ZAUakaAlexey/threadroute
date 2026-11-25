import { useLive, useSession } from '../../live/hooks.ts';
import { selectHasMessages, selectLevel, selectMessagesOf, selectSelectedHeld } from '../../live/selectors.ts';
import type { OperatorConversation } from '../../protocol/types.ts';
import { Button } from '../../ui/button.tsx';
import { CheckIcon } from '../../ui/icons.tsx';
import { languageName } from '../../ui/lang.ts';
import { MessageList } from '../../ui/message-list.tsx';
import { HelpHint } from '../../ui/help-hint.tsx';
import { useHelpText } from '../../ui/help-values.ts';
import { Panel } from '../../ui/panel.tsx';
import { FirstResponseBadge } from './held-panel.tsx';
import { WaitTimesBlocks } from './wait-times.tsx';

export function ConversationPanel() {
  const conversation = useLive(selectSelectedHeld);

  if (conversation === null) {
    return (
      <Panel title="Conversation" className="lg:min-h-[480px]">
        <div className="flex flex-col items-start gap-2 px-4 py-10 text-sm text-ink-3">
          <p className="text-ink-2">No conversation open.</p>
          <p>Accept an offer, or choose a conversation you hold, to see its messages here.</p>
        </div>
      </Panel>
    );
  }
  return <ConversationView key={conversation.id} conversation={conversation} />;
}

function ConversationView({ conversation }: { readonly conversation: OperatorConversation }) {
  const session = useSession();
  const level = useLive(selectLevel);
  const messages = useLive(selectMessagesOf(conversation.id));
  const hasMessages = useLive(selectHasMessages(conversation.id));
  const isReplying = useLive((s) => s.busy.has(`reply:${conversation.offer_id}`));
  const isResolving = useLive((s) => s.busy.has(`resolve:${conversation.offer_id}`));
  const isLive = useLive((s) => s.connection.phase === 'open');
  const note = useHelpText('operator.conversation.note');

  function handleReply() {
    void session.offerAction('reply', conversation.offer_id);
  }

  function handleResolve() {
    void session.offerAction('resolve', conversation.offer_id);
  }

  return (
    <Panel
      title="Conversation"
      hint="operator.panel.conversation"
      className="flex flex-col lg:min-h-[480px]"
      meta={
        <span>
          {languageName(conversation.lang)}, session {conversation.session}
        </span>
      }
      actions={level === null ? undefined : <HelpHint id={level === 'demo' ? 'live.messages.demo' : 'live.messages.full'} label="Messages" />}
    >
      <div className="grid gap-4 border-b border-line px-4 py-4 sm:grid-cols-[1fr_auto]">
        <div className="min-w-0">
          <h3 className="truncate font-mono text-base font-medium">{conversation.customer}</h3>
          <p className="truncate text-xs text-ink-3">
            writes to <span className="font-mono">{conversation.company}</span>
          </p>
        </div>
        <dl className="flex flex-wrap items-start gap-x-6 gap-y-2 text-xs">
          <WaitTimesBlocks conversation={conversation} />
          <div>
            <dt className="eyebrow flex items-center gap-1.5">
              First response
              <HelpHint id="operator.first-response" label="First response" />
            </dt>
            <dd className="text-lg">
              <FirstResponseBadge row={conversation} hasCaption={false} />
            </dd>
          </div>
          <div>
            <dt className="eyebrow flex items-center gap-1.5">
              Messages
              <HelpHint id="operator.messages" label="Messages count" />
            </dt>
            <dd className="font-mono text-lg tnum">
              {conversation.customer_messages}
              <span className="text-ink-3"> / {conversation.company_messages}</span>
            </dd>
          </div>
        </dl>
      </div>
      <div className="flex-1 px-4 py-4">
        <MessageList messages={messages} isLoading={!hasMessages && isLive} level={level} customerLabel="Customer" companyLabel="Company" />
      </div>
      <footer className="flex flex-wrap items-center gap-2 border-t border-line bg-sunken/50 px-4 py-3">
        <Button tone="primary" onClick={handleReply} disabled={conversation.replied || isReplying}>
          {conversation.replied ? (
            <>
              <CheckIcon className="size-4" /> Replied
            </>
          ) : (
            'Mark replied'
          )}
        </Button>
        <Button onClick={handleResolve} disabled={isResolving}>
          Resolve
        </Button>
        <p className="ml-auto flex max-w-sm items-center gap-1.5 text-xs text-ink-3">
          {note}
          <HelpHint id="operator.actions" label="Conversation actions" />
        </p>
      </footer>
    </Panel>
  );
}
