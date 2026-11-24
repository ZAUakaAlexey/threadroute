import * as Dialog from '@radix-ui/react-dialog';
import { useId, useMemo, useState, type ChangeEvent } from 'react';

import { useLive, useSession } from '../../live/hooks.ts';
import { selectConversations, selectHasMessages, selectLevel, selectMessagesOf } from '../../live/selectors.ts';
import { Elapsed } from '../../live/timers.tsx';
import { CONVERSATION_STATES, type ConversationRow, type ConversationState } from '../../protocol/types.ts';
import { CloseIcon, EyeIcon } from '../../ui/icons.tsx';
import { languageName } from '../../ui/lang.ts';
import { MessageList } from '../../ui/message-list.tsx';
import { HelpHint } from '../../ui/help-hint.tsx';
import { LabelWithHint, Panel } from '../../ui/panel.tsx';
import { Select } from '../../ui/select.tsx';
import { VirtualTable, type Column } from '../../ui/virtual-table.tsx';

const ALL = 'all';

const STATE_STYLES: Readonly<Record<ConversationState, string>> = {
  open: 'bg-sunken text-ink-2',
  waiting: 'bg-wait-soft text-wait',
  offered: 'bg-accent-soft text-accent',
  assigned: 'bg-accent text-accent-ink',
  resolved: 'bg-done-soft text-done',
};

export function ConversationsPanel() {
  const all = useLive(selectConversations);
  const [lang, setLang] = useState(ALL);
  const [state, setState] = useState(ALL);
  const langId = useId();
  const stateId = useId();
  const languages = useMemo(() => [...new Set(all.map((c) => c.lang))].sort(), [all]);
  const rows = useMemo(
    () => all.filter((c) => (lang === ALL || c.lang === lang) && (state === ALL || c.state === state)),
    [all, lang, state],
  );

  function handleLangChange(event: ChangeEvent<HTMLSelectElement>) {
    setLang(event.target.value);
  }

  function handleStateChange(event: ChangeEvent<HTMLSelectElement>) {
    setState(event.target.value);
  }

  return (
    <Panel
      title="Conversations"
      hint="supervisor.panel.conversations"
      meta={`${rows.length} of ${all.length} shown`}
      actions={
        <>
          <HelpHint id="supervisor.conversations.filters" label="Filters" />
          <label htmlFor={langId} className="sr-only">
            Filter by language
          </label>
          <Select id={langId} value={lang} onChange={handleLangChange} width="w-40">
            <option value={ALL}>All languages</option>
            {languages.map((l) => (
              <option key={l} value={l}>
                {languageName(l)} ({l})
              </option>
            ))}
          </Select>
          <label htmlFor={stateId} className="sr-only">
            Filter by state
          </label>
          <Select id={stateId} value={state} onChange={handleStateChange} width="w-28">
            <option value={ALL}>All states</option>
            {CONVERSATION_STATES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </Select>
        </>
      }
    >
      <VirtualTable
        label="Conversations"
        rows={rows}
        columns={COLUMNS}
        template="minmax(150px,1.4fr) 64px 116px minmax(100px,1fr) 88px 72px minmax(90px,0.8fr) 72px"
        rowKey={(c) => c.id}
        maxHeight={480}
        empty="No conversation matches the filters."
        minWidth={900}
      />
      <MessagesDialog />
    </Panel>
  );
}

const COLUMNS: readonly Column<ConversationRow>[] = [
  {
    key: 'customer',
    header: <LabelWithHint label="Customer" hint="supervisor.conversations.customer" />,
    label: 'Customer',
    render: (c) => (
      <span className="min-w-0">
        <span className="block truncate font-mono text-[13px]">{c.customer}</span>
        <span className="block truncate font-mono text-[11px] text-ink-3">{c.company}</span>
      </span>
    ),
  },
  {
    key: 'lang',
    header: <LabelWithHint label="Lang" hint="supervisor.conversations.lang" />,
    label: 'Lang',
    render: (c) => <span className="font-mono text-xs">{c.lang}</span>,
  },
  {
    key: 'state',
    header: <LabelWithHint label="State" hint="supervisor.conversations.state" />,
    label: 'State',
    render: (c) => (
      <span className={`rounded-full px-2 py-0.5 text-xs font-medium whitespace-nowrap ${STATE_STYLES[c.state]}`}>
        {c.state}
        {c.team_queue && c.state === 'waiting' ? ' (team)' : ''}
      </span>
    ),
  },
  {
    key: 'operator',
    header: <LabelWithHint label="Operator" hint="supervisor.conversations.operator" />,
    label: 'Operator',
    render: (c) => <span className="truncate font-mono text-xs text-ink-2">{c.operator_id ?? ''}</span>,
  },
  {
    key: 'wait',
    header: <LabelWithHint label="Waiting" hint="supervisor.conversations.waiting" />,
    label: 'Waiting',
    align: 'end',
    render: (c) =>
      c.queued_at_us === undefined || c.state === 'assigned' ? (
        <span className="text-ink-3">-</span>
      ) : (
        <Elapsed sinceUs={c.queued_at_us} label="Waiting time" className="text-xs" />
      ),
  },
  {
    key: 'messages',
    header: <LabelWithHint label="Msgs" hint="supervisor.conversations.messages" />,
    label: 'Messages',
    align: 'end',
    render: (c) => <span className="font-mono text-xs tnum">{c.customer_messages + c.company_messages}</span>,
  },
  {
    key: 'sla',
    header: <LabelWithHint label="Targets" hint="supervisor.conversations.targets" />,
    label: 'Targets',
    render: (c) =>
      c.assignment_missed || c.first_response_missed ? (
        <span className="text-xs font-medium text-late">
          missed {[c.assignment_missed ? 'assign' : '', c.first_response_missed ? 'reply' : ''].filter(Boolean).join(', ')}
        </span>
      ) : (
        <span className="text-xs text-ink-3">on time</span>
      ),
  },
  { key: 'view', header: <span className="sr-only">Messages</span>, align: 'end', render: (c) => <ViewButton row={c} /> },
];

function ViewButton({ row }: { readonly row: ConversationRow }) {
  const session = useSession();

  function handleOpen() {
    session.select(row.id);
  }

  return (
    <button
      type="button"
      onClick={handleOpen}
      aria-label={`View messages of ${row.customer}`}
      className="inline-flex h-7 items-center gap-1 rounded-md px-2 text-xs text-accent hover:bg-accent-soft"
    >
      <EyeIcon className="size-3.5" />
      View
    </button>
  );
}

function MessagesDialog() {
  const session = useSession();
  const selectedId = useLive((s) => s.selectedId);
  const row = useLive((s) => (s.selectedId === null ? null : (s.conversations.get(s.selectedId) ?? null)));
  const messages = useLive(selectMessagesOf(selectedId));
  const hasMessages = useLive(selectHasMessages(selectedId));
  const level = useLive(selectLevel);
  const isConnected = useLive((s) => s.connection.phase === 'open');

  function handleOpenChange(isOpen: boolean) {
    if (!isOpen) session.select(null);
  }

  return (
    // Open while selected, also between the frames of a snapshot when the row
    // is not there yet.
    <Dialog.Root open={selectedId !== null} onOpenChange={handleOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-40 bg-ink/30 backdrop-blur-[2px]" />
        <Dialog.Content className="fixed inset-x-3 top-[8vh] z-50 mx-auto flex max-h-[84vh] max-w-xl flex-col rounded-xl border border-line-strong bg-panel shadow-2xl">
          {selectedId !== null && (
            <>
              <header className="flex items-start gap-3 border-b border-line px-5 py-4">
                <div className="min-w-0 flex-1">
                  <Dialog.Title className="truncate font-mono text-base font-medium">{row?.customer ?? 'Conversation'}</Dialog.Title>
                  <Dialog.Description className="truncate text-xs text-ink-3">
                    {row === null ? (
                      'Loading the conversation.'
                    ) : (
                      <>
                        {languageName(row.lang)}, {row.state}, writes to <span className="font-mono">{row.company}</span>
                      </>
                    )}
                  </Dialog.Description>
                </div>
                {level !== null && (
                  <HelpHint
                    id={level === 'demo' ? 'live.messages.demo' : 'live.messages.full'}
                    label="Messages"
                    className="mt-1.5"
                  />
                )}
                <Dialog.Close className="rounded-md p-1.5 text-ink-3 hover:bg-sunken hover:text-ink" aria-label="Close">
                  <CloseIcon className="size-4" />
                </Dialog.Close>
              </header>
              <div className="overflow-y-auto px-5 py-4">
                <MessageList
                  messages={messages}
                  isLoading={!hasMessages && isConnected}
                  level={level}
                  customerLabel="Customer"
                  companyLabel="Company"
                />
              </div>
            </>
          )}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
