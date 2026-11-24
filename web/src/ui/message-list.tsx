import { formatClock } from '../lib/format.ts';
import type { ChatMessage } from '../live/store.ts';
import type { Level } from '../protocol/types.ts';

interface MessageListProps {
  readonly messages: readonly ChatMessage[];
  readonly isLoading: boolean;
  readonly level: Level | null;
  readonly customerLabel: string;
  readonly companyLabel: string;
}

/** The messages of one conversation, oldest first. */
export function MessageList({ messages, isLoading, level, customerLabel, companyLabel }: MessageListProps) {
  if (isLoading) return <p className="text-sm text-ink-3">Loading messages.</p>;
  if (messages.length === 0) {
    return <p className="text-sm text-ink-3">No message text is stored for this conversation any more.</p>;
  }
  return (
    <ol aria-label="Messages" className="flex flex-col gap-3">
      {messages.map((m) => (
        <li key={m.id} className={`flex flex-col gap-1 ${m.direction === 'company' ? 'items-end' : 'items-start'}`}>
          <span className="flex items-center gap-2 text-[11px] text-ink-3">
            <span className="font-medium text-ink-2">{m.direction === 'customer' ? customerLabel : companyLabel}</span>
            <time dateTime={new Date(m.sourceUs / 1000).toISOString()} className="font-mono tnum">
              {formatClock(m.sourceUs)}
            </time>
            {m.edited && <span className="rounded bg-sunken px-1">edited</span>}
          </span>
          <MessageBody message={m} level={level} />
        </li>
      ))}
    </ol>
  );
}

function MessageBody({ message, level }: { readonly message: ChatMessage; readonly level: Level | null }) {
  const bubble = message.direction === 'company' ? 'bg-accent-soft' : 'bg-sunken';
  if (message.removedReason !== undefined) {
    return (
      <p className="max-w-[80%] rounded-lg border border-dashed border-line-strong px-3 py-2 text-sm text-ink-3 italic">
        {message.removedReason === 'deleted' ? 'Message removed: the author deleted it.' : 'Message removed: the author is gone.'}
      </p>
    );
  }
  if (message.text !== undefined) {
    return <p className={`max-w-[80%] rounded-lg px-3 py-2 text-sm leading-relaxed break-words whitespace-pre-wrap ${bubble}`}>{message.text}</p>;
  }
  return (
    <p className={`flex max-w-[80%] items-center gap-2 rounded-lg px-3 py-2 text-sm text-ink-2 ${bubble}`}>
      <span aria-hidden="true" className="hidden shrink-0 gap-0.5 sm:flex">
        {Array.from({ length: Math.min(12, Math.max(2, Math.round(message.length / 8))) }, (_, i) => (
          <span key={i} className="h-1.5 w-2.5 rounded-full bg-ink-3/30" />
        ))}
      </span>
      <span>
        {level === 'demo' ? 'Text hidden at the demo level' : 'Text not available'},{' '}
        <span className="font-mono tnum">{message.length}</span> characters
      </span>
    </p>
  );
}
