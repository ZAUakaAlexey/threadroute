// The wait clocks of a held conversation. Before the accept, "Waiting" runs
// from the queue time. After the accept, "Waited" is the fixed time from the
// queue to the accept, and "In work" runs from the accept.

import { Elapsed, FixedDuration } from '../../live/timers.tsx';
import type { OperatorConversation } from '../../protocol/types.ts';
import { HelpHint } from '../../ui/help-hint.tsx';

interface WaitTimesProps {
  readonly conversation: OperatorConversation;
}

/** The small line in a row of the held list: "waited 2:15, in work 5:00". */
export function WaitTimesInline({ conversation }: WaitTimesProps) {
  const acceptedAtUs = conversation.accepted_at_us;
  if (acceptedAtUs === undefined) {
    return (
      <span>
        waiting <Elapsed sinceUs={conversation.queued_at_us} label="Waiting time" className="text-ink-2" />
      </span>
    );
  }
  return (
    <span className="truncate">
      waited <FixedDuration fromUs={conversation.queued_at_us} toUs={acceptedAtUs} className="text-ink-2" />, in work{' '}
      <Elapsed sinceUs={acceptedAtUs} label="Time in work" className="text-ink-2" />
    </span>
  );
}

/** The wait blocks in the header of the open conversation, inside its dl. */
export function WaitTimesBlocks({ conversation }: WaitTimesProps) {
  const acceptedAtUs = conversation.accepted_at_us;
  if (acceptedAtUs === undefined) {
    return (
      <div>
        <dt className="eyebrow flex items-center gap-1.5">
          Waiting
          <HelpHint id="operator.waiting" label="Waiting" />
        </dt>
        <dd className="text-lg">
          <Elapsed sinceUs={conversation.queued_at_us} label="Waiting time" />
        </dd>
      </div>
    );
  }
  return (
    <>
      <div>
        <dt className="eyebrow flex items-center gap-1.5">
          Waited
          <HelpHint id="operator.waiting" label="Waited" />
        </dt>
        <dd className="text-lg">
          <FixedDuration fromUs={conversation.queued_at_us} toUs={acceptedAtUs} />
        </dd>
      </div>
      <div>
        <dt className="eyebrow flex items-center gap-1.5">
          In work
          <HelpHint id="operator.in-work" label="In work" />
        </dt>
        <dd className="text-lg">
          <Elapsed sinceUs={acceptedAtUs} label="Time in work" />
        </dd>
      </div>
    </>
  );
}
