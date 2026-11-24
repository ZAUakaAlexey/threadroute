// Selectors for the screens. Each returns a stable reference while its
// input does not change, as Zustand 5 requires: the derived list is cached
// against the identity of the input map.

import type { ConversationRow, OperatorConversation, OperatorRow, QueueRow, TeamRow } from '../protocol/types.ts';
import type { ChatMessage, LiveState } from './store.ts';

function memoByInput<I, O>(derive: (input: I) => O): (input: I) => O {
  let lastInput: I | undefined;
  let lastOutput: O | undefined;
  let hasValue = false;
  return (input) => {
    if (hasValue && input === lastInput) return lastOutput as O;
    lastInput = input;
    lastOutput = derive(input);
    hasValue = true;
    return lastOutput;
  };
}

const offersOf = memoByInput((map: ReadonlyMap<string, OperatorConversation>) =>
  [...map.values()].filter((c) => c.state === 'offered').sort((a, b) => (a.accept_by_us ?? 0) - (b.accept_by_us ?? 0)),
);

const heldOf = memoByInput((map: ReadonlyMap<string, OperatorConversation>) =>
  [...map.values()].filter((c) => c.state === 'assigned').sort((a, b) => a.queued_at_us - b.queued_at_us),
);

const operatorsOf = memoByInput((map: ReadonlyMap<string, OperatorRow>) =>
  [...map.values()].sort(
    (a, b) =>
      Number(b.presence === 'online') - Number(a.presence === 'online') ||
      Number(b.kind === 'human') - Number(a.kind === 'human') ||
      a.id.localeCompare(b.id),
  ),
);

const queuesOf = memoByInput((map: ReadonlyMap<string, QueueRow>) =>
  [...map.values()].sort((a, b) => b.waiting + b.offered - (a.waiting + a.offered) || a.id.localeCompare(b.id)),
);

const teamsOf = memoByInput((map: ReadonlyMap<string, TeamRow>) =>
  [...map.values()].sort((a, b) => b.active_load - a.active_load || a.id.localeCompare(b.id)),
);

const conversationsOf = memoByInput((map: ReadonlyMap<string, ConversationRow>) =>
  [...map.values()].sort((a, b) => (a.queued_at_us ?? a.opened_source_us) - (b.queued_at_us ?? b.opened_source_us)),
);

const NO_MESSAGES: readonly ChatMessage[] = [];

export const selectOffers = (s: LiveState): readonly OperatorConversation[] => offersOf(s.myConversations);
export const selectHeld = (s: LiveState): readonly OperatorConversation[] => heldOf(s.myConversations);
export const selectOperators = (s: LiveState): readonly OperatorRow[] => operatorsOf(s.operators);
export const selectQueues = (s: LiveState): readonly QueueRow[] => queuesOf(s.queues);
export const selectTeams = (s: LiveState): readonly TeamRow[] => teamsOf(s.teams);
export const selectConversations = (s: LiveState): readonly ConversationRow[] => conversationsOf(s.conversations);

export function selectSelectedHeld(s: LiveState): OperatorConversation | null {
  if (s.selectedId === null) return null;
  const row = s.myConversations.get(s.selectedId);
  return row?.state === 'assigned' ? row : null;
}

export function selectMessagesOf(id: string | null): (s: LiveState) => readonly ChatMessage[] {
  return (s) => (id === null ? NO_MESSAGES : (s.messages.get(id) ?? NO_MESSAGES));
}

/** True once the first messages snapshot of the conversation arrived. */
export function selectHasMessages(id: string | null): (s: LiveState) => boolean {
  return (s) => id !== null && s.messages.has(id);
}

export const selectLevel = (s: LiveState) => s.welcome?.level ?? null;
export const selectSpeedFactor = (s: LiveState) => s.welcome?.speed_factor ?? 1;
