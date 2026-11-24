import { useState } from 'react';

/** Teams up to this size show every member. */
export const MEMBERS_SHOWN = 6;

interface TeamMembersProps {
  readonly team: string;
  readonly members: readonly string[];
}

/**
 * The member IDs of a team. A large team shows the first five and a
 * "+N more" button that opens the full list, so a row takes at most two
 * lines of IDs, even on a phone.
 */
export function TeamMembers({ team, members }: TeamMembersProps) {
  const [expanded, setExpanded] = useState(false);
  const canCollapse = members.length > MEMBERS_SHOWN;
  const shown = canCollapse && !expanded ? members.slice(0, MEMBERS_SHOWN - 1) : members;
  const toggleText = expanded ? 'show fewer' : `+${members.length - shown.length} more`;

  return (
    <ul aria-label={`Members of ${team}`} className="flex min-w-0 flex-wrap items-center justify-end gap-x-2 font-mono text-[11px] text-ink-3">
      {shown.map((id) => (
        <li key={id}>{id}</li>
      ))}
      {canCollapse && (
        <li>
          {/* The name starts with the visible text and adds the team. */}
          <button
            type="button"
            aria-expanded={expanded}
            aria-label={`${toggleText} members of ${team}`}
            onClick={() => setExpanded(!expanded)}
            className="rounded-sm font-sans text-xs text-accent underline-offset-2 hover:underline"
          >
            {toggleText}
          </button>
        </li>
      )}
    </ul>
  );
}
