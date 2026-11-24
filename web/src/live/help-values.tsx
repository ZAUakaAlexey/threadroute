// Fills the help texts with the settings the gateway sent in its welcome
// message, so a text names the real accept timeout and SLA targets.

import { useMemo, type ReactNode } from 'react';

import { helpDuration, teamHelpValues, type HelpValues } from '../lib/help-texts.ts';
import { HelpValuesContext } from '../ui/help-values.ts';
import { useLive } from './hooks.ts';

export function LiveHelpValues({ children }: { readonly children: ReactNode }) {
  const acceptTimeout = useLive((s) => s.welcome?.accept_timeout_us ?? null);
  const assignmentTarget = useLive((s) => s.welcome?.assignment_target_us ?? null);
  const firstResponseTarget = useLive((s) => s.welcome?.first_response_target_us ?? null);
  const speedFactor = useLive((s) => s.welcome?.speed_factor ?? null);
  const capacity = useLive((s) => s.me?.capacity ?? null);
  // The team numbers in use, when the gateway sends stand settings.
  const teams = useLive((s) => s.settings?.current.teams ?? null);

  const values = useMemo(() => {
    const v: { -readonly [K in keyof HelpValues]: string } = {};
    if (acceptTimeout !== null) v.acceptTimeout = helpDuration(acceptTimeout);
    if (assignmentTarget !== null) v.assignmentTarget = helpDuration(assignmentTarget);
    if (firstResponseTarget !== null) v.firstResponseTarget = helpDuration(firstResponseTarget);
    if (speedFactor !== null) v.speedFactor = String(speedFactor);
    if (capacity !== null) v.capacity = String(capacity);
    if (teams !== null) Object.assign(v, teamHelpValues(teams));
    return v;
  }, [acceptTimeout, assignmentTarget, firstResponseTarget, speedFactor, capacity, teams]);

  return <HelpValuesContext value={values}>{children}</HelpValuesContext>;
}
