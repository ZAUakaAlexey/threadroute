// One live session per screen, connected while the screen is shown.

import { useState, type ReactNode } from 'react';

import { loadConfig } from '../config.ts';
import { useMountEffect } from '../lib/use-mount-effect.ts';
import type { View } from '../protocol/types.ts';
import { LiveHelpValues } from './help-values.tsx';
import { LiveSessionContext } from './hooks.ts';
import { createLiveSession, type LiveSession } from './session.ts';
import { settingsStore } from './settings-store.ts';
import { readSiteToken, sessionResumeTokens } from './settings.ts';

interface LiveSessionProviderProps {
  readonly view: View;
  readonly children: ReactNode;
  /** A ready session, for tests. */
  readonly session?: LiveSession;
}

export function LiveSessionProvider({ view, children, session: given }: LiveSessionProviderProps) {
  const [session] = useState(() => given ?? createDefaultSession(view));
  useMountEffect(() => {
    session.connect();
    return () => session.dispose();
  });
  return (
    <LiveSessionContext value={session}>
      <LiveHelpValues>{children}</LiveHelpValues>
    </LiveSessionContext>
  );
}

function createDefaultSession(view: View): LiveSession {
  const config = loadConfig();
  return createLiveSession({
    view,
    url: config.gatewayWsUrl,
    getLanguages: () => settingsStore.getState().languages,
    getToken: () => readSiteToken(config.siteTokenKey),
    resumeTokens: sessionResumeTokens,
  });
}
