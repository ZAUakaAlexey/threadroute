// Build-time settings (see README.md, "Configuration"). Relative URLs are
// resolved against the page, so the built files work under any path.

export interface ConsoleConfig {
  readonly gatewayWsUrl: string;
  readonly publicSnapshotUrl: string;
  readonly privacyUrl: string;
  readonly sourceUrl: string;
  /** localStorage key under which the site keeps its sign-in token, or "". */
  readonly siteTokenKey: string;
}

const DEFAULT_PRIVACY_URL =
  'https://github.com/ZAUakaAlexey/threadroute/blob/main/docs/decisions/0004-data-handling-and-privacy.md';
const DEFAULT_SOURCE_URL = 'https://github.com/ZAUakaAlexey/threadroute';

function envString(value: string | undefined, fallback: string): string {
  return value === undefined || value.trim() === '' ? fallback : value.trim();
}

function resolveUrl(url: string, base: string): URL {
  return new URL(url, base);
}

/** Turns an http(s) URL into the ws(s) URL of the same place. */
export function toWebSocketUrl(url: URL): string {
  const next = new URL(url);
  if (next.protocol === 'http:') next.protocol = 'ws:';
  if (next.protocol === 'https:') next.protocol = 'wss:';
  return next.toString();
}

export function loadConfig(base: string = document.baseURI): ConsoleConfig {
  const env = import.meta.env;
  return {
    gatewayWsUrl: toWebSocketUrl(resolveUrl(envString(env.VITE_GATEWAY_WS_URL, 'v1/ws'), base)),
    publicSnapshotUrl: resolveUrl(envString(env.VITE_PUBLIC_SNAPSHOT_URL, 'v1/public/snapshot'), base).toString(),
    privacyUrl: envString(env.VITE_PRIVACY_URL, DEFAULT_PRIVACY_URL),
    sourceUrl: envString(env.VITE_SOURCE_URL, DEFAULT_SOURCE_URL),
    siteTokenKey: envString(env.VITE_SITE_TOKEN_KEY, ''),
  };
}
