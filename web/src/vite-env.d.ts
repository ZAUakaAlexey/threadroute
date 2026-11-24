/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_GATEWAY_WS_URL?: string;
  readonly VITE_PUBLIC_SNAPSHOT_URL?: string;
  readonly VITE_PRIVACY_URL?: string;
  readonly VITE_SOURCE_URL?: string;
  readonly VITE_SITE_TOKEN_KEY?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
