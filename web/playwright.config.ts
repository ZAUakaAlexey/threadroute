import { defineConfig, devices } from '@playwright/test';

// End-to-end tests run the production build (vite preview) against the fake
// gateway in manual mode at speed 5, so a 20 s accept timeout of model time
// takes 4 s. One worker: the fake gateway holds shared state.
const gatewayPort = process.env.E2E_GATEWAY_PORT ?? '19573';
const webPort = process.env.E2E_WEB_PORT ?? '15274';
export const GATEWAY_URL = `http://127.0.0.1:${gatewayPort}`;
export const SITE_TOKEN_KEY = 'threadroute.e2e.site-token';

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI === undefined ? 0 : 1,
  forbidOnly: process.env.CI !== undefined,
  timeout: 30_000,
  expect: { timeout: 8_000 },
  reporter: process.env.CI === undefined ? 'list' : [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: `http://127.0.0.1:${webPort}/`,
    trace: 'retain-on-failure',
    ...devices['Desktop Chrome'],
  },
  webServer: [
    {
      command: 'node fake-gateway/main.ts',
      url: `${GATEWAY_URL}/healthz`,
      reuseExistingServer: false,
      // Three rows per snapshot frame: the supervisor's first state comes in
      // several frames, as a large one does from the real gateway.
      env: { FAKE_GATEWAY_PORT: gatewayPort, FAKE_MANUAL: '1', FAKE_SPEED_FACTOR: '5', FAKE_SEED: '3', FAKE_SNAPSHOT_FRAME_ROWS: '3' },
    },
    {
      command: 'node node_modules/vite/bin/vite.js build --logLevel warn && node node_modules/vite/bin/vite.js preview',
      url: `http://127.0.0.1:${webPort}/`,
      reuseExistingServer: false,
      timeout: 120_000,
      env: { WEB_PORT: webPort, FAKE_GATEWAY_PORT: gatewayPort, VITE_SITE_TOKEN_KEY: SITE_TOKEN_KEY, CONSOLE_BASE: './' },
    },
  ],
});
