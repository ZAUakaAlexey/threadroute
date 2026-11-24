// Starts the fake gateway. Settings come from the environment:
//   FAKE_GATEWAY_PORT   port, default 9472 (0 picks a free one)
//   FAKE_SEED           seed of the synthetic world, default 7
//   FAKE_SPEED_FACTOR   speed factor, default 1
//   FAKE_MANUAL         "1": nothing happens unless the control API asks
//   FAKE_SNAPSHOT_FRAME_ROWS  most conversation rows in one snapshot frame, default 500
//   FAKE_TREND_INTERVAL_MS    time between two trend points, default 10000, 2000 in manual mode
// Run with `pnpm fake-gateway`. Not part of the built console.

import { startFakeGateway } from './server.ts';

const env = process.env;
const gateway = await startFakeGateway({
  port: Number(env.FAKE_GATEWAY_PORT ?? '9472'),
  seed: Number(env.FAKE_SEED ?? '7'),
  speedFactor: Number(env.FAKE_SPEED_FACTOR ?? '1'),
  isManual: env.FAKE_MANUAL === '1',
  ...(env.FAKE_SNAPSHOT_FRAME_ROWS === undefined ? {} : { snapshotFrameRows: Number(env.FAKE_SNAPSHOT_FRAME_ROWS) }),
  trendIntervalMs: Number(env.FAKE_TREND_INTERVAL_MS ?? (env.FAKE_MANUAL === '1' ? '2000' : '10000')),
  log: (line) => console.log(`[fake-gateway] ${line}`),
});

function stop(): void {
  void gateway.close().then(() => process.exit(0));
}

process.on('SIGINT', stop);
process.on('SIGTERM', stop);
