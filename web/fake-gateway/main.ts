// Starts the fake gateway. Settings come from the environment:
//   FAKE_GATEWAY_PORT   port, default 9472 (0 picks a free one)
//   FAKE_SEED           seed of the synthetic world, default 7
//   FAKE_SPEED_FACTOR   speed factor, default 1
//   FAKE_MANUAL         "1": nothing happens unless the control API asks
// Run with `pnpm fake-gateway`. Not part of the built console.

import { startFakeGateway } from './server.ts';

const env = process.env;
const gateway = await startFakeGateway({
  port: Number(env.FAKE_GATEWAY_PORT ?? '9472'),
  seed: Number(env.FAKE_SEED ?? '7'),
  speedFactor: Number(env.FAKE_SPEED_FACTOR ?? '1'),
  isManual: env.FAKE_MANUAL === '1',
  log: (line) => console.log(`[fake-gateway] ${line}`),
});

function stop(): void {
  void gateway.close().then(() => process.exit(0));
}

process.on('SIGINT', stop);
process.on('SIGTERM', stop);
