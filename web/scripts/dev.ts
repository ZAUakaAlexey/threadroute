// `pnpm dev`: the fake gateway and the Vite dev server together. Vite
// forwards /v1 to the fake gateway. Ports: FAKE_GATEWAY_PORT (default 9472)
// and WEB_PORT (default 5174).

import { spawn, type ChildProcess } from 'node:child_process';

const env = { ...process.env, FAKE_GATEWAY_PORT: process.env.FAKE_GATEWAY_PORT ?? '9472' };
const children: ChildProcess[] = [
  spawn(process.execPath, ['fake-gateway/main.ts'], { stdio: 'inherit', env }),
  spawn(process.execPath, ['node_modules/vite/bin/vite.js'], { stdio: 'inherit', env }),
];

function stopAll(code: number): void {
  for (const child of children) {
    if (child.exitCode === null) child.kill('SIGTERM');
  }
  process.exitCode = code;
}

for (const child of children) child.on('exit', (code) => stopAll(code ?? 0));
process.on('SIGINT', () => stopAll(0));
process.on('SIGTERM', () => stopAll(0));
