/// <reference types="vitest/config" />
import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig, loadEnv } from 'vite';

// CONSOLE_BASE: the path the built files are served under, for example
// "/demos/threadroute/". The default "./" makes every asset URL relative.
// FAKE_GATEWAY_PORT: the port of the fake gateway that the dev server
// forwards /v1 to (only for `pnpm dev`).
// CONSOLE_OUT_DIR: the folder that `vite build` writes and `vite preview`
// serves, default "dist". The e2e tests use their own folder, so a running
// preview of dist keeps its files.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const base = env.CONSOLE_BASE ?? './';
  const gatewayPort = env.FAKE_GATEWAY_PORT ?? '9472';
  const gatewayTarget = `http://127.0.0.1:${gatewayPort}`;
  const devPrefix = base.startsWith('/') ? base : '/';

  return {
    base,
    plugins: [react(), tailwindcss()],
    server: {
      host: '127.0.0.1',
      port: Number(env.WEB_PORT ?? '5174'),
      strictPort: true,
      proxy: {
        [`${devPrefix}v1`]: {
          target: gatewayTarget,
          ws: true,
          rewrite: (path: string) => path.slice(devPrefix.length - 1),
        },
      },
    },
    preview: {
      host: '127.0.0.1',
      port: Number(env.WEB_PORT ?? '5174'),
      strictPort: true,
      proxy: {
        [`${devPrefix}v1`]: {
          target: gatewayTarget,
          ws: true,
          rewrite: (path: string) => path.slice(devPrefix.length - 1),
        },
      },
    },
    build: {
      outDir: env.CONSOLE_OUT_DIR ?? 'dist',
      target: 'es2023',
      sourcemap: false,
      reportCompressedSize: true,
    },
    test: {
      globals: true,
      environment: 'jsdom',
      setupFiles: ['./src/test/setup.ts'],
      include: ['src/**/*.test.{ts,tsx}', 'fake-gateway/**/*.test.ts'],
      restoreMocks: true,
    },
  };
});
