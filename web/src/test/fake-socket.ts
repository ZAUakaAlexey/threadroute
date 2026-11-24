// An in-memory WebSocket for client tests.

import type { SocketLike } from '../protocol/client.ts';

export class FakeSocket implements SocketLike {
  readyState = 0;
  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  readonly sent: unknown[] = [];
  closedWith: number | null = null;
  readonly url: string;

  constructor(url: string) {
    this.url = url;
  }

  send(data: string): void {
    this.sent.push(JSON.parse(data) as unknown);
  }

  close(code = 1000): void {
    this.closedWith = code;
    this.readyState = 3;
  }

  serverOpen(): void {
    this.readyState = 1;
    this.onopen?.(new Event('open'));
  }

  serverSend(message: unknown): void {
    const data = typeof message === 'string' ? message : JSON.stringify(message);
    this.onmessage?.(new MessageEvent('message', { data }));
  }

  serverClose(code: number): void {
    this.readyState = 3;
    this.onclose?.(new CloseEvent('close', { code }));
  }

  lastSent(): Record<string, unknown> {
    return (this.sent.at(-1) ?? {}) as Record<string, unknown>;
  }
}

export function socketFactory(): { readonly sockets: FakeSocket[]; readonly create: (url: string) => FakeSocket } {
  const sockets: FakeSocket[] = [];
  return {
    sockets,
    create: (url) => {
      const s = new FakeSocket(url);
      sockets.push(s);
      return s;
    },
  };
}
