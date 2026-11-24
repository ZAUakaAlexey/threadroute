import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';

if (typeof window !== 'undefined') {
  // jsdom has no layout. The virtualized lists need a size to render rows.
  class ResizeObserverStub {
    observe(): void {
      return undefined;
    }
    unobserve(): void {
      return undefined;
    }
    disconnect(): void {
      return undefined;
    }
  }
  window.ResizeObserver = ResizeObserverStub;
  Object.defineProperties(HTMLElement.prototype, {
    offsetHeight: { configurable: true, get: () => 600 },
    offsetWidth: { configurable: true, get: () => 1000 },
    clientHeight: { configurable: true, get: () => 600 },
    clientWidth: { configurable: true, get: () => 1000 },
  });
  HTMLElement.prototype.getBoundingClientRect = () => DOMRect.fromRect({ x: 0, y: 0, width: 1000, height: 600 });
}

afterEach(() => {
  if (typeof window === 'undefined') return;
  cleanup();
  window.localStorage.clear();
  window.sessionStorage.clear();
});
