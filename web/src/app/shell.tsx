import type { ReactNode } from 'react';

import { RouteMark } from '../ui/icons.tsx';
import { ROUTES, type Route } from './route.ts';

const NAV: readonly { readonly route: Route; readonly label: string }[] = [
  { route: 'public', label: 'Overview' },
  { route: 'operator', label: 'Operator' },
  { route: 'supervisor', label: 'Supervisor' },
];

interface ShellProps {
  readonly route: Route;
  /** Status items on the right of the header (level, connection). */
  readonly status?: ReactNode;
  readonly children: ReactNode;
}

export function Shell({ route, status, children }: ShellProps) {
  return (
    <div className="flex min-h-dvh flex-col">
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:left-2 focus:z-50 focus:rounded-md focus:bg-panel focus:px-3 focus:py-2"
      >
        Skip to content
      </a>
      <header className="sticky top-0 z-30 border-b border-line bg-paper/90 backdrop-blur supports-[backdrop-filter]:bg-paper/75">
        <div className="mx-auto flex max-w-[1440px] flex-wrap items-center gap-x-4 px-4 sm:h-14 sm:flex-nowrap sm:px-6">
          <a
            href={ROUTES.public}
            aria-label="threadroute, overview"
            className="flex shrink-0 items-center gap-2 font-mono text-sm font-medium tracking-tight text-ink"
          >
            <RouteMark className="size-5 text-accent" />
            <span className="hidden sm:inline">threadroute</span>
          </a>
          <nav aria-label="Screens" className="flex h-14 min-w-0 items-center gap-1 overflow-x-auto">
            {NAV.map((item) => (
              <a
                key={item.route}
                href={ROUTES[item.route]}
                aria-current={item.route === route ? 'page' : undefined}
                className="rounded-md px-2.5 py-1.5 text-sm text-ink-2 hover:bg-sunken hover:text-ink aria-[current=page]:bg-sunken aria-[current=page]:font-medium aria-[current=page]:text-ink"
              >
                {item.label}
              </a>
            ))}
          </nav>
          {status !== undefined && (
            <div className="-mx-4 flex w-[calc(100%+2rem)] items-center gap-2 border-t border-line px-4 py-1.5 sm:mx-0 sm:ml-auto sm:w-auto sm:border-t-0 sm:p-0">
              {status}
            </div>
          )}
        </div>
      </header>
      <main id="main" tabIndex={-1} className="mx-auto flex w-full max-w-[1440px] flex-1 flex-col gap-4 px-4 py-5 outline-none sm:px-6">
        {children}
      </main>
    </div>
  );
}
