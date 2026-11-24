import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { lazy, Suspense, useState } from 'react';

import { PublicScreen } from '../screens/public/public-screen.tsx';
import { useRoute } from './route.ts';

// The live screens load on demand, so the public page stays small.
const OperatorScreen = lazy(() => import('../screens/operator/operator-screen.tsx').then((m) => ({ default: m.OperatorScreen })));
const SupervisorScreen = lazy(() =>
  import('../screens/supervisor/supervisor-screen.tsx').then((m) => ({ default: m.SupervisorScreen })),
);

export function App() {
  const route = useRoute();
  const [queryClient] = useState(() => new QueryClient());

  return (
    <QueryClientProvider client={queryClient}>
      <Suspense fallback={<p className="p-6 text-sm text-ink-3">Loading the screen.</p>}>
        {/* The key gives every screen a fresh connection. */}
        {route === 'operator' && <OperatorScreen key="operator" />}
        {route === 'supervisor' && <SupervisorScreen key="supervisor" />}
        {route === 'public' && <PublicScreen key="public" />}
      </Suspense>
    </QueryClientProvider>
  );
}
