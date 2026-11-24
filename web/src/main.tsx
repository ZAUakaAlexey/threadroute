import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { App } from './app/app.tsx';
import './index.css';

const root = document.getElementById('root');
if (root === null) throw new Error('the page has no #root element');

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
