import '@testing-library/jest-dom/vitest';
import { afterEach } from 'vitest';
import { cleanup } from '@testing-library/react';

afterEach(() => {
  // Some tests (the proxy integration test) run in a Node environment without a DOM.
  if (typeof window === 'undefined') return;
  cleanup();
  window.localStorage.clear();
});
