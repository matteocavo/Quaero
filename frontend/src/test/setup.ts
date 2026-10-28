import '@testing-library/jest-dom/vitest';
import { afterEach } from 'vitest';
import { cleanup, configure } from '@testing-library/react';

// waitFor/findBy* give up after 1 s by default. With every test file running in parallel on a busy machine
// a screen can take longer than that to render, which made some tests fail intermittently without any
// change in the code. Passing tests are not slowed down: these only return as soon as the condition holds.
configure({ asyncUtilTimeout: 5000 });

afterEach(() => {
  // Some tests (the proxy integration test) run in a Node environment without a DOM.
  if (typeof window === 'undefined') return;
  cleanup();
  window.localStorage.clear();
});
