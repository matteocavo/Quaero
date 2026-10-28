import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

// Separate from vite.config.ts so unit tests do not load the proxy plugin or read API secrets.
export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
    css: false,
    // The UI workflow tests drive whole screens through jsdom (about 1 s each alone). With every test file
    // running in parallel on a busy machine they can take several times longer, so the 5 s default caused
    // intermittent failures that say nothing about the code.
    testTimeout: 20_000,
  },
});
