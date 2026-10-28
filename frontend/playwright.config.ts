import { defineConfig } from '@playwright/test';

/**
 * Real-browser tests of the static public build, served exactly like GitHub Pages: under /Quaero/ and
 * built with VITE_DEMO_ONLY=true (`npm run build:pages`). They use the Chrome installed on the machine,
 * so no browser is downloaded.
 *
 * Set E2E_BASE_URL (ending with a slash) to run the same tests against an already deployed site, for
 * example E2E_BASE_URL=https://matteocavo.github.io/Quaero/ npm run test:e2e
 */
const EXTERNAL_URL = process.env.E2E_BASE_URL; // e.g. the deployed site, to audit it
export const BASE_URL = EXTERNAL_URL ?? 'http://127.0.0.1:3000/Quaero/';

export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  expect: { timeout: 8_000 },
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: BASE_URL,
    channel: 'chrome',
    headless: true,
    acceptDownloads: true,
    trace: 'retain-on-failure',
  },
  webServer: EXTERNAL_URL
    ? undefined
    : {
        command: 'npm run preview',
        url: BASE_URL,
        reuseExistingServer: false,
        timeout: 60_000,
        // The preview server must use the same base path and flag as the build.
        env: { VITE_BASE_PATH: '/Quaero/', VITE_DEMO_ONLY: 'true' },
      },
});
