import { defineConfig } from '@playwright/test';

const LOCAL_BASE_URL = process.env.LOCAL_E2E_BASE_URL ?? 'http://127.0.0.1:3000/';

export default defineConfig({
  testDir: './e2e-local',
  timeout: 60_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  use: {
    baseURL: LOCAL_BASE_URL,
    channel: 'chrome',
    headless: true,
    acceptDownloads: true,
    trace: 'retain-on-failure',
  },
});

