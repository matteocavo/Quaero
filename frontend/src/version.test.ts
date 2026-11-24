import { describe, expect, it } from 'vitest';
import pkg from '../package.json';
import { APP_VERSION } from './version';
import { QuaeroApiClient } from './services/apiClient';

describe('displayed version', () => {
  it('comes from package.json', () => {
    expect(APP_VERSION).toBe(pkg.version);
    expect(APP_VERSION).toMatch(/^\d+\.\d+\.\d+$/);
  });

  it('is what the demo health check reports', async () => {
    const health = await new QuaeroApiClient('demo').checkHealth();
    expect(health.version).toContain(`v${APP_VERSION}`);
  });
});
