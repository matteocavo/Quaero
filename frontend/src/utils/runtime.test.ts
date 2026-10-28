import { describe, expect, it } from 'vitest';
import { isLocalOperationsAvailable } from './runtime';

describe('isLocalOperationsAvailable', () => {
  it.each(['localhost', '127.0.0.1', '[::1]'])('is available on loopback host %s', (host) => {
    expect(isLocalOperationsAvailable(host)).toBe(true);
    expect(isLocalOperationsAvailable(host, 'false')).toBe(true);
  });

  it.each(['matteocavo.github.io', 'example.com', 'quaero.pages.dev', '192.168.1.20', ''])(
    'is not available on static or remote host %s',
    (host) => {
      expect(isLocalOperationsAvailable(host)).toBe(false);
    },
  );

  it('is disabled by the public-build flag even on loopback', () => {
    expect(isLocalOperationsAvailable('localhost', 'true')).toBe(false);
    expect(isLocalOperationsAvailable('127.0.0.1', 'true')).toBe(false);
  });
});
