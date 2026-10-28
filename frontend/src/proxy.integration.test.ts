// @vitest-environment node
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createServer, type ViteDevServer } from 'vite';

/**
 * Real Vite dev server (with the repository's vite.config.ts) in front of a fake backend.
 * Proves, end to end, that the API key is injected server-side only, that browser-supplied
 * credentials are dropped, and that only the allowlisted endpoints are forwarded.
 */

const FRONTEND_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const KEY = 'integration-test-key-not-a-real-secret';

interface Seen {
  method: string;
  url: string;
  headers: http.IncomingHttpHeaders;
  body: string;
}

let backend: http.Server;
let vite: ViteDevServer;
let port: number;
let host: string;
const seen: Seen[] = [];

function raw(
  method: string,
  requestPath: string,
  headers: Record<string, string> = {},
  body?: string,
): Promise<{ status: number; text: string }> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host, port, method, path: requestPath, headers: { ...headers } },
      (res) => {
        let text = '';
        res.on('data', (chunk) => (text += chunk));
        res.on('end', () => resolve({ status: res.statusCode ?? 0, text }));
      },
    );
    req.on('error', reject);
    if (body !== undefined) req.write(body);
    req.end();
  });
}

beforeAll(async () => {
  backend = http.createServer((req, res) => {
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      seen.push({ method: req.method ?? '', url: req.url ?? '', headers: req.headers, body });
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, path: req.url }));
    });
  });
  await new Promise<void>((resolve) => backend.listen(0, '127.0.0.1', resolve));
  const backendPort = (backend.address() as AddressInfo).port;

  process.env.QUAERO_API_URL = `http://127.0.0.1:${backendPort}`;
  process.env.QUAERO_API_KEY = KEY;

  vite = await createServer({
    root: FRONTEND_DIR,
    configFile: path.join(FRONTEND_DIR, 'vite.config.ts'),
    logLevel: 'silent',
    server: { port: 0, strictPort: false },
  });
  await vite.listen();
  const address = vite.httpServer?.address() as AddressInfo;
  port = address.port;
  host = address.address;
});

afterAll(async () => {
  await vite?.close();
  await new Promise<void>((resolve) => backend.close(() => resolve()));
  delete process.env.QUAERO_API_URL;
  delete process.env.QUAERO_API_KEY;
});

beforeEach(() => {
  seen.length = 0;
});

describe('dev server exposure', () => {
  it('listens on loopback only', () => {
    expect(host).toBe('127.0.0.1');
  });
});

describe('API key handling', () => {
  it('adds the server-side key to forwarded requests', async () => {
    const res = await raw('GET', '/api/health');
    expect(res.status).toBe(200);
    expect(seen).toHaveLength(1);
    expect(seen[0].url).toBe('/health');
    expect(seen[0].headers['x-api-key']).toBe(KEY);
  });

  it('replaces credentials supplied by the browser', async () => {
    await raw('GET', '/api/projects', {
      'X-API-Key': 'attacker-key',
      Authorization: 'Bearer stolen',
      Cookie: 'session=abc',
    });
    expect(seen).toHaveLength(1);
    expect(seen[0].headers['x-api-key']).toBe(KEY);
    expect(seen[0].headers.authorization).toBeUndefined();
    expect(seen[0].headers.cookie).toBeUndefined();
  });

  it('never serves the key to the browser (HTML and source modules)', async () => {
    for (const asset of ['/', '/src/main.tsx', '/src/services/apiClient.ts', '/src/context/AppContext.tsx']) {
      const res = await raw('GET', asset);
      expect(res.status).toBe(200);
      expect(res.text).not.toContain(KEY);
    }
    expect(seen).toHaveLength(0);
    // The first request makes Vite transform modules and pre-bundle dependencies, which can take several
    // seconds on a busy machine; the assertion is about content, not speed.
  }, 30_000);

  it('does not echo the key in proxied responses', async () => {
    const res = await raw('GET', '/api/projects');
    expect(res.text).not.toContain(KEY);
  });
});

describe('endpoint allowlist', () => {
  it('forwards path and query of allowed reads', async () => {
    await raw('GET', '/api/projects/demo/marts/mart_a?page=2&page_size=25');
    expect(seen[0].url).toBe('/projects/demo/marts/mart_a?page=2&page_size=25');
  });

  it('forwards the run request with its body', async () => {
    const body = JSON.stringify({ question: 'Which year?' });
    const res = await raw(
      'POST',
      '/api/projects/demo/run',
      { 'Content-Type': 'application/json', 'Content-Length': String(Buffer.byteLength(body)) },
      body,
    );
    expect(res.status).toBe(200);
    expect(seen[0].method).toBe('POST');
    expect(seen[0].url).toBe('/projects/demo/run');
    expect(seen[0].body).toBe(body);
  });

  it.each([
    ['/api/openapi.json'],
    ['/api/docs'],
    ['/api/projects/demo/unknown'],
    ['/api/projects/%2e%2e/overview'],
    ['/api/projects/..%2f..%2fhealth/overview'],
    ['/api/projects/a%2Fb/overview'],
  ])('blocks %s without calling the backend', async (requestPath) => {
    const res = await raw('GET', requestPath);
    expect(res.status).toBe(404);
    expect(seen).toHaveLength(0);
  });

  it('blocks methods outside the allowlist', async () => {
    for (const [method, requestPath] of [
      ['DELETE', '/api/projects'],
      ['PUT', '/api/projects/demo/run'],
      ['GET', '/api/projects/demo/run'],
      ['POST', '/api/projects/demo/overview'],
    ]) {
      const res = await raw(method, requestPath, { 'Content-Length': '0' });
      expect(res.status).toBe(405);
    }
    expect(seen).toHaveLength(0);
  });

  it('blocks an oversized run body', async () => {
    const body = JSON.stringify({ question: 'x'.repeat(20_000) });
    const res = await raw(
      'POST',
      '/api/projects/demo/run',
      { 'Content-Type': 'application/json', 'Content-Length': String(Buffer.byteLength(body)) },
      body,
    );
    expect(res.status).toBe(413);
    expect(seen).toHaveLength(0);
  });
});
