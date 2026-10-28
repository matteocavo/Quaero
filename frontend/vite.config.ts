import type { IncomingMessage, ServerResponse } from 'node:http';
import { defineConfig, type Plugin } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { API_PREFIX, checkProxyRequest, STRIPPED_REQUEST_HEADERS } from './src/proxyPolicy';

/**
 * Local Operations mode: the browser talks to this server's `/api` and the server forwards to the
 * Quaero FastAPI. The API key is read from the server's environment only (QUAERO_API_KEY) and is
 * added to the forwarded request here; it is never defined for client code, so it cannot end up in
 * the bundle. Only a fixed allowlist of endpoints is forwarded (see src/proxyPolicy.ts).
 *
 * The Public Demo (GitHub Pages build) never uses this proxy.
 */
const API_TARGET = process.env.QUAERO_API_URL || 'http://127.0.0.1:8000';
const API_KEY = process.env.QUAERO_API_KEY || '';

type Next = (err?: unknown) => void;

/** Connect middleware mounted at `/api`: `req.url` has the mount path stripped. */
function guard(req: IncomingMessage, res: ServerResponse, next: Next): void {
  const verdict = checkProxyRequest(
    req.method,
    `${API_PREFIX}${req.url ?? ''}`,
    req.headers['content-length'],
  );
  if (!verdict.allowed) {
    res.statusCode = verdict.status;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ detail: verdict.detail }));
    return;
  }
  // The browser never chooses credentials: drop anything it sent before forwarding.
  for (const header of STRIPPED_REQUEST_HEADERS) {
    delete req.headers[header];
  }
  next();
}

function quaeroApiGuard(): Plugin {
  return {
    name: 'quaero-api-guard',
    configureServer(server) {
      server.middlewares.use(API_PREFIX, guard);
    },
    configurePreviewServer(server) {
      server.middlewares.use(API_PREFIX, guard);
    },
  };
}

const proxy = {
  [API_PREFIX]: {
    target: API_TARGET,
    changeOrigin: true,
    rewrite: (path: string) => path.replace(new RegExp(`^${API_PREFIX}`), ''),
    headers: API_KEY ? { 'X-API-Key': API_KEY } : {},
  },
};

export default defineConfig({
  // `./` keeps the build relocatable; the GitHub Pages workflow sets VITE_BASE_PATH=/Quaero/.
  base: process.env.VITE_BASE_PATH || './',
  plugins: [react(), tailwindcss(), quaeroApiGuard()],
  server: {
    // Loopback only: this server injects the API key, so it must not be reachable from the LAN.
    host: '127.0.0.1',
    port: 3000,
    strictPort: true,
    proxy,
  },
  preview: {
    host: '127.0.0.1',
    port: 3000,
    strictPort: true,
    proxy,
  },
});
