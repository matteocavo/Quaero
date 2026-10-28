/**
 * Allowlist for the Vite dev/preview proxy that fronts the Quaero FastAPI (Local Operations mode).
 *
 * The proxy injects the API key server-side, so it must only forward the endpoints the app
 * actually uses, with the right methods, and must never let the browser choose credentials.
 * This module is pure (no Vite, no Node APIs) so it can be unit-tested.
 */

const ID = '[A-Za-z0-9_-]+';

interface Route {
  method: 'GET' | 'POST';
  pattern: RegExp;
}

/** Every Quaero API endpoint the frontend calls (see src/services/apiClient.ts). */
export const ALLOWED_ROUTES: readonly Route[] = [
  { method: 'GET', pattern: /^\/health$/ },
  { method: 'GET', pattern: /^\/projects$/ },
  {
    method: 'GET',
    pattern: new RegExp(
      `^/projects/${ID}/(overview|summary|marts|runs|data-quality|anomalies|lineage)$`,
    ),
  },
  { method: 'GET', pattern: new RegExp(`^/projects/${ID}/marts/${ID}$`) },
  { method: 'GET', pattern: new RegExp(`^/projects/${ID}/runs/${ID}$`) },
  { method: 'POST', pattern: new RegExp(`^/projects/${ID}/run$`) },
];

export const API_PREFIX = '/api';
export const MAX_QUERY_LENGTH = 256;
/** The run request body is a short JSON document (question + optional URL). */
export const MAX_BODY_BYTES = 8 * 1024;

/** Request headers the browser must never be able to forward to the API. */
export const STRIPPED_REQUEST_HEADERS = ['x-api-key', 'authorization', 'cookie'] as const;

export type ProxyVerdict =
  | { allowed: true }
  | { allowed: false; status: 400 | 404 | 405 | 413; detail: string };

/**
 * Decide whether a request to ``/api/...`` may be forwarded.
 *
 * @param method HTTP method of the incoming request
 * @param url    raw request URL including the ``/api`` prefix and any query string
 * @param contentLength value of the Content-Length header, if any
 */
export function checkProxyRequest(
  method: string | undefined,
  url: string | undefined,
  contentLength?: string | string[],
): ProxyVerdict {
  const rawUrl = url ?? '';
  if (!rawUrl.startsWith(`${API_PREFIX}/`)) {
    return { allowed: false, status: 404, detail: 'Not found.' };
  }

  const queryStart = rawUrl.indexOf('?');
  const path = queryStart === -1 ? rawUrl : rawUrl.slice(0, queryStart);
  const query = queryStart === -1 ? '' : rawUrl.slice(queryStart + 1);
  if (query.length > MAX_QUERY_LENGTH) {
    return { allowed: false, status: 400, detail: 'Query string too long.' };
  }

  const apiPath = path.slice(API_PREFIX.length);
  const matching = ALLOWED_ROUTES.filter((route) => route.pattern.test(apiPath));
  if (matching.length === 0) {
    return { allowed: false, status: 404, detail: 'This endpoint is not exposed by the proxy.' };
  }

  const verb = (method ?? '').toUpperCase();
  if (!matching.some((route) => route.method === verb)) {
    return { allowed: false, status: 405, detail: 'Method not allowed for this endpoint.' };
  }

  if (verb === 'POST') {
    const length = Number(Array.isArray(contentLength) ? contentLength[0] : contentLength);
    if (!Number.isFinite(length) || length > MAX_BODY_BYTES) {
      return { allowed: false, status: 413, detail: 'Request body too large.' };
    }
  }

  return { allowed: true };
}
