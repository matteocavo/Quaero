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
  maxBodyBytes?: number;
}

export const API_PREFIX = '/api';
export const MAX_QUERY_LENGTH = 256;
/** The run and project-create request bodies are short JSON documents. */
export const MAX_BODY_BYTES = 8 * 1024;
/** Multipart CSV uploads are bounded to 10 MiB plus multipart framing overhead. */
export const MAX_UPLOAD_BODY_BYTES = 11 * 1024 * 1024;

/** Every Quaero API endpoint the frontend calls (see src/services/apiClient.ts). */
export const ALLOWED_ROUTES: readonly Route[] = [
  { method: 'GET', pattern: /^\/health$/ },
  { method: 'GET', pattern: /^\/uploads$/ },
  { method: 'GET', pattern: new RegExp(`^/uploads/${ID}$`) },
  { method: 'POST', pattern: /^\/uploads$/, maxBodyBytes: MAX_UPLOAD_BODY_BYTES },
  { method: 'POST', pattern: new RegExp(`^/uploads/${ID}/interpretation$`), maxBodyBytes: MAX_BODY_BYTES },
  { method: 'GET', pattern: /^\/storage$/ },
  { method: 'POST', pattern: /^\/storage\/(policy|cleanup)$/, maxBodyBytes: MAX_BODY_BYTES },
  { method: 'GET', pattern: /^\/projects$/ },
  { method: 'POST', pattern: /^\/projects$/, maxBodyBytes: MAX_BODY_BYTES },
  {
    method: 'GET',
    pattern: new RegExp(
      `^/projects/${ID}/(overview|summary|marts|runs|data-quality|anomalies|lineage)$`,
    ),
  },
  { method: 'GET', pattern: new RegExp(`^/projects/${ID}/marts/${ID}$`) },
  { method: 'GET', pattern: new RegExp(`^/projects/${ID}/runs/${ID}$`) },
  { method: 'POST', pattern: new RegExp(`^/projects/${ID}/run$`), maxBodyBytes: MAX_BODY_BYTES },
];

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
  const methodMatch = matching.find((route) => route.method === verb);
  if (!methodMatch) {
    return { allowed: false, status: 405, detail: 'Method not allowed for this endpoint.' };
  }

  if (verb === 'POST') {
    const maxBytes = methodMatch.maxBodyBytes ?? MAX_BODY_BYTES;
    const length = Number(Array.isArray(contentLength) ? contentLength[0] : contentLength);
    if (!Number.isFinite(length) || length > maxBytes) {
      return { allowed: false, status: 413, detail: 'Request body too large.' };
    }
  }

  return { allowed: true };
}
