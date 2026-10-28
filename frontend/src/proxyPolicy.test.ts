import { describe, expect, it } from 'vitest';
import {
  ALLOWED_ROUTES,
  MAX_BODY_BYTES,
  MAX_UPLOAD_BODY_BYTES,
  MAX_QUERY_LENGTH,
  STRIPPED_REQUEST_HEADERS,
  checkProxyRequest,
} from './proxyPolicy';

const allowed = (method: string, url: string, length?: string) =>
  checkProxyRequest(method, url, length).allowed;

describe('proxy allowlist', () => {
  it.each([
    ['GET', '/api/health'],
    ['GET', '/api/uploads'],
    ['GET', '/api/uploads/upload_a1b2c3d4e5f6'],
    ['GET', '/api/storage'],
    ['GET', '/api/projects'],
    ['GET', '/api/projects/release_impact/overview'],
    ['GET', '/api/projects/release_impact/summary'],
    ['GET', '/api/projects/release_impact/marts'],
    ['GET', '/api/projects/release_impact/marts/mart_time_trend?page=2&page_size=50'],
    ['GET', '/api/projects/release_impact/runs?limit=10'],
    ['GET', '/api/projects/release_impact/runs/3f1c9a52-8d1b-4c9e-9a77-0d6a4b1f2e10'],
    ['GET', '/api/projects/release_impact/data-quality'],
    ['GET', '/api/projects/release_impact/anomalies'],
    ['GET', '/api/projects/release_impact/lineage'],
  ])('forwards %s %s', (method, url) => {
    expect(allowed(method, url)).toBe(true);
  });

  it('forwards small run, project creation, and upload requests', () => {
    expect(allowed('POST', '/api/projects/release_impact/run', '120')).toBe(true);
    expect(allowed('POST', '/api/projects', '250')).toBe(true);
    expect(allowed('POST', '/api/uploads', '4096')).toBe(true);
    expect(allowed('POST', '/api/storage/policy', '40')).toBe(true);
    expect(allowed('POST', '/api/storage/cleanup', '90')).toBe(true);
  });

  it('keeps the storage endpoints to the verbs and paths the app uses', () => {
    // reads are GET only, changes are POST only; PUT/DELETE stay unavailable (see the method test below)
    expect(allowed('POST', '/api/storage')).toBe(false);
    expect(allowed('GET', '/api/storage/cleanup')).toBe(false);
    expect(allowed('GET', '/api/storage/policy')).toBe(false);
    expect(allowed('PUT', '/api/storage/policy', '40')).toBe(false);
    expect(allowed('DELETE', '/api/storage')).toBe(false);
    expect(allowed('POST', '/api/storage/cleanup/extra', '90')).toBe(false);
    expect(allowed('POST', '/api/storage/../uploads', '90')).toBe(false);
    expect(allowed('POST', '/api/storage/cleanup', String(MAX_BODY_BYTES + 1))).toBe(false); // too large
    expect(allowed('POST', '/api/storage/cleanup')).toBe(false); // no content-length
  });

  it('only exposes endpoints the frontend uses', () => {
    expect(ALLOWED_ROUTES).toHaveLength(12);
  });

  it.each([
    '/api',
    '/api/',
    '/health',
    '/api/openapi.json',
    '/api/docs',
    '/api/redoc',
    '/api/projects/',
    '/api/projects/a/b/c',
    '/api/projects/release_impact/unknown',
    '/api/projects/release_impact/marts/a/b',
    '/api/projects/../health',
    '/api/projects/%2e%2e/overview',
    '/api/projects/a%2Fb/overview',
    '/api/projects/a b/overview',
    '/api/projects/a.b/overview',
    '/api/projects//overview',
  ])('rejects unexpected path %s', (url) => {
    const verdict = checkProxyRequest('GET', url);
    expect(verdict.allowed).toBe(false);
    if (!verdict.allowed) expect(verdict.status).toBe(404);
  });

  it.each(['DELETE', 'PUT', 'PATCH', 'OPTIONS', 'HEAD', ''])('rejects method %s', (method) => {
    const verdict = checkProxyRequest(method, '/api/projects');
    expect(verdict.allowed).toBe(false);
    if (!verdict.allowed) expect(verdict.status).toBe(405);
  });

  it('does not allow GET on the run endpoint or POST on read-only endpoints', () => {
    expect(allowed('GET', '/api/projects/x/run')).toBe(false);
    expect(allowed('POST', '/api/projects')).toBe(false); // missing content-length
    expect(allowed('POST', '/api/projects/x/overview', '10')).toBe(false);
    expect(allowed('POST', '/api/uploads/upload_1234567890ab', '10')).toBe(false);
  });

  it('rejects oversized or unsized POST bodies', () => {
    const verdict = checkProxyRequest('POST', '/api/projects/x/run', String(MAX_BODY_BYTES + 1));
    expect(verdict.allowed).toBe(false);
    if (!verdict.allowed) expect(verdict.status).toBe(413);
    expect(allowed('POST', '/api/projects/x/run')).toBe(false);
    expect(allowed('POST', '/api/projects/x/run', 'abc')).toBe(false);
    expect(allowed('POST', '/api/projects/x/run', String(MAX_BODY_BYTES))).toBe(true);
    expect(allowed('POST', '/api/projects', String(MAX_BODY_BYTES + 1))).toBe(false);
    expect(allowed('POST', '/api/uploads', String(MAX_UPLOAD_BODY_BYTES))).toBe(true);
    expect(allowed('POST', '/api/uploads', String(MAX_UPLOAD_BODY_BYTES + 1))).toBe(false);
  });

  it('rejects very long query strings', () => {
    const long = `/api/projects?${'a'.repeat(MAX_QUERY_LENGTH + 1)}`;
    const verdict = checkProxyRequest('GET', long);
    expect(verdict.allowed).toBe(false);
    if (!verdict.allowed) expect(verdict.status).toBe(400);
  });

  it('ignores the query string when matching the path', () => {
    expect(allowed('GET', '/api/projects?x=/health')).toBe(true);
    expect(allowed('GET', '/api/health?../..')).toBe(true);
  });

  it('handles missing url or method', () => {
    expect(checkProxyRequest(undefined, undefined).allowed).toBe(false);
    expect(checkProxyRequest('GET', undefined).allowed).toBe(false);
  });

  it('strips credentials a browser could try to send', () => {
    expect(STRIPPED_REQUEST_HEADERS).toEqual(['x-api-key', 'authorization', 'cookie']);
  });
});
