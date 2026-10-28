/**
 * Where the app runs decides whether Local Operations can work at all.
 *
 * Local Operations talks to a Quaero API on the user's own machine through the dev/preview server's
 * `/api` proxy. A static host (GitHub Pages, any CDN) has neither the proxy nor the API, so there the
 * mode must be unavailable and must never send a request.
 */
const LOOPBACK_HOSTS = new Set(['localhost', '127.0.0.1', '[::1]', '::1']);

export function isLocalOperationsAvailable(hostname: string, demoOnlyFlag?: string): boolean {
  if (demoOnlyFlag === 'true') return false;
  return LOOPBACK_HOSTS.has(hostname);
}

export function localOperationsAvailable(): boolean {
  if (typeof window === 'undefined') return false;
  return isLocalOperationsAvailable(window.location.hostname, import.meta.env.VITE_DEMO_ONLY);
}

export const LOCAL_UNAVAILABLE_MESSAGE =
  'Local Operations is not available on this public site: it needs the Quaero API running on your own ' +
  'machine. Run Quaero locally (see the README) to use it. Everything else works here in your browser.';
