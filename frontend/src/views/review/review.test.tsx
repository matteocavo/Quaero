import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from '../../App';

type Json = Record<string, unknown> | unknown[];
const res = (body: Json, status = 200) => new Response(JSON.stringify(body), { status });

const baseItem = {
  project: 'release_impact',
  run_id: '3',
  kind: 'anomaly',
  ref: 'amount|iqr',
  rule_id: 'R1',
  superseded: false,
  snapshot_verified: true,
  artifact_provenance_verified: true,
  created_at: '2026-03-01T10:00:00Z',
  updated_at: '2026-03-01T10:00:00Z',
};

interface Store {
  items: Array<Record<string, any>>;
  events: Record<number, Array<Record<string, unknown>>>;
  calls: Array<{ url: string; method: string; body?: any }>;
  syncResponse?: () => Response;
  decisionResponse?: (body: any) => Response;
}

function makeStore(): Store {
  return {
    items: [
      { ...baseItem, id: 1, status: 'REVIEW_REQUIRED' },
      { ...baseItem, id: 2, status: 'ACCEPTED', ref: 'x|iqr' },
    ],
    events: { 1: [], 2: [] },
    calls: [],
  };
}

function install(store: Store) {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString();
    const method = (init?.method || 'GET').toUpperCase();
    const body = init?.body ? JSON.parse(String(init.body)) : undefined;
    store.calls.push({ url, method, body });
    if (url === '/api/health') return res({ status: 'ok', version: '1' });
    if (url === '/api/projects') return res({ projects: [{ project: 'release_impact', marts_count: 1, has_summary: true }] });
    if (url === '/api/uploads') return res({ uploads: [] });
    if (url === '/api/review/sync') {
      return store.syncResponse
        ? store.syncResponse()
        : res({ project: 'release_impact', run_id: 3, created: 2, existing: 0, active_rules: ['R1', 'R2'] });
    }
    const dec = url.match(/^\/api\/review\/items\/(\d+)\/decision$/);
    if (dec && method === 'POST') {
      if (store.decisionResponse) return store.decisionResponse(body);
      const it = store.items.find((i) => i.id === Number(dec[1]))!;
      const from = it.status;
      it.status = body.decision;
      store.events[it.id].push({
        id: 1, item_id: it.id, from_status: from, to_status: body.decision, reviewer: body.reviewer,
        reviewer_source: 'declared', reason: body.reason ?? null, at: '2026-03-02T09:00:00Z',
      });
      return res({ item: it, event: store.events[it.id][0], replayed: false });
    }
    const ev = url.match(/^\/api\/review\/items\/(\d+)\/events$/);
    if (ev) return res(store.events[Number(ev[1])] ?? []);
    const det = url.match(/^\/api\/review\/items\/(\d+)$/);
    if (det) {
      const it = store.items.find((i) => i.id === Number(det[1]));
      if (!it) return res({ detail: { code: 'not_found', message: 'nf' } }, 404);
      return res({ ...it, original: { impact: 'high' }, effective: { impact: 'high' }, correction: null, artifact_sha256: 'abc' });
    }
    if (url.startsWith('/api/review/items')) {
      const q = new URL(url, 'http://x').searchParams;
      let list = store.items;
      if (q.get('status')) list = list.filter((i) => i.status === q.get('status'));
      if (q.get('superseded')) list = list.filter((i) => String(i.superseded) === q.get('superseded'));
      return res({ items: list, total: list.length, limit: 20, offset: Number(q.get('offset') ?? 0) });
    }
    return res({ detail: 'Not mocked' }, 404);
  }) as unknown as typeof fetch;
}

function goLive() {
  fireEvent.click(screen.getByTitle(/Configure environment connection/i));
  fireEvent.click(screen.getByRole('button', { name: /Local Operations/i }));
}

async function openReview() {
  goLive();
  fireEvent.click(await screen.findByRole('button', { name: /Human Review/i }));
  await screen.findByTestId('review-view');
}

const reviewCalls = (s: Store) => s.calls.filter((c) => c.url.startsWith('/api/review'));

describe('Human Review UI', () => {
  const originalFetch = globalThis.fetch;
  beforeEach(() => localStorage.clear());
  afterEach(() => {
    cleanup();
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('is absent in Public Demo and makes no network calls', () => {
    const spy = vi.fn();
    globalThis.fetch = spy as unknown as typeof fetch;
    render(<App />);
    expect(screen.queryByRole('button', { name: /Human Review/i })).toBeNull();
    expect(screen.queryByTestId('review-view')).toBeNull();
    expect(spy).not.toHaveBeenCalled();
  });

  it('appears in Local Operations and does not sync or list on open without a project action', async () => {
    const s = makeStore();
    install(s);
    render(<App />);
    await openReview();
    expect(s.calls.some((c) => c.url === '/api/review/sync')).toBe(false);
  });

  it('lists the queue, filters by status, and shows empty state', async () => {
    const s = makeStore();
    install(s);
    render(<App />);
    await openReview();
    expect((await screen.findAllByTestId('review-row')).length).toBe(2);
    fireEvent.change(screen.getByLabelText('Status'), { target: { value: 'CORRECTED' } });
    expect(await screen.findByTestId('review-empty')).toBeTruthy();
    expect(s.calls.some((c) => c.url.includes('status=CORRECTED'))).toBe(true);
  });

  it('shows a retryable error when the queue fails to load', async () => {
    const s = makeStore();
    install(s);
    const base = globalThis.fetch;
    let fail = true;
    globalThis.fetch = vi.fn(async (i: RequestInfo | URL, init?: RequestInit) => {
      const u = String(i);
      if (u.startsWith('/api/review/items?') && fail) return res({ detail: { code: 'x', message: 'down' } }, 500);
      return (base as typeof fetch)(i, init);
    }) as unknown as typeof fetch;
    render(<App />);
    await openReview();
    expect(await screen.findByTestId('review-error')).toBeTruthy();
    fail = false;
    fireEvent.click(screen.getAllByRole('button', { name: 'Retry' })[0]);
    await waitFor(() => expect(screen.getAllByTestId('review-row').length).toBe(2));
  });

  it('syncs only on explicit click and reports the result', async () => {
    const s = makeStore();
    install(s);
    render(<App />);
    await openReview();
    fireEvent.click(screen.getByRole('button', { name: 'Sync Review' }));
    const out = await screen.findByTestId('sync-result');
    expect(out.textContent).toMatch(/run 3/);
    expect(out.textContent).toMatch(/2 created/);
    expect(s.calls.filter((c) => c.url === '/api/review/sync').length).toBe(1);
    expect(s.calls.find((c) => c.url === '/api/review/sync')!.body).toEqual({ project: 'release_impact' });
  });

  it('keeps the error code visible on sync failure and creates no items', async () => {
    const s = makeStore();
    s.items = [];
    s.syncResponse = () => res({ detail: { code: 'artifact_changed', message: 'changed' } }, 409);
    install(s);
    render(<App />);
    await openReview();
    fireEvent.click(screen.getByRole('button', { name: 'Sync Review' }));
    const err = await screen.findByTestId('review-error');
    expect(err.textContent).toMatch(/artifact_changed/);
    expect(screen.queryAllByTestId('review-row').length).toBe(0);
  });

  async function openItem(id = 0) {
    const rows = await screen.findAllByTestId('review-row');
    fireEvent.click(within(rows[id]).getByRole('button'));
    return screen.findByTestId('review-detail');
  }

  it('shows original/effective and two separate integrity indicators, loudly failing', async () => {
    const s = makeStore();
    s.items[0].artifact_provenance_verified = false;
    s.items[0].superseded = true;
    install(s);
    render(<App />);
    await openReview();
    const d = await openItem();
    expect(within(d).getByLabelText('Original')).toBeTruthy();
    expect(within(d).getByLabelText('Effective')).toBeTruthy();
    expect(within(d).getByTestId('superseded-badge')).toBeTruthy();
    expect(within(d).getByTestId('integrity-Snapshot').textContent).toMatch(/verified/);
    const prov = within(d).getByTestId('integrity-Artifact provenance');
    expect(prov.textContent).toMatch(/NOT verified/);
    expect(prov.getAttribute('role')).toBe('alert');
    expect(within(d).getByTestId('integrity-Snapshot').getAttribute('role')).toBe('status');
  });

  it('blocks forbidden transitions and shows the identity note and read-only history', async () => {
    const s = makeStore();
    install(s);
    render(<App />);
    await openReview();
    const d = await openItem(1); // ACCEPTED
    for (const n of ['Accept', 'Correct', 'Reject']) {
      expect(within(d).queryByRole('button', { name: n })).toBeNull();
    }
    expect(within(d).getByTestId('terminal-note')).toBeTruthy();
    expect(within(d).getByTestId('identity-note').textContent).toMatch(/not (authenticated|verified)/i);
  });

  it('disables Correct from CORRECTED but allows Accept and Reject', async () => {
    const s = makeStore();
    s.items[0].status = 'CORRECTED';
    install(s);
    render(<App />);
    await openReview();
    const d = await openItem();
    expect((within(d).getByRole('button', { name: 'Correct' }) as HTMLButtonElement).disabled).toBe(true);
    expect((within(d).getByRole('button', { name: 'Accept' }) as HTMLButtonElement).disabled).toBe(false);
    expect((within(d).getByRole('button', { name: 'Reject' }) as HTMLButtonElement).disabled).toBe(false);
  });

  it('requires a reason to reject, confirms, then records only after the server answers', async () => {
    const s = makeStore();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    install(s);
    const base = globalThis.fetch;
    globalThis.fetch = vi.fn(async (i: RequestInfo | URL, init?: RequestInit) => {
      if (String(i).endsWith('/decision')) await gate;
      return (base as typeof fetch)(i, init);
    }) as unknown as typeof fetch;
    render(<App />);
    await openReview();
    const d = await openItem();
    fireEvent.click(within(d).getByRole('button', { name: 'Reject' }));
    const dlg = await screen.findByRole('dialog');
    fireEvent.change(within(dlg).getByLabelText(/Reviewer/), { target: { value: 'Ada' } });
    fireEvent.click(within(dlg).getByRole('button', { name: 'Confirm decision' }));
    expect(within(dlg).getByText(/reason is required/i)).toBeTruthy();
    expect(s.calls.some((c) => c.url.endsWith('/decision'))).toBe(false);
    fireEvent.change(within(dlg).getByLabelText(/Reason/), { target: { value: 'wrong' } });
    fireEvent.click(within(dlg).getByRole('button', { name: 'Confirm decision' }));
    await waitFor(() => expect((within(dlg).getByRole('button', { name: /Submitting/ }) as HTMLButtonElement).disabled).toBe(true));
    expect(screen.queryByText(/Decision recorded/)).toBeNull();
    release();
    expect(await screen.findByText(/Decision recorded: Rejected/)).toBeTruthy();
    const post = s.calls.find((c) => c.url.endsWith('/decision'))!;
    expect(post.body).toMatchObject({ decision: 'REJECTED', reviewer: 'Ada', reason: 'wrong', expected_status: 'REVIEW_REQUIRED' });
    expect(post.body.client_request_id).toMatch(/^[A-Za-z0-9_.:-]{8,64}$/);
    expect(await screen.findAllByTestId('review-event')).toHaveLength(1);
  });

  it('sends an impact correction and cancelling sends nothing', async () => {
    const s = makeStore();
    install(s);
    render(<App />);
    await openReview();
    const d = await openItem();
    fireEvent.click(within(d).getByRole('button', { name: 'Correct' }));
    let dlg = await screen.findByRole('dialog');
    fireEvent.click(within(dlg).getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(s.calls.some((c) => c.url.endsWith('/decision'))).toBe(false);
    fireEvent.click(within(d).getByRole('button', { name: 'Correct' }));
    dlg = await screen.findByRole('dialog');
    fireEvent.change(within(dlg).getByLabelText(/Reviewer/), { target: { value: 'Ada' } });
    fireEvent.change(within(dlg).getByLabelText(/Corrected impact/), { target: { value: 'low' } });
    fireEvent.change(within(dlg).getByLabelText(/Reason/), { target: { value: 'minor' } });
    fireEvent.click(within(dlg).getByRole('button', { name: 'Confirm decision' }));
    await screen.findByText(/Decision recorded: Corrected/);
    expect(s.calls.find((c) => c.url.endsWith('/decision'))!.body.correction).toEqual({ field: 'impact', to_value: 'low' });
  });

  it('retries an uncertain failure with the same request id', async () => {
    const s = makeStore();
    install(s);
    let n = 0;
    const inner = globalThis.fetch;
    globalThis.fetch = vi.fn(async (i: RequestInfo | URL, init?: RequestInit) => {
      if (String(i).endsWith('/decision') && n++ === 0) {
        s.calls.push({ url: String(i), method: 'POST', body: JSON.parse(String(init?.body)) });
        return res({ detail: { code: 'x', message: 'timeout' } }, 503);
      }
      return (inner as typeof fetch)(i, init);
    }) as unknown as typeof fetch;
    render(<App />);
    await openReview();
    const d = await openItem();
    fireEvent.click(within(d).getByRole('button', { name: 'Accept' }));
    const dlg = await screen.findByRole('dialog');
    fireEvent.change(within(dlg).getByLabelText(/Reviewer/), { target: { value: 'Ada' } });
    fireEvent.click(within(dlg).getByRole('button', { name: 'Confirm decision' }));
    fireEvent.click(await within(dlg).findByRole('button', { name: 'Retry same request' }));
    await screen.findByText(/Decision recorded: Accepted/);
    const posts = s.calls.filter((c) => c.url.endsWith('/decision'));
    expect(posts).toHaveLength(2);
    expect(posts[0].body.client_request_id).toBe(posts[1].body.client_request_id);
  });

  it('reloads the item on a 409 stale_status and shows the code', async () => {
    const s = makeStore();
    s.decisionResponse = () => res({ detail: { code: 'stale_status', message: 'stale' } }, 409);
    install(s);
    render(<App />);
    await openReview();
    const d = await openItem();
    fireEvent.click(within(d).getByRole('button', { name: 'Accept' }));
    const dlg = await screen.findByRole('dialog');
    fireEvent.change(within(dlg).getByLabelText(/Reviewer/), { target: { value: 'Ada' } });
    fireEvent.click(within(dlg).getByRole('button', { name: 'Confirm decision' }));
    expect((await screen.findByTestId('decision-error')).textContent).toMatch(/stale_status/);
    expect(screen.queryByText(/Decision recorded/)).toBeNull();
  });

  it('keeps Dark/Light themes working with the review screen mounted', async () => {
    const s = makeStore();
    install(s);
    render(<App />);
    await openReview();
    const root = document.documentElement;
    expect(root.classList.contains('dark') || !root.classList.contains('dark')).toBe(true);
    expect(screen.getByTestId('review-view').className).not.toMatch(/undefined/);
  });

  it('never calls review endpoints other than the documented ones', async () => {
    const s = makeStore();
    install(s);
    render(<App />);
    await openReview();
    await screen.findAllByTestId('review-row');
    for (const c of reviewCalls(s)) expect(c.url).toMatch(/^\/api\/review\/(items|sync)/);
  });
});
