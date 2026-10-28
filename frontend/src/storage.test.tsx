import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import type {
  CleanupRequest,
  LifecyclePolicy,
  StorageProjectItem,
  StorageStatusResponse,
  StorageUploadItem,
} from './types/api';

const upload = (n: number, state: StorageUploadItem['state'], extra: Partial<StorageUploadItem> = {}): StorageUploadItem => ({
  upload_id: `upload_${String(n).padStart(12, '0')}`,
  original_filename: `data_${n}.csv`,
  size_bytes: 2048 * n,
  created_at: `2026-10-0${n}T10:00:00Z`,
  state,
  linked_projects: state === 'saved' ? ['my_project'] : [],
  incomplete: false,
  ...extra,
});

const projects: StorageProjectItem[] = [
  { project: 'release_impact', kind: 'preexisting', upload_id: null, config_bytes: 900, artifacts_bytes: 50_000, artifact_files: 12, marts_count: 8, can_clear_artifacts: false },
  { project: 'my_project', kind: 'upload_project', upload_id: 'upload_000000000003', config_bytes: 400, artifacts_bytes: 8192, artifact_files: 5, marts_count: 3, can_clear_artifacts: true },
];

/** A small in-memory stand-in for the API that behaves like it: a deletion changes the next status. */
function fakeBackend(initial: StorageUploadItem[]) {
  let uploads = [...initial];
  let policy: LifecyclePolicy = 'ask_on_exit';
  let artifacts = 8192;
  const calls: { url: string; method: string; body: Record<string, unknown> | null }[] = [];
  const skipOnNextCleanup = new Set<string>();
  let refuseWith: { status: number; detail: string } | null = null;

  const status = (): StorageStatusResponse => {
    const temp = uploads.filter((u) => u.state !== 'saved');
    const sum = (items: StorageUploadItem[]) => items.reduce((t, u) => t + u.size_bytes, 0);
    return {
      session_id: 'session_test',
      lifecycle_policy: policy,
      totals: {
        temporary_uploads_bytes: sum(temp),
        saved_uploads_bytes: sum(uploads.filter((u) => u.state === 'saved')),
        project_configs_bytes: 1300,
        artifacts_bytes: 50_000 + artifacts,
        total_bytes: 0,
      },
      current_session_uploads: uploads.filter((u) => u.state === 'current_session').length,
      orphaned_uploads: uploads.filter((u) => u.state === 'orphaned').length,
      uploads,
      projects: projects.map((p) => (p.project === 'my_project' ? { ...p, artifacts_bytes: artifacts, can_clear_artifacts: artifacts > 0 } : p)),
    };
  };

  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString();
    const method = (init?.method || 'GET').toUpperCase();
    const body = init?.body ? (JSON.parse(String(init.body)) as Record<string, unknown>) : null;
    calls.push({ url, method, body });
    const json = (data: unknown, code = 200) => new Response(JSON.stringify(data), { status: code });

    if (url === '/api/health') return json({ status: 'ok', version: '1.0.0' });
    if (url === '/api/projects' && method === 'GET') return json({ projects: [{ project: 'release_impact', marts_count: 8, has_summary: true }] });
    if (url === '/api/uploads' && method === 'GET') return json({ uploads: [] });
    if (url === '/api/storage' && method === 'GET') return json(status());
    if (url === '/api/storage/policy' && method === 'POST') {
      policy = (body as { lifecycle_policy: LifecyclePolicy }).lifecycle_policy;
      return json({ lifecycle_policy: policy });
    }
    if (url === '/api/storage/cleanup' && method === 'POST') {
      const req = body as unknown as CleanupRequest;
      if (refuseWith) return json({ detail: refuseWith.detail }, refuseWith.status);
      if (req.confirm !== true) return json({ detail: 'Permanent deletion needs explicit confirmation (confirm=true).' }, 422);
      const wanted = req.scope === 'current_session' || req.scope === 'orphaned' ? req.scope : null;
      const targets = uploads.filter((u) => (wanted ? u.state === wanted : u.upload_id === req.target_id));
      const skipped = targets.filter((u) => skipOnNextCleanup.has(u.upload_id));
      const gone = req.scope === 'project_artifacts' ? [] : targets.filter((u) => !skipOnNextCleanup.has(u.upload_id));
      uploads = uploads.filter((u) => !gone.includes(u));
      if (req.scope === 'project_artifacts') artifacts = 0;
      return json({
        scope: req.scope,
        deleted: req.scope === 'project_artifacts' ? [`${req.target_id}: 5 generated files`] : gone.map((u) => u.upload_id),
        freed_bytes: gone.reduce((t, u) => t + u.size_bytes, 0),
        skipped: skipped.map((u) => ({ id: u.upload_id, reason: 'in use by another program' })),
        message: `Deleted ${gone.length} items, freed ${gone.reduce((t, u) => t + u.size_bytes, 0)} bytes.`,
      });
    }
    return json({ detail: 'Not mocked' }, 404);
  });

  return {
    fetchMock,
    calls,
    cleanupCalls: () => calls.filter((c) => c.url === '/api/storage/cleanup'),
    skip: (id: string) => skipOnNextCleanup.add(id),
    refuse: (status: number, detail: string) => {
      refuseWith = { status, detail };
    },
  };
}

function openStorage() {
  fireEvent.click(screen.getByTitle(/Configure environment connection/i));
  fireEvent.click(screen.getByRole('button', { name: /Local Operations/i }));
  fireEvent.click(screen.getAllByRole('button', { name: /Local Storage/i })[0]);
}

const dialog = () => screen.getByRole('dialog');

describe('Local Storage screen', () => {
  const originalFetch = globalThis.fetch;

  beforeEach(() => localStorage.clear());
  afterEach(() => {
    cleanup();
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  const standard = () => [upload(1, 'current_session'), upload(2, 'orphaned'), upload(3, 'saved')];

  it('does not exist in the public demo and makes no request there', () => {
    const backend = fakeBackend(standard());
    globalThis.fetch = backend.fetchMock as unknown as typeof fetch;
    render(<App />);
    expect(screen.queryByRole('button', { name: /Local Storage/i })).toBeNull();
    expect(backend.fetchMock).not.toHaveBeenCalled();
  });

  it('shows space by category and tells temporary, saved and protected data apart', async () => {
    globalThis.fetch = fakeBackend(standard()).fetchMock as unknown as typeof fetch;
    render(<App />);
    openStorage();

    await screen.findByRole('heading', { name: /Local Storage/ });
    const temporary = screen.getByText('Temporary uploads', { selector: 'h3' }).closest('section') as HTMLElement;
    expect(within(temporary).getByText('data_1.csv')).toBeTruthy();
    expect(within(temporary).getByText('data_2.csv')).toBeTruthy();
    expect(within(temporary).queryByText('data_3.csv')).toBeNull(); // used by a project: not temporary
    expect(within(temporary).getByText('this session')).toBeTruthy();
    expect(within(temporary).getByText('earlier session')).toBeTruthy();

    // totals come from the server
    expect(screen.getByText('Temporary uploads', { selector: 'div' }).nextElementSibling?.textContent).toBe('6.0 KB');
    expect(screen.getByText('Saved uploads').nextElementSibling?.textContent).toBe('6.0 KB');

    const saved = screen.getByText('Saved projects').closest('section') as HTMLElement;
    expect(within(saved).getByText(/existing project, protected/)).toBeTruthy();
    expect(within(saved).getByText(/uses data_3\.csv .*kept/)).toBeTruthy();
    // protected data has no delete button; only the upload-created project can be cleared
    expect(within(saved).getAllByRole('button')).toHaveLength(1);
    expect(within(saved).getByRole('button', { name: 'Clear generated files of my_project' })).toBeTruthy();
    expect(screen.queryByRole('button', { name: /Delete data_3/ })).toBeNull();
  });

  it('defaults to Ask on exit and saves another choice on the server', async () => {
    const backend = fakeBackend(standard());
    globalThis.fetch = backend.fetchMock as unknown as typeof fetch;
    render(<App />);
    openStorage();
    await screen.findByRole('heading', { name: /Local Storage/ });

    expect((screen.getByRole('radio', { name: /Ask on exit/ }) as HTMLInputElement).checked).toBe(true);
    fireEvent.click(screen.getByRole('radio', { name: /Keep files/ }));
    await waitFor(() => expect((screen.getByRole('radio', { name: /Keep files/ }) as HTMLInputElement).checked).toBe(true));
    expect(backend.calls.find((c) => c.url === '/api/storage/policy')?.body).toEqual({ lifecycle_policy: 'keep_files' });
    expect(backend.cleanupCalls()).toHaveLength(0);
  });

  it('never deletes anything just by looking, refreshing or opening the confirmation', async () => {
    const backend = fakeBackend(standard());
    globalThis.fetch = backend.fetchMock as unknown as typeof fetch;
    render(<App />);
    openStorage();
    await screen.findByRole('heading', { name: /Local Storage/ });
    fireEvent.click(screen.getByRole('button', { name: /Refresh/ }));
    fireEvent.click(screen.getByRole('button', { name: 'Delete data_1.csv' }));
    expect(dialog()).toBeTruthy();
    expect(backend.cleanupCalls()).toHaveLength(0);
  });

  it('asks for confirmation, lists exactly what goes, and sends only a scope and an id', async () => {
    const backend = fakeBackend(standard());
    globalThis.fetch = backend.fetchMock as unknown as typeof fetch;
    render(<App />);
    openStorage();
    await screen.findByRole('heading', { name: /Local Storage/ });

    fireEvent.click(screen.getByRole('button', { name: 'Delete data_1.csv' }));
    expect(within(dialog()).getByText(/permanently deletes/i)).toBeTruthy();
    expect(within(dialog()).getByText(/data_1\.csv \(upload_000000000001/)).toBeTruthy();
    expect(within(dialog()).queryByText(/data_2\.csv/)).toBeNull();
    // the safe button has the focus
    expect(document.activeElement).toBe(within(dialog()).getByRole('button', { name: 'Cancel' }));

    fireEvent.click(within(dialog()).getByRole('button', { name: 'Delete permanently' }));
    await waitFor(() => expect(backend.cleanupCalls()).toHaveLength(1));
    // no path, no file name: just the scope and the id that the server handed out
    expect(backend.cleanupCalls()[0].body).toEqual({ scope: 'single_upload', target_id: 'upload_000000000001', confirm: true });

    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    await screen.findByText(/Deleted upload_000000000001/);
    const temporary = screen.getByText('Temporary uploads', { selector: 'h3' }).closest('section') as HTMLElement;
    expect(within(temporary).queryByText('data_1.csv')).toBeNull(); // the list was reloaded from the server
    expect(within(temporary).getByText('data_2.csv')).toBeTruthy();
  });

  it('Cancel and Escape close the confirmation without deleting', async () => {
    const backend = fakeBackend(standard());
    globalThis.fetch = backend.fetchMock as unknown as typeof fetch;
    render(<App />);
    openStorage();
    await screen.findByRole('heading', { name: /Local Storage/ });

    fireEvent.click(screen.getByRole('button', { name: 'Delete data_1.csv' }));
    fireEvent.click(within(dialog()).getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Delete data_2.csv' }));
    fireEvent.keyDown(window, { key: 'Escape' });
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(backend.cleanupCalls()).toHaveLength(0);
  });

  it('after an unexpected close it warns about leftovers, deletes none by itself, and deletes them only when asked', async () => {
    const backend = fakeBackend([upload(1, 'orphaned'), upload(2, 'orphaned'), upload(3, 'saved'), upload(4, 'current_session')]);
    globalThis.fetch = backend.fetchMock as unknown as typeof fetch;
    render(<App />);
    openStorage();

    // told as soon as Local Operations connects
    await screen.findByText(/2 temporary uploads from an earlier session are still on disk\. Open Local Storage/);
    await screen.findByRole('heading', { name: /Local Storage/ });
    expect(backend.cleanupCalls()).toHaveLength(0);

    fireEvent.click(screen.getByRole('button', { name: /Review and delete/ }));
    expect(within(dialog()).getByText(/data_1\.csv/)).toBeTruthy();
    expect(within(dialog()).getByText(/data_2\.csv/)).toBeTruthy();
    expect(within(dialog()).queryByText(/data_4\.csv/)).toBeNull(); // this session's file is not a leftover
    expect(within(dialog()).queryByText(/data_3\.csv/)).toBeNull(); // saved data is not listed for deletion
    fireEvent.click(within(dialog()).getByRole('button', { name: 'Delete permanently' }));

    await waitFor(() => expect(backend.cleanupCalls()).toHaveLength(1));
    expect(backend.cleanupCalls()[0].body).toEqual({ scope: 'orphaned', confirm: true });
    await waitFor(() => expect(screen.queryByText(/still on disk, for example after Quaero closed/)).toBeNull());
  });

  it('says what could not be deleted, and why, instead of claiming success', async () => {
    const backend = fakeBackend(standard());
    backend.skip('upload_000000000001');
    globalThis.fetch = backend.fetchMock as unknown as typeof fetch;
    render(<App />);
    openStorage();
    await screen.findByRole('heading', { name: /Local Storage/ });

    fireEvent.click(screen.getByRole('button', { name: /Delete this session’s uploads \(1\)/ }));
    fireEvent.click(within(dialog()).getByRole('button', { name: 'Delete permanently' }));
    const note = await screen.findByText(/Left in place: upload_000000000001 \(in use by another program\)/);
    expect(note).toBeTruthy();
    expect(screen.getByText('data_1.csv')).toBeTruthy(); // still listed: it is still on disk
  });

  it('shows the server’s refusal and reloads the real state', async () => {
    const backend = fakeBackend(standard());
    backend.refuse(409, 'Upload upload_000000000002 is used by project x; saved data is never deleted here.');
    globalThis.fetch = backend.fetchMock as unknown as typeof fetch;
    render(<App />);
    openStorage();
    await screen.findByRole('heading', { name: /Local Storage/ });

    fireEvent.click(screen.getByRole('button', { name: 'Delete data_2.csv' }));
    fireEvent.click(within(dialog()).getByRole('button', { name: 'Delete permanently' }));
    await screen.findByText(/saved data is never deleted here/);
    expect(screen.getByText('data_2.csv')).toBeTruthy();
  });

  it('clears the generated files of a project only after confirmation and says the project stays', async () => {
    const backend = fakeBackend(standard());
    globalThis.fetch = backend.fetchMock as unknown as typeof fetch;
    render(<App />);
    openStorage();
    await screen.findByRole('heading', { name: /Local Storage/ });

    fireEvent.click(screen.getByRole('button', { name: 'Clear generated files of my_project' }));
    expect(within(dialog()).getByText(/5 generated files/)).toBeTruthy();
    expect(within(dialog()).getByText(/The project and its settings stay/)).toBeTruthy();
    expect(backend.cleanupCalls()).toHaveLength(0);
    fireEvent.click(within(dialog()).getByRole('button', { name: 'Delete permanently' }));
    await waitFor(() => expect(backend.cleanupCalls()).toHaveLength(1));
    expect(backend.cleanupCalls()[0].body).toEqual({ scope: 'project_artifacts', target_id: 'my_project', confirm: true });
    await waitFor(() => expect(screen.queryByRole('button', { name: 'Clear generated files of my_project' })).toBeNull());
  });
});
