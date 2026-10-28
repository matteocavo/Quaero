import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { expect, test, type Page } from '@playwright/test';
import { stopProcessTree } from './processes';

/**
 * The local data lifecycle against the real stack: a real FastAPI process, real files on disk, the real
 * Python CLI that the launcher runs at exit, and a crash (the API process is killed, not stopped).
 */
const REPO_ROOT = resolve(import.meta.dirname, '..', '..');
const FRONTEND_DIR = resolve(import.meta.dirname, '..');
const VENV_PYTHON = join(REPO_ROOT, '.venv', 'Scripts', 'python.exe');
const PYTHON_EXE = existsSync(VENV_PYTHON) ? VENV_PYTHON : 'python';

const API_PORT = 8124;
const WEB_PORT = 3124;
const KEY = 'e2e-storage-ephemeral-key-135792468';
const CSV = (tag: string) => `region,revenue\nNorth,${tag}0\nSouth,${tag}5\nEast,${tag}9\n`;

async function waitForHttp(url: string, timeoutMs = 25_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {
      // retry
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

test.describe('Local data lifecycle (real FastAPI, real files, real crash)', () => {
  test.use({ viewport: { width: 1280, height: 900 } });
  test.describe.configure({ mode: 'serial' });

  let workspace: string;
  let apiProc: ChildProcess | null = null;
  let viteProc: ChildProcess | null = null;

  const env = (session: string) => ({
    ...process.env,
    QUAERO_API_KEY: KEY,
    QUAERO_SESSION_ID: session,
    QUAERO_PROJECT_ROOT: workspace,
    QUAERO_DB_PATH: join(workspace, 'e2e_storage.db'),
    QUAERO_API_URL: `http://127.0.0.1:${API_PORT}`,
    QUAERO_CORS_ORIGINS: `http://127.0.0.1:${WEB_PORT},http://localhost:${WEB_PORT}`,
  });

  const startApi = async (session: string) => {
    apiProc = spawn(PYTHON_EXE, ['-m', 'uvicorn', 'api.main:app', '--host', '127.0.0.1', '--port', String(API_PORT)], {
      cwd: REPO_ROOT,
      env: env(session),
      stdio: 'ignore',
    });
    await waitForHttp(`http://127.0.0.1:${API_PORT}/health`);
  };

  /** A crash: the process is terminated at once, so no shutdown hook and no exit cleanup can run. */
  const crashApi = async () => {
    apiProc?.kill('SIGKILL');
    await new Promise((r) => setTimeout(r, 800));
    apiProc = null;
  };

  const api = (path: string, init: RequestInit = {}) =>
    fetch(`http://127.0.0.1:${API_PORT}${path}`, { ...init, headers: { 'X-API-Key': KEY, ...(init.headers ?? {}) } });

  const upload = async (name: string, csv: string): Promise<string> => {
    const form = new FormData();
    form.append('file', new Blob([csv], { type: 'text/csv' }), name);
    const res = await api('/uploads', { method: 'POST', body: form });
    expect(res.status).toBe(201);
    return ((await res.json()) as { upload_id: string }).upload_id;
  };

  const onDisk = (uploadId: string) => ({
    csv: existsSync(join(workspace, 'uploads', `${uploadId}.csv`)),
    meta: existsSync(join(workspace, 'uploads', `${uploadId}.json`)),
  });

  /** What the launcher runs after it stops the server. */
  const cliExit = (session: string, stdin: string) =>
    spawnSync(PYTHON_EXE, ['-m', 'api.lifecycle_cli', 'exit'], {
      cwd: REPO_ROOT,
      env: env(session),
      input: stdin,
      encoding: 'utf-8',
      timeout: 60_000,
    });

  const openStorage = async (page: Page) => {
    await page.goto(`http://127.0.0.1:${WEB_PORT}/`, { waitUntil: 'networkidle' });
    await page.getByTitle('Configure environment connection').click();
    await page.getByRole('button', { name: /Local Operations/i }).click();
    await expect(page.getByText(/Connected to local FastAPI backend/i)).toBeVisible();
    await page.getByRole('button', { name: 'Done' }).click();
    await page.locator('aside nav button', { hasText: 'Local Storage' }).click();
    await expect(page.getByRole('heading', { name: /Local Storage/ })).toBeVisible();
  };

  test.beforeAll(async () => {
    workspace = mkdtempSync(join(tmpdir(), 'quaero-storage-e2e-'));
    await startApi('session_e2e_first');
    const npxCmd = process.platform === 'win32' ? 'npx.cmd' : 'npx';
    viteProc = spawn(npxCmd, ['vite', '--host', '127.0.0.1', '--port', String(WEB_PORT), '--strictPort'], {
      cwd: FRONTEND_DIR,
      env: env('session_e2e_first'),
      stdio: 'ignore',
      shell: process.platform === 'win32',
    });
    await waitForHttp(`http://127.0.0.1:${WEB_PORT}/`);
  });

  test.afterAll(async () => {
    stopProcessTree(viteProc);
    stopProcessTree(apiProc);
    try {
      rmSync(workspace, { recursive: true, force: true });
    } catch {
      // Windows file lock race on cleanup
    }
  });

  let temporary = '';
  let saved = '';

  test('shows temporary and saved data, protects the saved upload, and deletes only after confirmation', async ({ page }) => {
    temporary = await upload('temporary_sales.csv', CSV('1'));
    saved = await upload('kept_sales.csv', CSV('2'));
    const made = await api('/projects', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ project: 'kept_project', upload_id: saved, question: 'Which region earns the most?' }),
    });
    expect(made.status).toBe(201);

    await openStorage(page);
    const temp = page.locator('section', { has: page.getByRole('heading', { name: 'Temporary uploads', exact: true }) });
    await expect(temp).toContainText('temporary_sales.csv');
    await expect(temp).toContainText('this session');
    await expect(temp).not.toContainText('kept_sales.csv'); // a project uses it
    const projects = page.locator('section', { has: page.getByRole('heading', { name: 'Saved projects', exact: true }) });
    await expect(projects).toContainText('kept_project');
    await expect(projects).toContainText('kept_sales.csv');
    await expect(page.getByRole('button', { name: /Delete kept_sales/ })).toHaveCount(0);

    // asking is not deleting
    await page.getByRole('button', { name: 'Delete temporary_sales.csv' }).click();
    await expect(page.getByRole('dialog')).toContainText('temporary_sales.csv');
    expect(onDisk(temporary)).toEqual({ csv: true, meta: true });
    await page.getByRole('button', { name: 'Cancel' }).click();
    expect(onDisk(temporary)).toEqual({ csv: true, meta: true });

    // the server refuses to delete what a project uses, even when asked directly
    const refused = await api('/storage/cleanup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ scope: 'single_upload', target_id: saved, confirm: true }),
    });
    expect(refused.status).toBe(409);
    expect(onDisk(saved)).toEqual({ csv: true, meta: true });

    // and without confirmation it does nothing
    const unconfirmed = await api('/storage/cleanup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ scope: 'current_session', confirm: false }),
    });
    expect(unconfirmed.status).toBe(422);
    expect(onDisk(temporary)).toEqual({ csv: true, meta: true });

    // the confirmed deletion removes the temporary upload from disk and nothing else
    await page.getByRole('button', { name: 'Delete temporary_sales.csv' }).click();
    await page.getByRole('button', { name: 'Delete permanently' }).click();
    await expect(page.getByText(`Deleted ${temporary}`)).toBeVisible();
    expect(onDisk(temporary)).toEqual({ csv: false, meta: false });
    expect(onDisk(saved)).toEqual({ csv: true, meta: true });
    expect(existsSync(join(workspace, 'projects', 'kept_project', 'project_config.json'))).toBe(true);
  });

  test('the cleanup policy is saved on the server and defaults to ask on exit', async ({ page }) => {
    const policyFile = join(workspace, 'uploads', '_lifecycle.json');
    expect(existsSync(policyFile)).toBe(false); // never chosen: the default applies
    await openStorage(page);
    await expect(page.getByRole('radio', { name: /Ask on exit/ })).toBeChecked();
    await page.getByRole('radio', { name: /Keep files/ }).check();
    await expect.poll(() => (existsSync(policyFile) ? JSON.parse(readFileSync(policyFile, 'utf-8')).lifecycle_policy : null)).toBe('keep_files');
    await page.getByRole('radio', { name: /Ask on exit/ }).check();
    await expect.poll(() => JSON.parse(readFileSync(policyFile, 'utf-8')).lifecycle_policy).toBe('ask_on_exit');
  });

  let leftover = '';

  test('after a crash the new session lists the leftover, never deletes it by itself, and deletes it on request', async ({ page }) => {
    leftover = await upload('left_behind.csv', CSV('3'));
    await crashApi(); // killed: nothing got to clean up
    expect(onDisk(leftover)).toEqual({ csv: true, meta: true });

    await startApi('session_e2e_second');
    // the launcher's exit step of the NEW session: with no terminal to ask, it keeps everything
    const exit = cliExit('session_e2e_second', '');
    expect(exit.status).toBe(0);
    expect(onDisk(leftover)).toEqual({ csv: true, meta: true });

    await openStorage(page);
    await expect(page.getByText(/1 temporary upload from an earlier session is still on disk/).first()).toBeVisible();
    const temp = page.locator('section', { has: page.getByRole('heading', { name: 'Temporary uploads', exact: true }) });
    await expect(temp).toContainText('left_behind.csv');
    await expect(temp).toContainText('earlier session');

    await page.getByRole('button', { name: /Review and delete/ }).click();
    await expect(page.getByRole('dialog')).toContainText('left_behind.csv');
    expect(onDisk(leftover)).toEqual({ csv: true, meta: true });
    await page.getByRole('button', { name: 'Delete permanently' }).click();
    await expect(page.getByText(`Deleted ${leftover}`)).toBeVisible();
    expect(onDisk(leftover)).toEqual({ csv: false, meta: false });
    expect(onDisk(saved)).toEqual({ csv: true, meta: true }); // still untouched
  });

  test('on exit: ask_on_exit asks, auto_cleanup removes only this session’s temporary uploads, keep_files keeps all', async () => {
    const earlier = await upload('from_second_session.csv', CSV('4'));
    await crashApi();
    await startApi('session_e2e_third');
    const mine = await upload('from_third_session.csv', CSV('5'));

    // ask_on_exit (the default), a person answers "n": nothing is deleted
    const no = cliExit('session_e2e_third', 'n\n');
    expect(no.status).toBe(0);
    expect(onDisk(mine)).toEqual({ csv: true, meta: true });

    // keep_files: nothing is deleted
    expect((await api('/storage/policy', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lifecycle_policy: 'keep_files' }) })).status).toBe(200);
    cliExit('session_e2e_third', 'y\n');
    expect(onDisk(mine)).toEqual({ csv: true, meta: true });

    // auto_cleanup: this session's temporary upload goes; the earlier session's and the saved one stay
    expect((await api('/storage/policy', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lifecycle_policy: 'auto_cleanup' }) })).status).toBe(200);
    const auto = cliExit('session_e2e_third', '');
    expect(auto.status).toBe(0);
    expect(onDisk(mine)).toEqual({ csv: false, meta: false });
    expect(onDisk(earlier)).toEqual({ csv: true, meta: true });
    expect(onDisk(saved)).toEqual({ csv: true, meta: true });
    expect(existsSync(join(workspace, 'projects', 'kept_project', 'project_config.json'))).toBe(true);
  });
});
