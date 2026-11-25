import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { copyFileSync, mkdtempSync, readFileSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { expect, test } from '@playwright/test';
import { stopProcessTree } from './processes';

const REPO_ROOT = resolve(import.meta.dirname, '..', '..');
const FRONTEND_DIR = resolve(import.meta.dirname, '..');
const VENV_PYTHON = join(REPO_ROOT, '.venv', 'Scripts', 'python.exe');
const PYTHON_EXE = existsSync(VENV_PYTHON) ? VENV_PYTHON : 'python';

const EXTERNAL_CSV_NAME = 'external_logistics_2025.csv';
const EXTERNAL_CSV_CONTENT = [
  'ship_year,carrier,route_zone,shipments,freight_cost',
  '2021,AtlasFreight,EU_North,45,12400.50',
  '2021,BalticLine,EU_South,38,9800.00',
  '2022,AtlasFreight,EU_North,52,14900.75',
  '2022,BalticLine,EU_South,41,10600.25',
  '2023,NordicCargo,EU_East,60,17800.00',
  '2023,AtlasFreight,EU_West,49,13900.50',
  '2024,NordicCargo,EU_North,68,20500.00',
  '2024,AtlasFreight,EU_East,310,195000.00',
].join('\n');

const REVIEW_CSV_NAME = 'review_orders_synthetic.csv';
// Synthetic data: 4 of 24 rows (16.7%) are extreme amounts, so the real pipeline reports a high-impact anomaly (R1).
const REVIEW_CSV_CONTENT = [
  'order_id,region,amount',
  ...Array.from({ length: 40 }, (_, i) => {
    const amount = i < 37 ? 100 + (i % 5) * 3 : 9000 + i * 100;
    return `${i + 1},${['North', 'South', 'East'][i % 3]},${amount}`;
  }),
].join('\n');

/** Read-only query against the isolated e2e SQLite DB through the project's Python (stdlib sqlite3). */
function queryDb(dbPath: string, sql: string): unknown[][] {
  const code =
    'import sqlite3,sys,json;c=sqlite3.connect("file:"+sys.argv[1]+"?mode=ro",uri=True);' +
    'print(json.dumps(c.execute(sys.argv[2]).fetchall()))';
  return JSON.parse(execFileSync(PYTHON_EXE, ['-c', code, dbPath, sql], { encoding: 'utf8' }));
}

async function waitForHttp(url: string, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) return;
    } catch {
      // retry
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

test.describe('Local Operations full-stack E2E (FastAPI + Python pipeline + React)', () => {
  test.use({ viewport: { width: 1280, height: 900 } });

  let tempWorkspace: string;
  let apiProc: ChildProcess | null = null;
  let viteProc: ChildProcess | null = null;
  const apiPort = 8123;
  const webPort = 3123;
  const sessionKey = 'e2e-local-ops-ephemeral-key-987654321';

  test.beforeAll(async () => {
    tempWorkspace = mkdtempSync(join(tmpdir(), 'quaero-local-e2e-'));
    // The review rules file is user-owned config read from the project root; give the temp workspace a copy.
    copyFileSync(join(REPO_ROOT, 'review_rules.json'), join(tempWorkspace, 'review_rules.json'));
    const env = {
      ...process.env,
      QUAERO_API_KEY: sessionKey,
      QUAERO_PROJECT_ROOT: tempWorkspace,
      QUAERO_DB_PATH: join(tempWorkspace, 'e2e_quaero.db'),
      QUAERO_API_URL: `http://127.0.0.1:${apiPort}`,
      QUAERO_CORS_ORIGINS: `http://127.0.0.1:${webPort},http://localhost:${webPort}`,
    };

    apiProc = spawn(
      PYTHON_EXE,
      ['-m', 'uvicorn', 'api.main:app', '--host', '127.0.0.1', '--port', String(apiPort)],
      { cwd: REPO_ROOT, env, stdio: 'ignore' },
    );
    await waitForHttp(`http://127.0.0.1:${apiPort}/health`);

    const npxCmd = process.platform === 'win32' ? 'npx.cmd' : 'npx';
    viteProc = spawn(
      npxCmd,
      ['vite', '--host', '127.0.0.1', '--port', String(webPort), '--strictPort'],
      { cwd: FRONTEND_DIR, env, stdio: 'ignore', shell: process.platform === 'win32' },
    );
    await waitForHttp(`http://127.0.0.1:${webPort}/`);
  });

  test.afterAll(async () => {
    stopProcessTree(viteProc);
    stopProcessTree(apiProc);
    if (tempWorkspace) {
      try {
        rmSync(tempWorkspace, { recursive: true, force: true });
      } catch {
        // ignore Windows file lock cleanup race
      }
    }
  });

  test('uploads external CSV, creates local project, runs Python pipeline, explores marts/DQ/anomalies/lineage, and downloads exports', async ({
    page,
  }) => {
    await page.goto(`http://127.0.0.1:${webPort}/`, { waitUntil: 'networkidle' });

    // 1. Switch to Local Operations mode
    await page.getByTitle('Configure environment connection').click();
    await page.getByRole('button', { name: /Local Operations/i }).click();
    await expect(page.getByText(/Connected to local FastAPI backend/i)).toBeVisible();
    await page.getByRole('button', { name: 'Done' }).click();

    // 2. Open Import & Profile Data screen and upload external CSV to FastAPI
    await page.locator('aside nav button', { hasText: 'Import & Profile Data' }).click();
    const wizard = page.getByTestId('local-pipeline-wizard');
    await expect(wizard).toBeVisible();

    await page.getByTestId('local-csv-upload-input').setInputFiles({
      name: EXTERNAL_CSV_NAME,
      mimeType: 'text/csv',
      buffer: Buffer.from(EXTERNAL_CSV_CONTENT, 'utf-8'),
    });

    await expect(page.getByTestId('upload-metadata-card')).toBeVisible();
    await expect(page.getByTestId('upload-metadata-card')).toContainText(EXTERNAL_CSV_NAME);

    // 3. Configure project & question, and launch the real Python pipeline
    await expect(page.getByTestId('local-project-name-input')).toHaveValue(
      'external_logistics_2025',
    );
    await page
      .getByTestId('local-project-question-input')
      .fill('Which carrier generates the highest average freight cost?');
    await page.getByTestId('local-metric-select').selectOption('freight_cost');
    await page.getByTestId('local-dimension-select').selectOption('carrier');

    // Running needs the person to have seen how the question is read
    await expect(page.getByTestId('create-and-run-btn')).toBeDisabled();
    await page.getByTestId('check-interpretation-btn').click();
    await expect(page.getByTestId('interpretation-panel')).toContainText(/average/i);
    await expect(page.getByTestId('interpretation-panel')).toContainText('freight_cost');
    await page.getByTestId('create-and-run-btn').click();

    // 4. Verify run completes and results panel appears in Pipeline Operations
    const resultsPanel = page.getByTestId('local-results-panel');
    await expect(resultsPanel).toBeVisible({ timeout: 25_000 });
    await expect(resultsPanel).toContainText('Marts Generated');

    // Verify artifacts exist on disk in the isolated local workspace
    const projectDir = join(tempWorkspace, 'projects', 'external_logistics_2025');
    expect(existsSync(join(projectDir, 'project_config.json'))).toBe(true);
    expect(existsSync(join(projectDir, 'marts', 'mart_question_answer.parquet'))).toBe(true);
    expect(
      existsSync(join(projectDir, 'metadata', 'dq_score_external_logistics_2025.json')),
    ).toBe(true);
    expect(
      existsSync(join(projectDir, 'metadata', 'anomaly_report_v2_external_logistics_2025.json')),
    ).toBe(true);
    expect(existsSync(join(projectDir, 'metadata', 'lineage.json'))).toBe(true);

    // 5. Download Project Summary JSON from OperationsView
    const [summaryDownload] = await Promise.all([
      page.waitForEvent('download'),
      page.getByTestId('download-summary-btn').click(),
    ]);
    expect(summaryDownload.suggestedFilename()).toBe(
      'external_logistics_2025_summary_report.json',
    );
    const summaryJson = JSON.parse(readFileSync((await summaryDownload.path()) as string, 'utf8'));
    expect(summaryJson.source).toBe('external_logistics_2025');
    expect(summaryJson.metric_column).toBe('freight_cost');
    expect(summaryJson.dimension_column).toBe('carrier');

    // 6. Explore Generated Marts & Download Mart CSV
    await page.getByTestId('goto-marts-btn').click();
    await expect(page.locator('main table tbody tr').first()).toBeVisible();
    await expect(page.locator('main')).toContainText('AtlasFreight');

    const [martCsvDownload] = await Promise.all([
      page.waitForEvent('download'),
      page.getByRole('button', { name: /Export Page \(CSV\)/i }).click(),
    ]);
    expect(martCsvDownload.suggestedFilename()).toContain('external_logistics_2025');
    const martCsvText = readFileSync((await martCsvDownload.path()) as string, 'utf8');
    expect(martCsvText).toContain('AtlasFreight');

    // 7. Inspect Data Quality & Anomalies
    await page.locator('aside nav button', { hasText: 'Quality & Anomalies' }).click();
    await expect(page.locator('main')).toContainText('freight_cost');
    await expect(page.locator('main')).toContainText('v2');

    // 8. View Data Lineage DAG
    await page.locator('aside nav button', { hasText: 'Data Lineage DAG' }).click();
    await expect(page.locator('main')).toContainText('mart_question_answer');

    // 9. Human Review: nothing is synced on open; an explicit sync creates items; a decision is recorded
    const reviewRequests: string[] = [];
    page.on('request', (r) => {
      if (r.url().includes('/review/')) reviewRequests.push(`${r.method()} ${new URL(r.url()).pathname}`);
    });
    await page.locator('aside nav button', { hasText: 'Human Review' }).click();
    await expect(page.getByRole('heading', { name: /Human Review/i }).first()).toBeVisible();
    await expect(page.getByRole('button', { name: /Sync Review/i })).toBeVisible();
    expect(reviewRequests.some((r) => r.startsWith('POST') && r.endsWith('/review/sync'))).toBe(false);
    await page.getByRole('button', { name: /Sync Review/i }).click();
    await expect
      .poll(() => reviewRequests.filter((r) => r.endsWith('/review/sync')).length)
      .toBeGreaterThan(0);
    await expect(page.getByRole('button', { name: /Sync Review/i })).toBeEnabled({ timeout: 60_000 });
    // The proxy must expose /review/* and the real backend must answer: a sync summary, no proxy error.
    const main = page.locator('main');
    await expect(main).toContainText(/Synced external_logistics_2025, run [0-9a-f-]+: \d+ created, \d+ already existing/);
    await expect(main).toContainText('Active rules: R1');
    await expect(main).not.toContainText('not exposed by the proxy');
    await expect(main).not.toContainText('rules_config_invalid');
    // This small CSV has no high-impact anomaly, so the queue is honestly empty (no fake items).
    await expect(main).toContainText('No review items yet.');
    expect(reviewRequests).toContain('GET /api/review/items');
  });

  test('human review end to end: real pipeline item, sync, reject, persisted event, history survives a reload', async ({
    page,
  }) => {
    await page.goto(`http://127.0.0.1:${webPort}/`, { waitUntil: 'networkidle' });
    await page.getByTitle('Configure environment connection').click();
    await page.getByRole('button', { name: /Local Operations/i }).click();
    await expect(page.getByText(/Connected to local FastAPI backend/i)).toBeVisible();
    await page.getByRole('button', { name: 'Done' }).click();
    await page.locator('aside nav button', { hasText: 'Import & Profile Data' }).click();
    await page.getByTestId('local-csv-upload-input').setInputFiles({
      name: REVIEW_CSV_NAME,
      mimeType: 'text/csv',
      buffer: Buffer.from(REVIEW_CSV_CONTENT, 'utf-8'),
    });
    await expect(page.getByTestId('upload-metadata-card')).toBeVisible();
    await page.getByTestId('local-project-question-input').fill('Which region has the highest average amount?');
    await page.getByTestId('local-metric-select').selectOption('amount');
    await page.getByTestId('local-dimension-select').selectOption('region');
    await page.getByTestId('check-interpretation-btn').click();
    await expect(page.getByTestId('interpretation-panel')).toContainText(/average/i);
    await page.getByTestId('create-and-run-btn').click();
    await expect(page.getByTestId('local-results-panel')).toBeVisible({ timeout: 25_000 });

    // Sync: items come from the real anomaly report, never inserted by the test
    await page.locator('aside nav button', { hasText: 'Human Review' }).click();
    await page.getByRole('button', { name: /Sync Review/i }).click();
    const main = page.locator('main');
    await expect(main).toContainText(/Synced review_orders_synthetic, run [0-9a-f-]+: [1-9]\d* created/, {
      timeout: 60_000,
    });
    const rows = page.getByTestId('review-row');
    await expect(rows.first()).toBeVisible();
    await expect(rows.first()).toContainText('Review required');

    // Detail, then reject with a reviewer and a reason
    await rows.first().click();
    const detail = page.getByTestId('review-detail');
    await expect(detail).toBeVisible();
    await expect(page.getByTestId('integrity-block')).toHaveCount(0);
    await detail.getByRole('button', { name: /^Reject$/ }).click();
    const dialog = page.getByRole('dialog');
    await expect(dialog).toBeVisible();
    await dialog.locator('#rv-reviewer').fill('E2E Reviewer');
    await dialog.locator('#rv-reason').fill('Synthetic outliers are expected in this test');
    await dialog.getByRole('button', { name: 'Confirm decision' }).click();
    await expect(dialog).toBeHidden({ timeout: 15_000 });
    await expect(rows.first()).toContainText('Rejected', { timeout: 15_000 });
    await expect(page.getByTestId('review-event').first()).toContainText('E2E Reviewer');

    // The event is persisted in the isolated SQLite DB
    const dbPath = join(tempWorkspace, 'e2e_quaero.db');
    const events = queryDb(
      dbPath,
      "SELECT from_status,to_status,reviewer,reason FROM review_events ORDER BY id",
    );
    expect(events).toEqual([
      ['REVIEW_REQUIRED', 'REJECTED', 'E2E Reviewer', 'Synthetic outliers are expected in this test'],
    ]);
    expect(queryDb(dbPath, "SELECT status FROM review_items WHERE status='REJECTED'").length).toBe(1);

    // A reload keeps the decision and its history
    await page.reload({ waitUntil: 'networkidle' });
    await page.getByTitle('Configure environment connection').click();
    await page.getByRole('button', { name: /Local Operations/i }).click();
    await expect(page.getByText(/Connected to local FastAPI backend/i)).toBeVisible();
    await page.getByRole('button', { name: 'Done' }).click();
    await page.locator('aside nav button', { hasText: 'Human Review' }).click();
    // After a reload the default project may differ (the workspace is shared across tests): pick ours.
    await page.locator('label:has(span:text-is("Project")) select').selectOption('review_orders_synthetic');
    await expect(page.getByTestId('review-row').first()).toContainText('Rejected', { timeout: 15_000 });
    await page.getByTestId('review-row').first().click();
    await expect(page.getByTestId('review-event').first()).toContainText('E2E Reviewer');
    await expect(page.getByTestId('review-event').first()).toContainText('Synthetic outliers are expected');
  });

  test('a question the pipeline cannot answer is explained and cannot be run; an explicit sum is honoured', async ({
    page,
  }) => {
    await page.goto(`http://127.0.0.1:${webPort}/`, { waitUntil: 'networkidle' });
    await page.getByTitle('Configure environment connection').click();
    await page.getByRole('button', { name: /Local Operations/i }).click();
    await expect(page.getByText(/Connected to local FastAPI backend/i)).toBeVisible();
    await page.getByRole('button', { name: 'Done' }).click();
    await page.locator('aside nav button', { hasText: 'Import & Profile Data' }).click();

    await page.getByTestId('local-csv-upload-input').setInputFiles({
      name: 'second_logistics.csv',
      mimeType: 'text/csv',
      buffer: Buffer.from(EXTERNAL_CSV_CONTENT, 'utf-8'),
    });
    await expect(page.getByTestId('upload-metadata-card')).toBeVisible();

    // 1. a median is not an available aggregation: it is refused, never turned into an average
    await page
      .getByTestId('local-project-question-input')
      .fill('What is the median freight cost per carrier?');
    await page.getByTestId('check-interpretation-btn').click();
    await expect(page.getByTestId('interpretation-panel')).toContainText(/median/i);
    await expect(page.getByTestId('create-and-run-btn')).toBeDisabled();

    // 2. asking for a total, with the aggregation chosen explicitly, runs exactly that
    await page
      .getByTestId('local-project-question-input')
      .fill('Which carrier has the most freight cost?');
    await page.getByTestId('local-metric-select').selectOption('freight_cost');
    await page.getByTestId('local-dimension-select').selectOption('carrier');
    await page.getByTestId('local-aggregation-select').selectOption('sum');
    await page.getByTestId('check-interpretation-btn').click();
    await expect(page.getByTestId('interpretation-panel')).toContainText(/sum|total/i);
    await page.getByTestId('create-and-run-btn').click();
    await expect(page.getByTestId('local-results-panel')).toBeVisible({ timeout: 25_000 });
  });
});
