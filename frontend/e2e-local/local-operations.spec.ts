import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { expect, test } from '@playwright/test';

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
    if (viteProc && !viteProc.killed) {
      viteProc.kill();
    }
    if (apiProc && !apiProc.killed) {
      apiProc.kill();
    }
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
  });
});
