import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';

const SCREENS = [
  'Get Started & Workflow',
  'Import & Profile Data',
  'Overview Dashboard',
  'Project Explorer',
  'Pipeline Operations',
  'Mart Explorer',
  'Quality & Anomalies',
  'Data Lineage DAG',
];

/** Collects everything a user could never see but that signals a broken page. */
function watch(page: Page) {
  const problems: string[] = [];
  const requests: string[] = [];
  page.on('console', (m) => {
    if (m.type() === 'error' || m.type() === 'warning') problems.push(`${m.type()}: ${m.text()}`);
  });
  page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));
  page.on('requestfailed', (r) => problems.push(`requestfailed: ${r.url()}`));
  page.on('response', (r) => {
    if (r.status() >= 400) problems.push(`HTTP ${r.status()}: ${r.url()}`);
  });
  page.on('request', (r) => requests.push(r.url()));
  return { problems, requests };
}

const nav = (page: Page, label: string) => page.locator('aside nav button', { hasText: label });

async function openScreen(page: Page, label: string, mobile: boolean) {
  if (mobile) {
    const menu = page.getByRole('button', { name: 'Open navigation menu' });
    if ((await menu.getAttribute('aria-expanded')) !== 'true') await menu.click();
  }
  await nav(page, label).click();
}

test.describe('public demo on desktop', () => {
  test.use({ viewport: { width: 1280, height: 900 } });

  test('loads under /Quaero/ with all assets and a clean console', async ({ page }) => {
    const { problems } = watch(page);
    await page.goto('./', { waitUntil: 'networkidle' });
    expect(new URL(page.url()).pathname).toBe('/Quaero/');
    await expect(page).toHaveTitle('Quaero Web');
    await expect(page.locator('h1').first()).toContainText('Question-Driven Analytics Pipeline');
    await expect(page.getByText('Public Demo Sandbox')).toBeVisible();
    expect(problems).toEqual([]);
  });

  test('every screen shows real content', async ({ page }) => {
    const { problems } = watch(page);
    await page.goto('./', { waitUntil: 'networkidle' });
    for (const label of SCREENS) {
      await openScreen(page, label, false);
      const text = await page.locator('main').innerText();
      expect(text.length, label).toBeGreaterThan(200);
      expect(text, label).not.toMatch(/undefined|NaN|\[object Object\]/);
    }
    expect(problems).toEqual([]);
  });

  test('Local Operations is unavailable on the public site and sends no API request', async ({
    page,
  }) => {
    const { requests, problems } = watch(page);
    await page.goto('./', { waitUntil: 'networkidle' });
    await page.getByTitle('Click to view environment settings').click();

    const local = page.getByRole('button', { name: /Local Operations/ });
    await expect(local).toBeDisabled();
    await expect(page.getByText(/not available on this public site/i).first()).toBeVisible();
    await local.click({ force: true });
    await page.waitForTimeout(500);

    await expect(page.getByText('Public Demo Sandbox')).toBeVisible();
    expect(requests.filter((url) => url.includes('/api/'))).toEqual([]);
    expect(problems).toEqual([]);
  });

  test('a CSV with awkward values is imported intact and exported as valid CSV', async ({ page }) => {
    await page.goto('./', { waitUntil: 'networkidle' });
    await openScreen(page, 'Import & Profile Data', false);
    const source =
      'id,note,amount\n1,"said ""hi"", ok",3\n2,"line1\nline2",4\n3,=SUM(A1:A2),\n';
    await page.locator('input[type=file]').setInputFiles({
      name: 'tricky.csv',
      mimeType: 'text/csv',
      buffer: Buffer.from(source),
    });

    await expect(page.getByText(/Created the Browser workspace for tricky\.csv \(3 rows, 3 columns\)/)).toBeVisible();

    const [download] = await Promise.all([
      page.waitForEvent('download'),
      page.getByRole('button', { name: 'CSV', exact: true }).click(),
    ]);
    expect(download.suggestedFilename()).toBe('cleaned_tricky.csv');
    const exported = readFileSync((await download.path()) as string, 'utf8');
    expect(exported).toBe(
      ['id,note,amount', '1,"said ""hi"", ok",3', '2,"line1\nline2",4', "3,'=SUM(A1:A2),"].join(
        '\r\n',
      ),
    );
  });

  test('marts without a bundled preview say so instead of showing invented data', async ({
    page,
  }) => {
    await page.goto('./', { waitUntil: 'networkidle' });
    await openScreen(page, 'Mart Explorer', false);
    const martSelect = page.locator('main select').first();

    await martSelect.selectOption('mart_release_impact');
    await expect(page.locator('main table tbody tr').first()).toBeVisible();

    await martSelect.selectOption('mart_top_entities');
    await expect(page.getByText("No preview is bundled for 'mart_top_entities'")).toBeVisible();
    await expect(page.locator('main table')).toHaveCount(0);
    expect(await page.locator('main').innerText()).not.toContain('metric_value');
  });

  test('never contacts a third party', async ({ page }) => {
    const { requests } = watch(page);
    await page.goto('./', { waitUntil: 'networkidle' });
    for (const label of SCREENS) await openScreen(page, label, false);
    const outside = requests.filter(
      (url) => !url.startsWith('http://127.0.0.1:3000/') && !/^(data|blob):/.test(url),
    );
    expect(outside).toEqual([]);
  });
});

test.describe('public demo on a phone', () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });

  test('fits the screen and the menu reaches every screen', async ({ page }) => {
    const { problems } = watch(page);
    await page.goto('./', { waitUntil: 'networkidle' });

    const overflows = () =>
      page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1);
    expect(await overflows()).toBe(false);

    // The navigation is a slide-over menu, so the content gets the whole width.
    const mainBox = await page.locator('main').boundingBox();
    expect(mainBox?.width ?? 0).toBeGreaterThan(330);

    // The header must fit: every control inside the viewport and none on top of another.
    const header = page.locator('header');
    expect(await header.evaluate((el) => el.scrollWidth <= el.clientWidth + 1)).toBe(true);
    const boxes: { x: number; y: number; width: number; height: number }[] = [];
    for (const control of await header.locator('button:visible, a:visible').all()) {
      const box = await control.boundingBox();
      expect(box, 'header control has no box').not.toBeNull();
      boxes.push(box!);
    }
    expect(boxes.length).toBeGreaterThanOrEqual(3);
    boxes.sort((a, b) => a.x - b.x);
    boxes.forEach((box, index) => {
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(390 + 1);
      if (index > 0) expect(box.x).toBeGreaterThanOrEqual(boxes[index - 1].x + boxes[index - 1].width - 1);
    });

    // The project selector lives in the menu on phones, and it changes what the screens show.
    await page.getByRole('button', { name: 'Open navigation menu' }).click();
    await page.locator('#project-select-menu').selectOption('global_economic_indicators');
    await nav(page, 'Mart Explorer').click();
    const marts = await page.locator('main select').first().locator('option').allInnerTexts();
    expect(marts.map((m) => m.trim())).toContain('mart_gdp_trend');
    expect(marts.map((m) => m.trim())).not.toContain('mart_release_impact');

    for (const label of SCREENS) {
      await openScreen(page, label, true);
      await expect(page.getByRole('button', { name: 'Open navigation menu' })).toHaveAttribute(
        'aria-expanded',
        'false',
      );
      const text = await page.locator('main').innerText();
      expect(text.length, label).toBeGreaterThan(200);
      expect(await overflows(), `${label} overflows horizontally`).toBe(false);
    }
    expect(problems).toEqual([]);
  });
});
