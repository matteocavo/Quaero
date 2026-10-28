import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';

/**
 * A CSV the app has never seen: built here, so the expected answers are computed independently of the
 * app's own code. 400 clinic visits, ~7% of the costs missing, one value with a quote and a comma.
 */
const DEPARTMENTS = ['Cardiology', 'Oncology', 'Pediatrics', 'Neurology'];
const CHANNELS = ['walk-in', 'referral', 'emergency'];
const visits = Array.from({ length: 400 }, (_, i) => {
  const id = i + 1;
  return {
    id,
    department: DEPARTMENTS[(id * 7) % 4],
    channel: CHANNELS[(id * 5) % 3],
    cost: id % 14 === 0 ? null : Math.round(((id * 37) % 900 + 40 + (id % 100) / 100) * 100) / 100,
    date: `2024-${String((id % 12) + 1).padStart(2, '0')}-${String((id % 28) + 1).padStart(2, '0')}`,
    physician: id % 7 === 0 ? 'Bianchi, "Gigi"' : 'Rossi',
  };
});
const csvQuote = (v: string) => (/[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v);
const CSV = [
  'visit_id,department,channel,cost_eur,visit_date,physician',
  ...visits.map((v) => [v.id, v.department, v.channel, v.cost ?? '', v.date, csvQuote(v.physician)].join(',')),
].join('\n');

const referrals = visits.filter((v) => v.channel === 'referral');
const sumByDepartment = (rows: typeof visits) =>
  Object.fromEntries(
    DEPARTMENTS.map((d) => [
      d,
      Math.round(rows.filter((v) => v.department === d && v.cost !== null).reduce((s, v) => s + (v.cost as number), 0) * 100) / 100,
    ]),
  );

function watch(page: Page) {
  const problems: string[] = [];
  const requests: string[] = [];
  page.on('console', (m) => {
    if (m.type() === 'error' || m.type() === 'warning') problems.push(`${m.type()}: ${m.text()}`);
  });
  page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));
  page.on('request', (r) => requests.push(r.url()));
  return { problems, requests };
}

async function openImport(page: Page, mobile: boolean) {
  if (mobile) await page.getByRole('button', { name: 'Open navigation menu' }).click();
  await page.locator('aside nav button', { hasText: 'Import & Profile Data' }).click();
}

async function expectNoHorizontalScroll(page: Page, where: string) {
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  expect(overflow, `horizontal overflow on ${where}`).toBeLessThanOrEqual(1);
}

async function workflow(page: Page, mobile: boolean) {
  const { problems, requests } = watch(page);
  await page.goto('./', { waitUntil: 'networkidle' });
  await openImport(page, mobile);

  // 1. upload and go straight to the workspace
  await page.locator('input[type=file]').setInputFiles({ name: 'clinic_visits.csv', mimeType: 'text/csv', buffer: Buffer.from(CSV) });
  await page.getByRole('button', { name: 'Analyze This Dataset' }).first().click();
  await expect(page.getByRole('heading', { name: 'clinic_visits.csv' })).toBeVisible();
  await expect(page.getByText('400 rows · 6 columns · your file')).toBeVisible();
  await expectNoHorizontalScroll(page, 'Data Preview');

  // 2. the data quality checks are still there
  await page.getByRole('tab', { name: 'Data Quality' }).click();
  await expect(page.getByRole('tabpanel')).toContainText(/missing/i);

  // 3. filter: referrals only
  await page.getByRole('tab', { name: 'Explore & Filter' }).click();
  await page.locator('#new-filter-column').selectOption('channel');
  await page.getByRole('button', { name: 'Add filter' }).click();
  await page.getByRole('checkbox', { name: /referral/ }).check();
  await expect(page.getByText('Rows that match').locator('xpath=following-sibling::*[1]')).toHaveText(String(referrals.length));
  await expectNoHorizontalScroll(page, 'Explore & Filter');

  // 4. aggregate: total cost per department
  await page.getByRole('tab', { name: 'Build Analysis' }).click();
  await page.getByRole('button', { name: 'Add grouping' }).click();
  await page.getByLabel('Group 1: column', { exact: true }).selectOption('department');
  await page.getByLabel('Calculation 1', { exact: true }).selectOption('sum');
  await page.getByLabel('of column', { exact: true }).selectOption('cost_eur');
  await expectNoHorizontalScroll(page, 'Build Analysis');
  await page.getByRole('button', { name: 'Run analysis' }).click();

  // 5. the table holds exactly the independently computed sums, and the chart is drawn
  const expected = sumByDepartment(referrals);
  const rows = await page.getByRole('table').locator('tbody tr').evaluateAll((trs) =>
    trs.map((tr) => [...tr.querySelectorAll('td')].map((td) => td.textContent ?? '')),
  );
  const shown = Object.fromEntries(rows.map(([name, value]) => [name, Number(value.replace(/,/g, ''))]));
  expect(shown).toEqual(expected);
  await expect(page.getByRole('img', { name: /Sum of cost_eur by department/ })).toBeVisible();
  await expectNoHorizontalScroll(page, 'Results & Charts');

  // 6. export: aggregated CSV, filtered rows, JSON report
  await page.getByRole('tab', { name: 'Export' }).click();
  await expectNoHorizontalScroll(page, 'Export');
  const download = async (button: RegExp) => {
    const [d] = await Promise.all([page.waitForEvent('download'), page.getByRole('button', { name: button }).click()]);
    return { name: d.suggestedFilename(), text: readFileSync((await d.path()) as string, 'utf8') };
  };

  const aggregated = await download(/Download results/);
  expect(aggregated.name).toBe('clinic_visits_analysis.csv');
  const lines = aggregated.text.split('\r\n');
  expect(lines[0]).toBe('department,sum(cost_eur)');
  expect(Object.fromEntries(lines.slice(1).map((l) => l.split(',')).map(([k, v]) => [k, Number(v)]))).toEqual(expected);
  expect(aggregated.text).not.toMatch(/\d\.\d{9,}/); // no floating-point noise

  const filtered = await download(/Download rows/);
  expect(filtered.name).toBe('clinic_visits_filtered.csv');
  expect(filtered.text.split('\r\n')).toHaveLength(referrals.length + 1);
  expect(filtered.text).not.toContain(',walk-in,');
  expect(filtered.text).toContain('"Bianchi, ""Gigi"""'); // awkward values survive the round trip

  const report = JSON.parse((await download(/Download report/)).text);
  expect(report.dataset).toMatchObject({ file_name: 'clinic_visits.csv', origin: 'upload', rows: 400 });
  expect(report.analysis.filters).toHaveLength(1);

  // 7. nothing left the browser and nothing was broken
  const foreign = requests.filter((u) => !u.startsWith('http://127.0.0.1:') && !u.startsWith('blob:') && !u.startsWith('data:'));
  expect(foreign).toEqual([]);
  expect(requests.filter((u) => u.includes('/api'))).toEqual([]);
  expect(problems).toEqual([]);
}

test.describe('CSV analysis workflow on desktop', () => {
  test.use({ viewport: { width: 1280, height: 900 } });
  test('upload -> filter -> aggregate -> chart -> export', async ({ page }) => {
    await workflow(page, false);
  });
});

test.describe('CSV analysis workflow on a phone', () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });
  test('upload -> filter -> aggregate -> chart -> export at 390 px', async ({ page }) => {
    await workflow(page, true);
  });
});
