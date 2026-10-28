import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';
import { CSV, referrals, sumByDepartment, visits } from './clinicVisits';

const WORKSPACE = '__browser_workspace__';
const MENU_SELECT = '#project-select-menu'; // always in the DOM; the header one is hidden on phones

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

async function setMenu(page: Page, open: boolean) {
  const menu = page.getByRole('button', { name: 'Open navigation menu' });
  if (((await menu.getAttribute('aria-expanded')) === 'true') !== open) await menu.click();
}

async function openScreen(page: Page, label: string, mobile: boolean) {
  if (mobile) await setMenu(page, true);
  await page.locator('aside nav button', { hasText: label }).click();
}

/** Pick a context in the selector the user can actually see (header on desktop, menu on a phone). */
async function chooseContext(page: Page, value: string, mobile: boolean) {
  if (mobile) {
    await setMenu(page, true);
    await page.locator(MENU_SELECT).selectOption(value);
    await setMenu(page, false);
  } else {
    await page.locator('#project-select').selectOption(value);
  }
}

async function expectNoHorizontalScroll(page: Page, where: string) {
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  expect(overflow, `horizontal overflow on ${where}`).toBeLessThanOrEqual(1);
}

const main = (page: Page) => page.locator('main');

async function workflow(page: Page, mobile: boolean) {
  const { problems, requests } = watch(page);
  await page.goto('./', { waitUntil: 'networkidle' });

  // 0. before any upload the selector holds only the demo projects
  await expect(page.locator(`${MENU_SELECT} option[value="${WORKSPACE}"]`)).toHaveCount(0);
  await expect(page.locator(MENU_SELECT)).toHaveValue('release_impact');

  // 1. upload -> the Browser Workspace exists and is selected
  await openScreen(page, 'Import & Profile Data', mobile);
  await page.locator('input[type=file]').setInputFiles({ name: 'clinic_visits.csv', mimeType: 'text/csv', buffer: Buffer.from(CSV) });
  await expect(page.getByText('Browser workspace created and selected')).toBeVisible();
  await expect(page.locator(MENU_SELECT)).toHaveValue(WORKSPACE);
  await expect(page.locator(`${MENU_SELECT} optgroup[label="Browser workspace (this tab only)"] option`)).toHaveText('Browser workspace: clinic_visits.csv');
  await expectNoHorizontalScroll(page, 'Import');

  // 2. Overview shows real numbers from the CSV and nothing from a demo project
  await openScreen(page, 'Overview Dashboard', mobile);
  await expect(page.getByRole('heading', { name: 'Overview: Browser Workspace' })).toBeVisible();
  const missingCost = visits.filter((v) => v.cost === null).length;
  const cells = visits.length * 6;
  const completeness = Math.round(((cells - missingCost) / cells) * 1000) / 10;
  await expect(main(page).getByText('Rows', { exact: true }).locator('xpath=following-sibling::*[1]')).toHaveText(String(visits.length));
  await expect(main(page).getByText('Columns', { exact: true }).locator('xpath=following-sibling::*[1]')).toHaveText('6');
  await expect(main(page).getByText('Completeness', { exact: true }).locator('xpath=following-sibling::*[1]')).toHaveText(`${completeness}%`);
  await expect(main(page).getByText(`${missingCost} missing cells`)).toBeVisible();
  await expect(main(page)).not.toContainText(/Registered Projects Repository|Pipeline Artifact Audit|mart_\w+/);
  await expect(main(page).getByText('Not available for a Browser Workspace')).toBeVisible();
  await expectNoHorizontalScroll(page, 'Overview');

  // 3. Project Explorer describes the same dataset
  await openScreen(page, 'Project Explorer', mobile);
  await expect(page.getByRole('heading', { name: 'Project Explorer: Browser Workspace' })).toBeVisible();
  await expect(main(page).getByText('File you selected on this device')).toBeVisible();
  await expect(main(page)).toContainText('cost_eur');
  await expectNoHorizontalScroll(page, 'Project Explorer');

  // 4. filter and aggregate in Analyze Dataset (same global state)
  await openScreen(page, 'Analyze Dataset', mobile);
  await page.getByRole('tab', { name: 'Explore & Filter' }).click();
  await page.locator('#new-filter-column').selectOption('channel');
  await page.getByRole('button', { name: 'Add filter' }).click();
  await page.getByRole('checkbox', { name: /referral/ }).check();
  await page.getByRole('tab', { name: 'Build Analysis' }).click();
  await page.getByRole('button', { name: 'Add grouping' }).click();
  await page.getByLabel('Group 1: column', { exact: true }).selectOption('department');
  await page.getByLabel('Calculation 1', { exact: true }).selectOption('sum');
  await page.getByLabel('of column', { exact: true }).selectOption('cost_eur');
  await page.getByRole('button', { name: 'Run analysis' }).click();
  await page.getByRole('table').waitFor();
  const expected = sumByDepartment(referrals);

  // 5. the Overview now reflects the filter and the analysis
  await openScreen(page, 'Overview Dashboard', mobile);
  await expect(main(page).getByText(`${referrals.length} of ${visits.length} rows match.`)).toBeVisible();
  await expect(main(page).getByText(/^Sum of cost_eur by department, where channel/)).toBeVisible();
  await expectNoHorizontalScroll(page, 'Overview with an analysis');

  // 6. a demo project, then back: no mixing, nothing lost
  await chooseContext(page, 'global_economic_indicators', mobile);
  await expect(page.locator(MENU_SELECT)).toHaveValue('global_economic_indicators');
  await openScreen(page, 'Overview Dashboard', mobile);
  await expect(main(page).getByText('Registered Projects Repository')).toBeVisible();
  await expect(main(page)).not.toContainText('clinic_visits.csv');
  await expect(page.locator('aside')).not.toContainText('BROWSER');
  await expectNoHorizontalScroll(page, 'demo Overview');

  await chooseContext(page, WORKSPACE, mobile);
  await openScreen(page, 'Mart Explorer', mobile);
  await expect(main(page).getByText(/Mart Explorer is not available for a Browser Workspace/)).toBeVisible();
  await expect(main(page)).not.toContainText(/mart_\w+/);
  await expectNoHorizontalScroll(page, 'Mart Explorer (unavailable)');

  // 7. the analysis is still there, and exports what was computed
  await openScreen(page, 'Analyze Dataset', mobile);
  await page.getByRole('tab', { name: 'Results & Charts' }).click();
  const rows = await page.getByRole('table').locator('tbody tr').evaluateAll((trs) =>
    trs.map((tr) => [...tr.querySelectorAll('td')].map((td) => td.textContent ?? '')),
  );
  expect(Object.fromEntries(rows.map(([k, v]) => [k, Number(v.replace(/,/g, ''))]))).toEqual(expected);
  await page.getByRole('tab', { name: 'Export' }).click();
  const [download] = await Promise.all([page.waitForEvent('download'), page.getByRole('button', { name: /Download results/ }).click()]);
  expect(download.suggestedFilename()).toBe('clinic_visits_analysis.csv');
  const lines = readFileSync((await download.path()) as string, 'utf8').split('\r\n');
  expect(Object.fromEntries(lines.slice(1).map((l) => l.split(',')).map(([k, v]) => [k, Number(v)]))).toEqual(expected);

  // 8. nothing left the browser, nothing was broken
  const foreign = requests.filter((u) => !u.startsWith('http://127.0.0.1:') && !u.startsWith('blob:') && !u.startsWith('data:'));
  expect(foreign).toEqual([]);
  expect(requests.filter((u) => u.includes('/api'))).toEqual([]);
  expect(problems).toEqual([]);

  // 9. a reload forgets the dataset: it was never stored
  await page.reload({ waitUntil: 'networkidle' });
  await expect(page.locator(`${MENU_SELECT} option[value="${WORKSPACE}"]`)).toHaveCount(0);
  await expect(page.locator(MENU_SELECT)).toHaveValue('release_impact');
}

test.describe('Browser Workspace on desktop', () => {
  test.use({ viewport: { width: 1280, height: 900 } });
  test('upload -> workspace in the selector -> overview -> explorer -> analysis -> demo -> workspace -> export', async ({ page }) => {
    await workflow(page, false);
  });
});

test.describe('Browser Workspace on a phone', () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });
  test('the same path at 390 px, with the selector in the menu', async ({ page }) => {
    await workflow(page, true);
  });
});
