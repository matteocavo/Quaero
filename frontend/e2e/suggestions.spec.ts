import { expect, test, type Page } from '@playwright/test';

/** The example CSV: listeners and saves are zero everywhere; streams is the real measure. */
const YEARS = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];
const STREAMS = [1200, 3400, 2800, 5100, 4300, 6100, 5800, 7200, 6900, 250000];
const CSV = [
  'song,listeners,streams,saves,release_date',
  ...YEARS.map((y, i) => `Song ${String.fromCharCode(65 + i)},0,${STREAMS[i]},0,${y}-03-1${i % 9}`),
].join('\n');

async function openScreen(page: Page, label: string, mobile: boolean) {
  if (mobile) {
    const menu = page.getByRole('button', { name: 'Open navigation menu' });
    if ((await menu.getAttribute('aria-expanded')) !== 'true') await menu.click();
  }
  await page.locator('aside nav button', { hasText: label }).click();
}

async function expectNoHorizontalScroll(page: Page, where: string) {
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  expect(overflow, `horizontal overflow on ${where}`).toBeLessThanOrEqual(1);
}

async function workflow(page: Page, mobile: boolean) {
  const problems: string[] = [];
  const requests: string[] = [];
  page.on('console', (m) => {
    if (m.type() === 'error' || m.type() === 'warning') problems.push(`${m.type()}: ${m.text()}`);
  });
  page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));
  page.on('request', (r) => requests.push(r.url()));

  await page.goto('./', { waitUntil: 'networkidle' });
  await openScreen(page, 'Import & Profile Data', mobile);
  await page.locator('input[type=file]').setInputFiles({ name: 'songs.csv', mimeType: 'text/csv', buffer: Buffer.from(CSV) });
  await page.getByRole('button', { name: 'Analyze This Dataset' }).first().click();
  await expect(page.getByText('4 suggested analyses are ready for this data.')).toBeVisible();

  // suggestions, with their reasons
  await page.getByRole('button', { name: 'See the suggestions' }).click();
  const titles = page.getByTestId('suggestion').getByRole('heading');
  await expect(titles).toHaveText(['streams per year of release_date', 'Top 10 song by streams', 'Distribution of streams', 'Unusual values in streams']);
  await expect(page.getByTestId('suggestions')).not.toContainText(/listeners|saves/);
  await expect(page.getByText('streams is a numeric column: 10 values, from 1,200 to 250,000, average 29,280').first()).toBeVisible();
  await expect(page.getByText('Sum of streams by release_date (per year).').first()).toBeVisible();
  await expectNoHorizontalScroll(page, 'suggestions');

  // one click applies it and shows the real numbers
  await page.getByRole('button', { name: 'Run this analysis: streams per year of release_date' }).click();
  const rows = await page.getByRole('table').locator('tbody tr').evaluateAll((trs) =>
    trs.map((tr) => [...tr.querySelectorAll('td')].map((td) => td.textContent ?? '')),
  );
  expect(rows).toEqual(YEARS.map((y, i) => [String(y), STREAMS[i].toLocaleString('en-US')]));
  await expectNoHorizontalScroll(page, 'results');

  // the unusual value, with its thresholds
  await page.getByRole('tab', { name: 'Build Analysis' }).click();
  await page.getByRole('button', { name: 'Show the unusual values: Unusual values in streams' }).click();
  await expect(page.getByTestId('unusual-values')).toContainText('0 below -1,850 and 1 above 12,150');
  await expectNoHorizontalScroll(page, 'distribution');

  // the constant columns are explained
  await page.getByRole('tab', { name: 'Data Quality' }).click();
  await expect(page.getByTestId('column-health').getByText('Every value is 0, so it cannot tell rows apart. Not used for suggestions.')).toHaveCount(2);
  await expectNoHorizontalScroll(page, 'data quality');

  const foreign = requests.filter((u) => !u.startsWith('http://127.0.0.1:') && !u.startsWith('blob:') && !u.startsWith('data:'));
  expect(foreign).toEqual([]);
  expect(requests.filter((u) => u.includes('/api'))).toEqual([]);
  expect(problems).toEqual([]);
}

test.describe('suggested analyses on desktop', () => {
  test.use({ viewport: { width: 1280, height: 900 } });
  test('upload -> suggestions -> apply -> distribution -> constant columns explained', async ({ page }) => {
    await workflow(page, false);
  });
});

test.describe('suggested analyses on a phone', () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });
  test('the same at 390 px', async ({ page }) => {
    await workflow(page, true);
  });
});
