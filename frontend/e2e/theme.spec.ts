import { expect, test, type Page } from '@playwright/test';

const isDark = (page: Page) => page.evaluate(() => document.documentElement.classList.contains('dark'));
const stored = (page: Page) => page.evaluate(() => JSON.stringify(Object.fromEntries(Object.entries(localStorage))));
const radio = (page: Page, name: string) => page.getByRole('radio', { name, exact: true });

/** Lightness 0 (black) to 1 (white) of a CSS colour as reported by the browser (rgb() or oklch()). */
function lightnessOf(color: string): number {
  const oklch = /oklch\(\s*([\d.]+)/.exec(color);
  if (oklch) return Number(oklch[1]);
  const rgb = /rgba?\(\s*(\d+)[,\s]+(\d+)[,\s]+(\d+)/.exec(color);
  if (rgb) return (0.2126 * Number(rgb[1]) + 0.7152 * Number(rgb[2]) + 0.0722 * Number(rgb[3])) / 255;
  throw new Error(`unrecognised colour: ${color}`);
}

async function expectNoHorizontalScroll(page: Page, where: string) {
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  expect(overflow, `horizontal overflow on ${where}`).toBeLessThanOrEqual(1);
}

for (const [name, viewport, mobile] of [
  ['desktop', { width: 1280, height: 900 }, false],
  ['a phone (390 px)', { width: 390, height: 844 }, true],
] as const) {
  test.describe(`theme on ${name}`, () => {
    test.use({ viewport, hasTouch: mobile, isMobile: mobile });

    test('a new user gets Dark with nothing stored, and the three choices fit on the screen', async ({ page }) => {
      await page.goto('./', { waitUntil: 'networkidle' });
      expect(await isDark(page)).toBe(true);
      await expect(radio(page, 'Dark')).toHaveAttribute('aria-checked', 'true');
      expect(await stored(page)).toBe('{}'); // opening the app is not a choice
      await expectNoHorizontalScroll(page, 'the header with the theme control');
      for (const label of ['Light', 'System', 'Dark']) {
        const box = await radio(page, label).boundingBox();
        expect(box, label).not.toBeNull();
        expect(box!.x + box!.width, `${label} inside the screen`).toBeLessThanOrEqual(viewport.width);
        expect(box!.x).toBeGreaterThanOrEqual(0);
      }
      // the page really is dark: the background is a dark colour, whatever format the browser reports it in
      const background = await page.evaluate(() => getComputedStyle(document.body).backgroundColor);
      expect(lightnessOf(background), background).toBeLessThan(0.25);
    });

    test('Light is remembered across a reload', async ({ page }) => {
      await page.goto('./', { waitUntil: 'networkidle' });
      await radio(page, 'Light').click();
      expect(await isDark(page)).toBe(false);
      expect(await stored(page)).toBe('{"quaero_theme_preference":"light"}');
      await page.reload({ waitUntil: 'networkidle' });
      expect(await isDark(page)).toBe(false);
      await expect(radio(page, 'Light')).toHaveAttribute('aria-checked', 'true');
    });
  });
}

test.describe('no flash of the wrong theme', () => {
  test.use({ viewport: { width: 1280, height: 900 } });

  test('the theme is applied before the app has loaded: with the app scripts blocked, Dark is already there', async ({ page }) => {
    await page.route(/\/assets\/.*\.js$/, (route) => route.abort());
    await page.goto('./', { waitUntil: 'domcontentloaded' });
    expect(await isDark(page)).toBe(true);
    await expect(page.locator('#root')).toBeEmpty(); // nothing from the app ran
  });

  test('a stored Light choice is also applied before the app loads', async ({ page }) => {
    await page.addInitScript(() => localStorage.setItem('quaero_theme_preference', 'light'));
    await page.route(/\/assets\/.*\.js$/, (route) => route.abort());
    await page.goto('./', { waitUntil: 'domcontentloaded' });
    expect(await isDark(page)).toBe(false);
  });
});

test.describe('System follows the device', () => {
  test.use({ viewport: { width: 1280, height: 900 }, colorScheme: 'light' });

  test('System is light on a light device, Dark stays dark, and System is remembered', async ({ page }) => {
    await page.goto('./', { waitUntil: 'networkidle' });
    expect(await isDark(page)).toBe(true); // the default does not look at the device
    await radio(page, 'System').click();
    expect(await isDark(page)).toBe(false);
    await page.emulateMedia({ colorScheme: 'dark' });
    expect(await isDark(page)).toBe(true); // follows the device live
    await page.emulateMedia({ colorScheme: 'light' });
    await page.reload({ waitUntil: 'networkidle' });
    expect(await isDark(page)).toBe(false); // and from the first paint after a reload
    await radio(page, 'Dark').click();
    expect(await isDark(page)).toBe(true);
  });
});

test.describe('someone who used the app before', () => {
  test.use({ viewport: { width: 1280, height: 900 } });

  test('the old key written automatically on the first visit does not keep them in Light', async ({ page }) => {
    await page.addInitScript(() => {
      if (!localStorage.getItem('migrated-once')) {
        localStorage.setItem('quaero_theme', 'light'); // what the previous version stored for everyone
        localStorage.setItem('migrated-once', '1');
      }
    });
    await page.goto('./', { waitUntil: 'networkidle' });
    expect(await isDark(page)).toBe(true);
    const keys = JSON.parse(await stored(page));
    expect(keys.quaero_theme).toBeUndefined(); // migrated away
    expect(keys.quaero_theme_preference).toBeUndefined(); // and nothing was invented for them
  });
});
