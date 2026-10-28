import { act, cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import {
  DEFAULT_THEME,
  LEGACY_THEME_KEY,
  THEME_KEY,
  migrateLegacyTheme,
  readThemePreference,
  resolveTheme,
  systemPrefersDark,
} from './utils/theme';

type Listener = (event: { matches: boolean }) => void;

/** A controllable stand-in for the device's colour scheme (jsdom has none). */
function deviceScheme(initialDark: boolean) {
  let dark = initialDark;
  const listeners = new Set<Listener>();
  vi.stubGlobal('matchMedia', (query: string) => ({
    get matches() {
      return query.includes('dark') ? dark : !dark;
    },
    media: query,
    addEventListener: (_: string, listener: Listener) => listeners.add(listener),
    removeEventListener: (_: string, listener: Listener) => listeners.delete(listener),
  }));
  return {
    change(next: boolean) {
      dark = next;
      listeners.forEach((l) => l({ matches: dark }));
    },
    listenerCount: () => listeners.size,
  };
}

const isDark = () => document.documentElement.classList.contains('dark');
const radio = (name: string) => screen.getByRole('radio', { name });

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new Error('no network in the theme tests'))));
  document.documentElement.classList.remove('dark');
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  document.documentElement.classList.remove('dark');
});

describe('theme rules', () => {
  it('Dark is the default and only a valid stored choice overrides it', () => {
    expect(DEFAULT_THEME).toBe('dark');
    expect(readThemePreference()).toBe('dark');
    for (const stored of ['light', 'system', 'dark'] as const) {
      localStorage.setItem(THEME_KEY, stored);
      expect(readThemePreference()).toBe(stored);
    }
    for (const junk of ['', 'blue', 'LIGHT', 'null', '1']) {
      localStorage.setItem(THEME_KEY, junk);
      expect(readThemePreference()).toBe('dark');
    }
  });

  it('system resolves to what the device asks for, and an unknown device falls back to the default', () => {
    expect(resolveTheme('system', true)).toBe('dark');
    expect(resolveTheme('system', false)).toBe('light');
    expect(resolveTheme('light', true)).toBe('light');
    expect(resolveTheme('dark', false)).toBe('dark');
    expect(systemPrefersDark()).toBe(true); // jsdom has no matchMedia
    deviceScheme(false);
    expect(systemPrefersDark()).toBe(false);
  });

  it('a stored light from the old key is dropped (it was written for everyone), a stored dark is kept as a choice', () => {
    localStorage.setItem(LEGACY_THEME_KEY, 'light');
    migrateLegacyTheme();
    expect(localStorage.getItem(THEME_KEY)).toBeNull();
    expect(localStorage.getItem(LEGACY_THEME_KEY)).toBeNull();
    expect(readThemePreference()).toBe('dark');

    localStorage.setItem(LEGACY_THEME_KEY, 'dark');
    migrateLegacyTheme();
    expect(localStorage.getItem(THEME_KEY)).toBe('dark');
    expect(localStorage.getItem(LEGACY_THEME_KEY)).toBeNull();
  });

  it('never overwrites a choice made with the new key when the old one is still there', () => {
    localStorage.setItem(THEME_KEY, 'light');
    localStorage.setItem(LEGACY_THEME_KEY, 'dark');
    migrateLegacyTheme();
    expect(localStorage.getItem(THEME_KEY)).toBe('light');
    expect(localStorage.getItem(LEGACY_THEME_KEY)).toBeNull();
  });

  it('blocked storage never breaks the page', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('blocked');
    });
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('blocked');
    });
    expect(readThemePreference()).toBe('dark');
    expect(() => migrateLegacyTheme()).not.toThrow();
  });
});

describe('theme in the app', () => {
  it('a new user gets Dark, with Dark selected, and nothing is stored', () => {
    render(<App />);
    expect(isDark()).toBe(true);
    expect(radio('Dark').getAttribute('aria-checked')).toBe('true');
    expect(radio('Light').getAttribute('aria-checked')).toBe('false');
    expect(radio('System').getAttribute('aria-checked')).toBe('false');
    expect(localStorage.length).toBe(0);
  });

  it('Light and System are available, and a choice is remembered for the next visit', async () => {
    const user = userEvent.setup();
    const first = render(<App />);
    await user.click(radio('Light'));
    expect(isDark()).toBe(false);
    expect(localStorage.getItem(THEME_KEY)).toBe('light');
    first.unmount();

    document.documentElement.classList.remove('dark');
    render(<App />); // the next visit
    expect(isDark()).toBe(false);
    expect(radio('Light').getAttribute('aria-checked')).toBe('true');
  });

  it('System follows the device while it is selected, and stops following when another choice is made', async () => {
    const device = deviceScheme(false);
    const user = userEvent.setup();
    render(<App />);
    expect(isDark()).toBe(true); // the default, before any choice

    await user.click(radio('System'));
    expect(localStorage.getItem(THEME_KEY)).toBe('system');
    expect(isDark()).toBe(false); // the device is light
    act(() => device.change(true));
    expect(isDark()).toBe(true); // and follows when it changes
    act(() => device.change(false));
    expect(isDark()).toBe(false);

    await user.click(radio('Dark'));
    expect(device.listenerCount()).toBe(0); // no longer listening
    act(() => device.change(false));
    expect(isDark()).toBe(true);
  });

  it('the old key is migrated when the app starts: light is ignored, dark becomes an explicit choice', () => {
    localStorage.setItem(LEGACY_THEME_KEY, 'light');
    const light = render(<App />);
    expect(isDark()).toBe(true);
    expect(localStorage.getItem(LEGACY_THEME_KEY)).toBeNull();
    expect(localStorage.getItem(THEME_KEY)).toBeNull();
    light.unmount();

    localStorage.setItem(LEGACY_THEME_KEY, 'dark');
    render(<App />);
    expect(isDark()).toBe(true);
    expect(localStorage.getItem(THEME_KEY)).toBe('dark');
    expect(localStorage.getItem(LEGACY_THEME_KEY)).toBeNull();
  });

  it('a damaged stored value falls back to Dark instead of failing', () => {
    localStorage.setItem(THEME_KEY, '{not valid');
    render(<App />);
    expect(isDark()).toBe(true);
    expect(radio('Dark').getAttribute('aria-checked')).toBe('true');
  });

  it('the theme choice is the only thing stored, and it is a keyboard-reachable radio group', async () => {
    const user = userEvent.setup();
    render(<App />);
    expect(screen.getByRole('radiogroup', { name: 'Theme' })).toBeTruthy();
    radio('Light').focus();
    await user.keyboard('{Enter}');
    expect(Object.keys(localStorage)).toEqual([THEME_KEY]);
  });
});
