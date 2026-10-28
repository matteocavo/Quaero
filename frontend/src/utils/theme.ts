/**
 * Theme preference: Dark by default, Light and System on request.
 *
 * Only a choice the user makes is stored. Nothing is written on the first visit, so the default can change later
 * without being frozen in every browser that merely opened the app.
 */
export type ThemePreference = 'dark' | 'light' | 'system';

export const DEFAULT_THEME: ThemePreference = 'dark';
export const THEME_KEY = 'quaero_theme_preference';
/** The key used before Dark became the default. It was written on every first visit, whatever the user chose. */
export const LEGACY_THEME_KEY = 'quaero_theme';

const isPreference = (value: unknown): value is ThemePreference =>
  value === 'dark' || value === 'light' || value === 'system';

/** The stored choice, or the default. Storage can be blocked or full; that never breaks the page. */
export function readThemePreference(): ThemePreference {
  try {
    const stored = window.localStorage.getItem(THEME_KEY);
    return isPreference(stored) ? stored : DEFAULT_THEME;
  } catch {
    return DEFAULT_THEME;
  }
}

export function saveThemePreference(preference: ThemePreference): void {
  try {
    window.localStorage.setItem(THEME_KEY, preference);
  } catch {
    // The choice still applies to this visit; it just is not remembered.
  }
}

/**
 * Moves the old key away. A stored `dark` could only come from the user switching to it, so it is kept as an
 * explicit choice. A stored `light` was also written for every first-time visitor who chose nothing; it cannot be
 * told apart from a deliberate choice, so it is dropped and the default applies. Anyone who really wants Light
 * picks it once more.
 */
export function migrateLegacyTheme(): void {
  try {
    const legacy = window.localStorage.getItem(LEGACY_THEME_KEY);
    if (legacy === null) return;
    if (legacy === 'dark' && !isPreference(window.localStorage.getItem(THEME_KEY))) {
      window.localStorage.setItem(THEME_KEY, 'dark');
    }
    window.localStorage.removeItem(LEGACY_THEME_KEY);
  } catch {
    // Nothing to migrate if storage cannot be read.
  }
}

export const DARK_QUERY = '(prefers-color-scheme: dark)';

/** What the device asks for. Unknown (no matchMedia) falls back to the default theme. */
export function systemPrefersDark(): boolean {
  try {
    return window.matchMedia(DARK_QUERY).matches;
  } catch {
    return DEFAULT_THEME === 'dark';
  }
}

export function resolveTheme(preference: ThemePreference, prefersDark: boolean): 'light' | 'dark' {
  if (preference === 'system') return prefersDark ? 'dark' : 'light';
  return preference;
}
