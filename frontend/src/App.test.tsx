import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import App from './App';

const fetchMock = vi.fn();

beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal('fetch', fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

const json = (body: unknown, status = 200) =>
  Promise.resolve(
    new Response(JSON.stringify(body), {
      status,
      headers: { 'Content-Type': 'application/json' },
    }),
  );

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

const nav = (label: string) =>
  within(document.querySelector('aside') as HTMLElement).getByRole('button', {
    name: new RegExp(label),
  });

async function switchToLive(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByTitle('Click to view environment settings'));
  await user.click(screen.getByRole('button', { name: /Local Operations/ }));
}

describe('Public Demo is the default and is fully offline', () => {
  it('starts in Public Demo on the Get Started screen', async () => {
    render(<App />);
    expect(await screen.findByText('Public Demo Sandbox')).toBeInTheDocument();
    expect(screen.queryByText('FastAPI Connected')).not.toBeInTheDocument();
    expect(screen.getByText(/Get Started/i, { selector: 'button *, h1, h2, span' })).toBeTruthy();
  });

  it('opens every screen without a single network request', async () => {
    const user = userEvent.setup();
    render(<App />);
    for (const label of SCREENS) {
      await user.click(nav(label));
      await waitFor(() => expect(document.querySelector('main')).not.toBeEmptyDOMElement());
    }
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('lists the demo projects in the project selector', async () => {
    render(<App />);
    const select = await screen.findByRole('combobox');
    await waitFor(() =>
      expect(within(select).getByRole('option', { name: /^release_impact \(/ })).toBeInTheDocument(),
    );
    expect(
      within(select).getByRole('option', { name: /^global_economic_indicators \(/ }),
    ).toBeInTheDocument();
  });

  it('explains that pipeline runs need the local backend and sends nothing', async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(nav('Pipeline Operations'));
    const run = await screen.findByRole('button', { name: /Run Pipeline \(Demo Mode Disabled\)/ });
    await user.click(run);
    expect(await screen.findByText(/requires running the local Python FastAPI engine/)).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('loads a sample CSV in the browser only', async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(nav('Import & Profile Data'));
    await user.click(await screen.findByRole('button', { name: /Sample: global_indicators\.csv/ }));
    expect(await screen.findByText(/Nothing is uploaded/)).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('persists only the theme in localStorage', async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(await screen.findByTitle(/Switch to dark mode/));
    expect(window.localStorage.getItem('quaero_theme')).toBe('dark');
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(Object.keys(window.localStorage)).toEqual(['quaero_theme']);
  });
});

describe('Local Operations mode', () => {
  it('shows the offline banner and no demo data when the backend is unreachable', async () => {
    fetchMock.mockImplementation(() => Promise.reject(new TypeError('Failed to fetch')));
    const user = userEvent.setup();
    render(<App />);
    await switchToLive(user);

    expect(await screen.findByText('FastAPI Backend Offline:')).toBeInTheDocument();
    await waitFor(() => expect(screen.getByText('FastAPI Offline')).toBeInTheDocument());
    const select = screen.getByRole('combobox');
    expect(within(select).queryAllByRole('option')).toHaveLength(0);
    expect(document.body.textContent).not.toContain('global_economic_indicators');
  });

  it('uses only the same-origin proxy and never sends credentials', async () => {
    fetchMock.mockImplementation((url: string) => {
      if (url === '/api/health') return json({ status: 'ok', version: '1.0.0' });
      if (url === '/api/projects') {
        return json({ projects: [{ project: 'live_project', marts_count: 1, has_summary: true }] });
      }
      return json({ detail: 'not found' }, 404);
    });
    const user = userEvent.setup();
    render(<App />);
    await switchToLive(user);

    expect(await screen.findByText('FastAPI Connected')).toBeInTheDocument();
    const select = screen.getByRole('combobox');
    await waitFor(() =>
      expect(within(select).getByRole('option', { name: /^live_project \(/ })).toBeInTheDocument(),
    );
    expect(within(select).queryByRole('option', { name: /^release_impact \(/ })).not.toBeInTheDocument();

    for (const [url, init] of fetchMock.mock.calls) {
      expect(String(url).startsWith('/api/')).toBe(true);
      const names = Object.keys((init?.headers ?? {}) as Record<string, string>).map((h) =>
        h.toLowerCase(),
      );
      expect(names).not.toContain('x-api-key');
    }
  });

  it('does not show demo metrics or config for a live project', async () => {
    fetchMock.mockImplementation((url: string) => {
      if (url === '/api/health') return json({ status: 'ok', version: '1.0.0' });
      if (url === '/api/projects') {
        return json({ projects: [{ project: 'live_project', marts_count: 0, has_summary: false }] });
      }
      return json({ detail: 'Project not found' }, 404);
    });
    const user = userEvent.setup();
    render(<App />);
    await switchToLive(user);
    await screen.findByText('FastAPI Connected');
    await user.click(screen.getByRole('button', { name: /Close|Done|×/ }).closest('button') as HTMLElement).catch(
      () => undefined,
    );
    await user.click(nav('Project Explorer'));
    await user.click(await screen.findByRole('button', { name: /Config/i }));
    expect(await screen.findByText(/is not exposed by the Quaero API yet/)).toBeInTheDocument();
    expect(screen.queryByText(/mart_question_answer/)).not.toBeInTheDocument();
  });

  it('starts a run through POST /api/projects/{project}/run', async () => {
    fetchMock.mockImplementation((url: string, init?: RequestInit) => {
      if (url === '/api/health') return json({ status: 'ok', version: '1.0.0' });
      if (url === '/api/projects') {
        return json({ projects: [{ project: 'live_project', marts_count: 0, has_summary: false }] });
      }
      if (url.startsWith('/api/projects/live_project/runs')) {
        return json({ project: 'live_project', runs: [], total: 0 });
      }
      if (url === '/api/projects/live_project/run' && init?.method === 'POST') {
        return json({ run_id: 'run-xyz', project: 'live_project', status: 'QUEUED', message: 'ok' }, 202);
      }
      return json({ detail: 'not found' }, 404);
    });
    const user = userEvent.setup();
    render(<App />);
    await switchToLive(user);
    await screen.findByText('FastAPI Connected');
    await act(async () => {
      document.querySelector<HTMLElement>('[class*="fixed inset-0"]')?.click();
    });
    await user.click(nav('Pipeline Operations'));
    await user.type(await screen.findByLabelText(/Business Analytics Question/), 'Which year is best?');
    await user.type(screen.getByLabelText(/Source Dataset URL/), 'https://example.com/d.csv');
    await user.click(screen.getByRole('button', { name: /^Run Pipeline$/ }));

    expect(await screen.findByText(/Run accepted & queued: run-xyz/)).toBeInTheDocument();
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === 'POST');
    expect(post?.[0]).toBe('/api/projects/live_project/run');
    expect(JSON.parse(post?.[1].body)).toEqual({
      question: 'Which year is best?',
      source_url: 'https://example.com/d.csv',
    });
  });
});
