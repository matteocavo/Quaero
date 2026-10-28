import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';

type User = ReturnType<typeof userEvent.setup>;

/** The example from the brief: listeners and saves are zero everywhere; streams is the real measure. */
const YEARS = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];
const STREAMS = [1200, 3400, 2800, 5100, 4300, 6100, 5800, 7200, 6900, 250000];
const SONGS = [
  'song,listeners,streams,saves,release_date',
  ...YEARS.map((y, i) => `Song ${String.fromCharCode(65 + i)},0,${STREAMS[i]},0,${y}-03-1${i % 9}`),
].join('\n');

const fetchMock = vi.fn();
beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal('fetch', fetchMock);
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const sidebar = () => document.querySelector('aside') as HTMLElement;
const nav = (label: string) => within(sidebar()).getByRole('button', { name: new RegExp(label) });
const tab = (name: string) => screen.getByRole('tab', { name });

async function openWorkspace(user: User, csv = SONGS, name = 'songs.csv') {
  await user.click(nav('Import & Profile Data'));
  await user.upload(screen.getByLabelText(/Click to upload your CSV/), new File([csv], name, { type: 'text/csv' }));
  await user.click((await screen.findAllByRole('button', { name: 'Analyze This Dataset' }))[0]);
  await screen.findByRole('tablist', { name: 'Analysis workspace' });
}

const suggestions = () => screen.getAllByTestId('suggestion');
const titleOf = (el: HTMLElement) => within(el).getByRole('heading').textContent;

describe('suggested analyses in the Browser Workspace', () => {
  it('proposes the analyses the data supports, with reasons, the exact calculation and the limits', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Build Analysis'));

    expect(suggestions().map(titleOf)).toEqual([
      'streams per year of release_date',
      'Top 10 song by streams',
      'Distribution of streams',
      'Unusual values in streams',
    ]);
    const first = suggestions()[0];
    expect(within(first).getByText(/streams is a numeric column: 10 values, from 1,200 to 250,000, average 29,280/)).toBeInTheDocument();
    expect(within(first).getByText('Sum of streams by release_date (per year).')).toBeInTheDocument();
    expect(within(first).getByText(/A period with more rows usually has a larger sum/)).toBeInTheDocument();
    expect(within(first).getByText('Why')).toBeInTheDocument();
    expect(within(first).getByText('What is calculated')).toBeInTheDocument();
    expect(within(first).getByText('Limits')).toBeInTheDocument();
  });

  it('says it is not AI and not the Python pipeline, and offers nothing about the constant columns', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Build Analysis'));

    expect(screen.getByText(/No AI is used and the Python pipeline is not involved/)).toBeInTheDocument();
    expect(screen.getAllByTestId('suggestions')[0].textContent).not.toMatch(/listeners|saves/);
    expect(screen.getByText(/Left out of the suggestions: listeners, saves/)).toBeInTheDocument();
    expect(screen.queryByText(/confidence|score|likely|probab/i, { selector: '[data-testid=suggestion] *' })).toBeNull();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('applies a suggestion in one click: the query builder is filled, it runs, and the real numbers show', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Build Analysis'));
    await user.click(screen.getByRole('button', { name: 'Run this analysis: streams per year of release_date' }));

    // lands on the results, with the calculation described and the actual table
    expect(await screen.findByText('Sum of streams by release_date (per year).')).toBeInTheDocument();
    const rows = within(screen.getByRole('table'))
      .getAllByRole('row')
      .slice(1)
      .map((r) => within(r).getAllByRole('cell').map((c) => c.textContent));
    expect(rows).toEqual(YEARS.map((y, i) => [String(y), STREAMS[i].toLocaleString('en-US')]));

    // the same analysis is in the builder, ready to change
    await user.click(tab('Build Analysis'));
    expect(screen.getAllByText('Sum of streams by release_date (per year).').length).toBeGreaterThan(0);
  });

  it('ranks the top songs, with the right figures', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Build Analysis'));
    await user.click(screen.getByRole('button', { name: 'Run this analysis: Top 10 song by streams' }));
    await screen.findByText(/top 10/);
    const first = within(screen.getByRole('table')).getAllByRole('row')[1];
    expect(within(first).getAllByRole('cell').map((c) => c.textContent)).toEqual(['Song J', '250,000']);
  });

  it('keeps the filters the user already set, and says so', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Explore & Filter'));
    await user.selectOptions(screen.getByLabelText('Column', { selector: '#new-filter-column' }), 'streams');
    await user.click(screen.getByRole('button', { name: 'Add filter' }));
    await user.type(screen.getByLabelText('Minimum'), '3000');

    await user.click(tab('Build Analysis'));
    expect(screen.getByText(/Your 1 filter stays applied when you run a suggestion/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Run this analysis: streams per year of release_date' }));
    expect(await screen.findByText(/, where streams/)).toBeInTheDocument();
    const years = within(screen.getByRole('table')).getAllByRole('row').slice(1).map((r) => within(r).getAllByRole('cell')[0].textContent);
    expect(years).toEqual(['2016', '2018', '2019', '2020', '2021', '2022', '2023', '2024']); // 2015 (1,200) and 2017 (2,800) are below 3,000
  });

  it('opens the distribution and shows the unusual value with its thresholds', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Build Analysis'));
    await user.click(screen.getByRole('button', { name: 'Show the unusual values: Unusual values in streams' }));

    expect(screen.getByRole('tab', { name: 'Explore & Filter', selected: true })).toBeInTheDocument();
    expect((screen.getByLabelText('Column', { selector: '#distribution-column' }) as HTMLSelectElement).value).toBe('streams');
    expect(screen.getByTestId('unusual-values').textContent).toMatch(/Unusual values: 0 below .* and 1 above .* \(1\.5 x the interquartile range, from 3,400 to 6,900\)\. Unusual is not the same as wrong\./);
  });

  it('points to the suggestions from the first screen', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    expect(screen.getByText(/4 suggested analyses are ready for this data/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'See the suggestions' }));
    expect(screen.getByRole('tab', { name: 'Build Analysis', selected: true })).toBeInTheDocument();
  });

  it('explains in Data Quality why a column is left out', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Data Quality'));
    const health = within(screen.getByTestId('column-health'));
    expect(health.getAllByText('Every value is 0, so it cannot tell rows apart. Not used for suggestions.')).toHaveLength(2);
    expect(health.getByText('listeners')).toBeInTheDocument();
    expect(health.getByText('saves')).toBeInTheDocument();
    expect(health.queryByText('streams')).toBeNull();
  });

  it('does not pick a constant column for the ready-made patterns either', async () => {
    const user = userEvent.setup();
    render(<App />);
    const csv = ['region,listeners,streams', 'N,0,10', 'S,0,20', 'N,0,30', 'S,0,40'].join('\n');
    await openWorkspace(user, csv, 'regional.csv');
    await user.click(tab('Build Analysis'));
    const compare = screen.getByRole('button', { name: /Compare a value across groups/ });
    expect(within(compare).getByText('Average of streams for each region')).toBeInTheDocument();
  });

  it('a different dataset gets its own suggestions, and an unusable one says why', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(nav('Import & Profile Data'));
    await user.upload(screen.getByLabelText(/Click to upload your CSV/), new File(['a,b\n1,x\n1,x\n1,x\n'], 'flat.csv', { type: 'text/csv' }));
    await user.click((await screen.findAllByRole('button', { name: 'Analyze This Dataset' }))[0]);
    await user.click(tab('Build Analysis'));
    expect(screen.queryAllByTestId('suggestion')).toHaveLength(0);
    expect(screen.getByText('Every column is empty, constant, mostly missing or a key, so there is nothing to analyze.')).toBeInTheDocument();
  });
});

describe('suggested setup for a Python pipeline run (Local Operations)', () => {
  const UPLOAD = {
    upload_id: 'upload_abc123def456',
    original_filename: 'songs.csv',
    size_bytes: 900,
    row_count: 10,
    column_count: 5,
    delimiter: ',',
    columns: [
      { name: 'song', normalized_name: 'song', dtype: 'string', null_count: 0, unique_count: 10, sample_values: ['Song A'] },
      { name: 'listeners', normalized_name: 'listeners', dtype: 'integer', null_count: 0, unique_count: 1, sample_values: ['0'] },
      { name: 'streams', normalized_name: 'streams', dtype: 'integer', null_count: 0, unique_count: 10, sample_values: ['1200'] },
      { name: 'saves', normalized_name: 'saves', dtype: 'integer', null_count: 0, unique_count: 1, sample_values: ['0'] },
      { name: 'release_date', normalized_name: 'release_date', dtype: 'datetime', null_count: 0, unique_count: 10, sample_values: ['2015-03-10'] },
    ],
    // the server lists every numeric column here, including the constant ones
    numeric_columns: ['listeners', 'streams', 'saves'],
    candidate_dimensions: ['song', 'release_date'],
    candidate_time_columns: ['release_date'],
    created_at: '2026-10-28T10:00:00Z',
    session_id: 'session_test',
  };

  it('picks the measure that varies, explains the constants, and can restore its choice', async () => {
    globalThis.fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const method = (init?.method || 'GET').toUpperCase();
      const json = (data: unknown, status = 200) => new Response(JSON.stringify(data), { status });
      if (url === '/api/health') return json({ status: 'ok', version: '1.0.0' });
      if (url === '/api/projects') return json({ projects: [] });
      if (url === '/api/uploads' && method === 'GET') return json({ uploads: [UPLOAD] });
      if (url === '/api/storage') return json({ detail: 'n/a' }, 404);
      return json({ detail: 'Not mocked' }, 404);
    }) as unknown as typeof fetch;

    render(<App />);
    fireEvent.click(screen.getByTitle(/Configure environment connection/i));
    fireEvent.click(screen.getByRole('button', { name: /Local Operations/i }));
    fireEvent.click(screen.getAllByRole('button', { name: /Import & Profile Data/i })[0]);
    await screen.findByTestId('local-pipeline-wizard');

    const metric = (await screen.findByTestId('local-metric-select')) as HTMLSelectElement;
    await waitFor(() => expect(metric.value).toBe('streams')); // not listeners, the first numeric column
    expect((screen.getByTestId('local-dimension-select') as HTMLSelectElement).value).toBe('song');
    expect((screen.getByTestId('local-time-select') as HTMLSelectElement).value).toBe('release_date');

    const advice = within(screen.getByTestId('setup-advice'));
    expect(advice.getByText(/not by the Python pipeline/)).toBeInTheDocument();
    expect(advice.getAllByText(/Every value is 0, so it cannot tell rows apart/)).toHaveLength(2);

    fireEvent.change(metric, { target: { value: 'listeners' } });
    expect(metric.value).toBe('listeners');
    fireEvent.click(screen.getByTestId('use-suggested-setup'));
    expect(metric.value).toBe('streams');
  });
});
