import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import App from './App';
import { buildDataset } from './analysis/dataset';
import { summarizeDataset } from './analysis/summary';
import { MAX_FILE_BYTES } from './analysis/limits';
import { BROWSER_WORKSPACE_ID } from './context/AppContext';
import { parseCSV } from './utils/csvProfiler';

type User = ReturnType<typeof userEvent.setup>;

const SALES = [
  'order_id,region,product,units,price,order_date,returned',
  '1,North,Widget,3,9.99,2024-01-05,no',
  '2,South,Gadget,1,24.50,2024-01-06,yes',
  '3,North,Widget,,9.99,2024-02-11,no',
  '4,East,Gizmo,7,3.25,2024-02-12,no',
  '5,South,Widget,2,9.99,2024-03-01,yes',
  '6,North,Gizmo,5,3.25,2023-12-30,no',
].join('\n');
const LEAGUE = 'team,points\nred,10\nblue,4\nred,6\n';

const fetchMock = vi.fn();
beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal('fetch', fetchMock);
});
afterEach(() => {
  vi.unstubAllGlobals();
});

const sidebar = () => document.querySelector('aside') as HTMLElement;
const main = () => document.querySelector('main') as HTMLElement;
const nav = (label: string) => within(sidebar()).getByRole('button', { name: new RegExp(label) });
const tab = (name: string) => screen.getByRole('tab', { name });
const selector = () => screen.getByLabelText('Select Active Project') as HTMLSelectElement;

async function upload(user: User, csv: string, name: string) {
  await user.click(nav('Import & Profile Data'));
  await user.upload(screen.getByLabelText(/Click to upload your CSV/), new File([csv], name, { type: 'text/csv' }));
  await screen.findByText(/Browser workspace created and selected/);
}

const selectContext = (user: User, value: string) => user.selectOptions(selector(), value);

/** Filter region to North, and run "sum of units by region". */
async function setUpAnalysis(user: User) {
  await user.click(nav('Analyze Dataset'));
  await user.click(tab('Explore & Filter'));
  await user.selectOptions(screen.getByLabelText('Column', { selector: '#new-filter-column' }), 'region');
  await user.click(screen.getByRole('button', { name: 'Add filter' }));
  await user.click(screen.getByRole('checkbox', { name: /North/ }));
  await user.click(tab('Build Analysis'));
  await user.click(screen.getByRole('button', { name: 'Add grouping' }));
  await user.selectOptions(screen.getByLabelText('Calculation 1'), 'sum');
  await user.selectOptions(screen.getByLabelText('of column'), 'units');
  await user.click(screen.getByRole('button', { name: 'Run analysis' }));
  await screen.findByRole('table');
}

describe('summarizeDataset', () => {
  const summary = () => {
    const profile = parseCSV(SALES, 'sales.csv');
    return summarizeDataset(buildDataset(profile, 'upload', 1), profile);
  };

  it('counts what is in the file and nothing else', () => {
    const s = summary();
    expect(s).toMatchObject({ rows: 6, columns: 7, cells: 42, missingCells: 1, duplicateRows: 0 });
    expect(s.completenessPct).toBe(97.6);
    expect(s.kindCounts).toEqual({ numeric: 3, categorical: 2, date: 1, boolean: 1 });
  });

  it('counts a value that cannot be read as missing once, not twice', () => {
    const csv = ['day', '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-13-45'].join('\n');
    const profile = parseCSV(csv, 'days.csv');
    const dataset = buildDataset(profile, 'upload', 3);
    expect(dataset.columns[0]).toMatchObject({ nullCount: 1, invalidCount: 1 }); // one bad value, in both counters
    const s = summarizeDataset(dataset, profile);
    expect(s.missingCells).toBe(1);
    expect(s.completenessPct).toBe(83.3); // 5 of 6 cells
    expect(s.columnSummaries[0]).toMatchObject({ missing: 1, missingPct: 16.7 });
    expect(s.unreadable).toBe(1);
  });

  it('computes numeric statistics over the non-missing values', () => {
    const units = summary().columnSummaries.find((c) => c.name === 'units')!;
    expect(units).toMatchObject({ missing: 1, missingPct: 16.7, min: 1, max: 7, mean: 3.6 });
  });

  it('lists the most frequent values of text columns without counting missing as a value', () => {
    const region = summary().columnSummaries.find((c) => c.name === 'region')!;
    expect(region.top).toEqual([
      { value: 'North', count: 3 },
      { value: 'South', count: 2 },
      { value: 'East', count: 1 },
    ]);
    const withGaps = parseCSV('a,b\nx,1\n,2\n,3\ny,4\n', 'gaps.csv');
    const top = summarizeDataset(buildDataset(withGaps, 'upload', 2), withGaps).columnSummaries[0].top!;
    expect(top.map((t) => t.value)).toEqual(['x', 'y']);
  });
});

describe('the global selector', () => {
  it('lists only the demo projects until a dataset is loaded', async () => {
    render(<App />);
    await screen.findAllByRole('option', { name: /release_impact/ }); // header and mobile-menu selectors
    const values = within(selector()).getAllByRole('option').map((o) => (o as HTMLOptionElement).value);
    expect(values).toEqual(expect.arrayContaining(['release_impact']));
    expect(values).not.toContain(BROWSER_WORKSPACE_ID);
  });

  it('adds the Browser Workspace in its own group and selects it after an upload', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');

    expect(selector().value).toBe(BROWSER_WORKSPACE_ID);
    const groups = [...selector().querySelectorAll('optgroup')].map((g) => g.label);
    expect(groups).toEqual(['Demo projects', 'Browser workspace (this tab only)']);
    expect(within(selector()).getByRole('option', { name: 'Browser workspace: sales.csv' })).toBeInTheDocument();
    // the demo projects are still there and unchanged
    expect(within(selector()).getByRole('option', { name: /release_impact/ })).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

describe('Overview and Project Explorer of a Browser Workspace', () => {
  it('shows real statistics, the schema and the quality of the uploaded CSV, and no demo data', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await user.click(nav('Overview Dashboard'));

    const overview = within(main());
    expect(overview.getByRole('heading', { name: 'Overview: Browser Workspace' })).toBeInTheDocument();
    expect(overview.getByText('sales.csv')).toBeInTheDocument();
    expect(overview.getByText('Rows').nextElementSibling).toHaveTextContent('6');
    expect(overview.getByText('Columns').nextElementSibling).toHaveTextContent('7');
    expect(overview.getByText('3 numeric · 2 categorical · 1 date · 1 boolean')).toBeInTheDocument();
    expect(overview.getByText('Completeness').nextElementSibling).toHaveTextContent('97.6%');
    expect(overview.getByText('1 missing cell')).toBeInTheDocument();
    // schema lists the real columns
    for (const column of ['order_id', 'region', 'product', 'units', 'price', 'order_date', 'returned']) {
      expect(overview.getAllByText(column).length).toBeGreaterThan(0);
    }
    expect(overview.getByText(/'units' has 1 missing value/)).toBeInTheDocument();

    // nothing from the demo projects
    expect(main().textContent).not.toMatch(/Registered Projects|Pipeline Artifact Audit|COMPLETED|mart_release_impact|93\.3%/);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('says what a Browser Workspace does not have instead of inventing it', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await user.click(nav('Overview Dashboard'));

    const card = within(main()).getByText('Not available for a Browser Workspace').closest('section') as HTMLElement;
    for (const item of ['Parquet marts', 'Python lineage', 'Pipeline execution']) {
      expect(within(card).getByText(item).parentElement).toHaveTextContent(/not available in the static demo/);
    }
  });

  it('describes origin, columns, types and statistics in the Project Explorer', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await user.click(nav('Project Explorer'));

    const explorer = within(main());
    expect(explorer.getByRole('heading', { name: 'Project Explorer: Browser Workspace' })).toBeInTheDocument();
    expect(explorer.getByText('File you selected on this device')).toBeInTheDocument();
    expect(explorer.getByText(/comma-separated/)).toBeInTheDocument();
    expect(explorer.getByText(/Not saved, not uploaded/)).toBeInTheDocument();
    expect(explorer.getByText(/min 1 · mean 3\.6 · max 7/)).toBeInTheDocument(); // units
    expect(explorer.getByText(/North \(3\), South \(2\), East \(1\)/)).toBeInTheDocument(); // region
    expect(explorer.getByText('2023-12-30 to 2024-03-01')).toBeInTheDocument(); // order_date
    expect(explorer.getByText('No analysis has been run yet.')).toBeInTheDocument();
    expect(main().textContent).not.toMatch(/Semantic Metric Definitions|project_config/);
  });

  it('lists the analyses created and reopens one in the builder', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await setUpAnalysis(user);

    await user.click(nav('Project Explorer'));
    const item = within(main()).getByText(/^1\. Sum of units by region, where region/).closest('li') as HTMLElement;
    expect(item).toHaveTextContent('3 of 6 rows, 1 result row, 1 filter');

    await user.click(within(item).getByRole('button', { name: 'Open in Build Analysis' }));
    expect(screen.getByRole('tab', { name: 'Build Analysis', selected: true })).toBeInTheDocument();
    expect(screen.getByText(/^Sum of units by region, where region/)).toBeInTheDocument();
  });

  it('shows the filters and the latest analysis on the Overview', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await setUpAnalysis(user);

    await user.click(nav('Overview Dashboard'));
    const card = within(main()).getByText('Filters and analysis').closest('section') as HTMLElement;
    expect(within(card).getByText(/region/, { selector: 'li' })).toBeInTheDocument();
    expect(within(card).getByText('3 of 6 rows match.')).toBeInTheDocument();
    expect(within(card).getByText(/^Sum of units by region, where region/)).toBeInTheDocument();
    expect(within(card).getByText('North')).toBeInTheDocument();
  });
});

describe('screens that need the Python pipeline', () => {
  it.each([
    ['Pipeline Operations', 'Pipeline Operations'],
    ['Mart Explorer', 'Mart Explorer'],
    ['Quality & Anomalies', 'Quality & Anomalies'],
    ['Data Lineage DAG', 'Data Lineage DAG'],
  ])('%s explains itself and shows no demo data', async (label, heading) => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await user.click(nav(label));

    expect(within(main()).getByRole('heading', { name: heading })).toBeInTheDocument();
    expect(within(main()).getByText(new RegExp(`${heading} is not available for a Browser Workspace`))).toBeInTheDocument();
    expect(main().textContent).toContain('Parquet marts');
    expect(main().textContent).not.toMatch(/mart_\w+|global_economic/); // no demo mart or project data
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('points Quality & Anomalies to the dataset’s own Data Quality tab', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await user.click(nav('Quality & Anomalies'));
    await user.click(screen.getByRole('button', { name: 'Open Data Quality for this dataset' }));
    expect(screen.getByRole('tab', { name: 'Data Quality', selected: true })).toBeInTheDocument();
  });
});

describe('moving between the demo and the workspace', () => {
  it('never mixes the two, and gives the demo back unchanged', async () => {
    const user = userEvent.setup();
    render(<App />);

    // the demo before any upload
    await user.click(nav('Overview Dashboard'));
    const demoOverview = main().textContent;
    expect(demoOverview).toContain('Registered Projects Repository');

    await upload(user, SALES, 'sales.csv');
    await user.click(nav('Overview Dashboard'));
    expect(main().textContent).toContain('Overview: Browser Workspace');
    expect(main().textContent).not.toContain('Registered Projects Repository');

    // back to a demo project
    await selectContext(user, 'release_impact');
    expect(selector().value).toBe('release_impact');
    expect(main().textContent).toContain('Registered Projects Repository');
    expect(main().textContent).not.toMatch(/sales\.csv|Overview: Browser Workspace/);
    expect(sidebar().textContent).not.toContain('BROWSER');
    expect(main().textContent).toBe(demoOverview);

    // and to the other demo project, then the workspace again
    await selectContext(user, 'global_economic_indicators');
    expect(main().textContent).not.toContain('sales.csv');
    await selectContext(user, BROWSER_WORKSPACE_ID);
    expect(main().textContent).toContain('sales.csv');
    expect(main().textContent).not.toContain('Registered Projects Repository');
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('keeps the filters and analyses while another context is selected', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await setUpAnalysis(user);

    await selectContext(user, 'release_impact'); // leaves the analysis screen
    expect(screen.queryByRole('tablist', { name: 'Analysis workspace' })).not.toBeInTheDocument();
    expect(main().textContent).not.toContain('sales.csv');

    await user.click(nav('Analyze Dataset')); // opening it selects the workspace again
    expect(selector().value).toBe(BROWSER_WORKSPACE_ID);
    await user.click(tab('Explore & Filter'));
    expect(screen.getByText('Rows that match').nextElementSibling).toHaveTextContent('3');
    await user.click(tab('Results & Charts'));
    expect(screen.getByText(/^Sum of units by region, where region/)).toBeInTheDocument();
    const cells = within(screen.getByRole('table')).getAllByRole('cell').map((c) => c.textContent);
    expect(cells).toEqual(['North', '8']);
  });

  it('can still export after switching contexts', async () => {
    const user = userEvent.setup();
    const blobs = new Map<string, Blob>();
    const names: string[] = [];
    URL.createObjectURL = vi.fn((b: Blob) => {
      const url = `blob:mock/${blobs.size}`;
      blobs.set(url, b);
      return url;
    });
    URL.revokeObjectURL = vi.fn();
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (this: HTMLAnchorElement) {
      names.push(this.download);
    });
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await setUpAnalysis(user);
    await selectContext(user, 'release_impact');
    await selectContext(user, BROWSER_WORKSPACE_ID);
    await user.click(nav('Analyze Dataset'));
    await user.click(tab('Export'));
    await user.click(screen.getByRole('button', { name: /Download results/ }));
    expect(names).toEqual(['sales_analysis.csv']);
  });
});

describe('replacing, removing and bad files', () => {
  it('replaces the dataset and discards everything that belonged to the old one', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await setUpAnalysis(user);

    await upload(user, LEAGUE, 'league.csv');
    expect(selector().value).toBe(BROWSER_WORKSPACE_ID);
    expect(within(selector()).getByRole('option', { name: 'Browser workspace: league.csv' })).toBeInTheDocument();
    expect(within(selector()).queryByRole('option', { name: /sales\.csv/ })).not.toBeInTheDocument();

    await user.click(nav('Project Explorer'));
    expect(main().textContent).toContain('league.csv');
    expect(main().textContent).not.toMatch(/sales\.csv|Widget|region/);
    expect(within(main()).getByText('No analysis has been run yet.')).toBeInTheDocument();

    await user.click(nav('Overview Dashboard'));
    expect(within(main()).getByText('No filters: every row is used.')).toBeInTheDocument();
    expect(within(main()).getByText('Rows').nextElementSibling).toHaveTextContent('3');
  });

  it('removes the dataset on request and returns to the demo', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await user.click(nav('Overview Dashboard'));

    await user.click(within(main()).getByRole('button', { name: 'Remove dataset' }));
    expect(main().textContent).toContain('Overview: Browser Workspace'); // asks first
    await user.click(within(main()).getByRole('button', { name: 'Remove' }));

    expect(selector().value).toBe('release_impact');
    expect(within(selector()).queryByRole('option', { name: /sales\.csv/ })).not.toBeInTheDocument();
    expect(main().textContent).toContain('Registered Projects Repository');
    expect(main().textContent).not.toContain('sales.csv');
    await user.click(nav('Analyze Dataset'));
    expect(await screen.findByRole('button', { name: 'Upload a CSV' })).toBeInTheDocument();
  });

  it.each([
    ['an empty file', '', 'empty.csv', /This file is empty/],
    ['a file with a header and no rows', 'a,b,c\n', 'header_only.csv', /header row but no data rows/],
  ])('does not create a workspace from %s', async (_label, csv, name, message) => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(nav('Import & Profile Data'));
    await user.upload(screen.getByLabelText(/Click to upload your CSV/), new File([csv], name, { type: 'text/csv' }));
    expect(await screen.findByText(message)).toBeInTheDocument();
    expect(within(selector()).queryByRole('group', { name: /Browser workspace/ })).not.toBeInTheDocument();
    expect(selector().value).not.toBe(BROWSER_WORKSPACE_ID);
  });

  it('keeps the current workspace when a later file is rejected', async () => {
    const user = userEvent.setup();
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    const big = new File(['a,b\n1,2\n'], 'big.csv', { type: 'text/csv' });
    Object.defineProperty(big, 'size', { value: MAX_FILE_BYTES + 1 });
    await user.upload(screen.getByLabelText(/Click to upload your CSV/), big);
    expect(await screen.findByText(/limited to 10\.0 MB/)).toBeInTheDocument();
    expect(selector().value).toBe(BROWSER_WORKSPACE_ID);
    expect(within(selector()).getByRole('option', { name: 'Browser workspace: sales.csv' })).toBeInTheDocument();
  });

  it('does not keep the dataset anywhere beyond the page', async () => {
    const user = userEvent.setup();
    const setItem = vi.spyOn(Storage.prototype, 'setItem');
    render(<App />);
    await upload(user, SALES, 'sales.csv');
    await setUpAnalysis(user);
    const written = setItem.mock.calls.map(([key, value]) => `${key}=${value}`).join('\n');
    expect(written).not.toMatch(/sales|Widget|North/);
    expect(JSON.stringify(Object.entries(sessionStorage))).not.toMatch(/Widget|North/);
    expect(JSON.stringify(Object.entries(localStorage))).not.toMatch(/Widget|North/);
    expect(fetchMock).not.toHaveBeenCalled();
    setItem.mockRestore();
  });

  it('starts empty after a reload (a fresh render has no workspace)', async () => {
    const user = userEvent.setup();
    const first = render(<App />);
    await upload(user, SALES, 'sales.csv');
    first.unmount();

    render(<App />);
    expect(within(selector()).queryByRole('option', { name: /sales\.csv/ })).not.toBeInTheDocument();
    await waitFor(() => expect(selector().value).toBe('release_impact'));
  });
});
