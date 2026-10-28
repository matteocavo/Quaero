import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import App from '../App';
import { MAX_FILE_BYTES } from './limits';

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

const fetchMock = vi.fn();
const downloads: { name: string; blob: Blob }[] = [];
const blobs = new Map<string, Blob>();

beforeEach(() => {
  fetchMock.mockReset();
  downloads.length = 0;
  blobs.clear();
  vi.stubGlobal('fetch', fetchMock);
  URL.createObjectURL = vi.fn((blob: Blob) => {
    const url = `blob:mock/${blobs.size}`;
    blobs.set(url, blob);
    return url;
  });
  URL.revokeObjectURL = vi.fn();
  vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (this: HTMLAnchorElement) {
    downloads.push({ name: this.download, blob: blobs.get(this.href) as Blob });
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

const readBlob = (blob: Blob) =>
  new Promise<string>((resolve) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.readAsText(blob);
  });

const downloaded = async (name: string) => {
  const found = downloads.find((d) => d.name === name);
  expect(found, `no download named ${name}`).toBeDefined();
  return readBlob(found!.blob);
};

const nav = (label: string) =>
  within(document.querySelector('aside') as HTMLElement).getByRole('button', { name: new RegExp(label) });
const tab = (name: string) => screen.getByRole('tab', { name });

async function uploadCsv(user: User, csv: string, name = 'sales.csv') {
  await user.click(nav('Import & Profile Data'));
  await user.upload(screen.getByLabelText(/Click to upload your CSV/), new File([csv], name, { type: 'text/csv' }));
}

async function openWorkspace(user: User, csv = SALES, name = 'sales.csv') {
  await uploadCsv(user, csv, name);
  await user.click((await screen.findAllByRole('button', { name: 'Analyze This Dataset' }))[0]);
  await screen.findByRole('tablist', { name: 'Analysis workspace' });
}

/** Group by `region`, sum of `units`. */
async function buildSumByRegion(user: User) {
  await user.click(tab('Build Analysis'));
  await user.click(screen.getByRole('button', { name: 'Add grouping' }));
  await user.selectOptions(screen.getByLabelText('Calculation 1'), 'sum');
  await user.selectOptions(screen.getByLabelText('of column'), 'units');
}

describe('from an uploaded CSV to a downloaded analysis', () => {
  it('starts empty: nothing is preloaded before the user loads a file', async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(nav('Analyze Dataset'));
    expect(await screen.findByRole('button', { name: 'Upload a CSV' })).toBeInTheDocument();
    expect(screen.queryByRole('tablist', { name: 'Analysis workspace' })).not.toBeInTheDocument();
    expect(document.body.textContent).not.toContain('Alpha');
  });

  it('offers Analyze This Dataset right after the upload and opens the six-part workspace', async () => {
    const user = userEvent.setup();
    render(<App />);
    await uploadCsv(user, SALES);

    const ctas = await screen.findAllByRole('button', { name: 'Analyze This Dataset' });
    expect(ctas.length).toBeGreaterThanOrEqual(1);
    await user.click(ctas[0]);

    const names = screen.getAllByRole('tab').map((t) => t.textContent);
    expect(names).toEqual(['Data Preview', 'Data Quality', 'Explore & Filter', 'Build Analysis', 'Results & Charts', 'Export']);
    expect(screen.getByRole('heading', { name: 'sales.csv' })).toBeInTheDocument();
    expect(screen.getByText(/6 rows · 7 columns · your file/)).toBeInTheDocument();
    // the preview shows the uploaded rows
    expect(screen.getAllByText('Widget').length).toBeGreaterThan(0);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('builds an aggregation, shows it as a table and a chart, and exports exactly that', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await buildSumByRegion(user);

    expect(screen.getByText('Sum of units by region.')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Run analysis' }));

    // Results & Charts: computed from the uploaded rows
    expect(await screen.findByText('Sum of units by region.')).toBeInTheDocument();
    const table = screen.getByRole('table');
    const rows = within(table).getAllByRole('row').slice(1).map((row) => within(row).getAllByRole('cell').map((c) => c.textContent));
    expect(rows).toEqual([
      ['North', '8'],
      ['East', '7'],
      ['South', '3'],
    ]);
    expect(screen.getByRole('img', { name: /Sum of units by region/ })).toBeInTheDocument();
    expect(screen.getByText(/1 of 6 rows have no value in 'units'/)).toBeInTheDocument();

    // Export
    await user.click(tab('Export'));
    await user.click(screen.getByRole('button', { name: /Download results/ }));
    expect(await downloaded('sales_analysis.csv')).toBe('region,sum(units)\r\nNorth,8\r\nEast,7\r\nSouth,3');
    expect(screen.getByRole('status')).toHaveTextContent('Downloaded sales_analysis.csv');

    await user.click(screen.getByRole('button', { name: /Download report/ }));
    const report = JSON.parse(await downloaded('sales_analysis_report.json'));
    expect(report.analysis.description).toBe('Sum of units by region.');
    expect(report.dataset).toMatchObject({ file_name: 'sales.csv', origin: 'upload', rows: 6 });
    expect(report.result.rows).toEqual([
      { region: 'North', 'sum(units)': 8 },
      { region: 'East', 'sum(units)': 7 },
      { region: 'South', 'sum(units)': 3 },
    ]);

    await user.click(screen.getByRole('button', { name: /Download rows/ }));
    const filtered = await downloaded('sales_filtered.csv');
    expect(filtered.split('\r\n')).toHaveLength(7);
    expect(filtered.startsWith('order_id,region,product,units,price,order_date,returned')).toBe(true);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('filters by values and exports only the matching rows, in the file’s own format', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);

    await user.click(tab('Explore & Filter'));
    await user.selectOptions(screen.getByLabelText('Column', { selector: '#new-filter-column' }), 'region');
    await user.click(screen.getByRole('button', { name: 'Add filter' }));
    expect(screen.getByText(/Not applied yet: .*at least one value/)).toBeInTheDocument();
    await user.click(screen.getByRole('checkbox', { name: /North/ }));
    await user.click(screen.getByRole('checkbox', { name: /South/ }));

    await waitFor(() => expect(screen.getByText('Rows that match').nextElementSibling).toHaveTextContent('5'));

    await buildSumByRegion(user);
    await user.click(screen.getByRole('button', { name: 'Run analysis' }));
    const table = await screen.findByRole('table');
    const labels = within(table).getAllByRole('row').slice(1).map((row) => within(row).getAllByRole('cell')[0].textContent);
    expect(labels).toEqual(['North', 'South']);

    await user.click(tab('Export'));
    await user.click(screen.getByRole('button', { name: /Download rows/ }));
    const csv = await downloaded('sales_filtered.csv');
    expect(csv.split('\r\n')).toHaveLength(6);
    expect(csv).not.toContain('East');
    expect(csv).toContain('1,North,Widget,3,9.99,2024-01-05,no');
  });

  it('filters by a numeric range and a date range', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Explore & Filter'));

    await user.selectOptions(screen.getByLabelText('Column', { selector: '#new-filter-column' }), 'units');
    await user.click(screen.getByRole('button', { name: 'Add filter' }));
    await user.type(screen.getByLabelText('Minimum'), '3');
    await waitFor(() => expect(screen.getByText('Rows that match').nextElementSibling).toHaveTextContent('3'));

    await user.selectOptions(screen.getByLabelText('Column', { selector: '#new-filter-column' }), 'order_date');
    await user.click(screen.getByRole('button', { name: 'Add filter' }));
    await user.type(screen.getByLabelText('From'), '2024-02-01');
    await waitFor(() => expect(screen.getByText('Rows that match').nextElementSibling).toHaveTextContent('1'));
  });

  it('groups a date column per month and offers a line chart', async () => {
    const user = userEvent.setup();
    const { container } = render(<App />);
    await openWorkspace(user);
    await user.click(tab('Build Analysis'));
    await user.click(screen.getByRole('button', { name: 'Add grouping' }));
    await user.selectOptions(screen.getByLabelText('Group 1: column'), 'order_date');
    await user.selectOptions(screen.getByLabelText('Date detail'), 'month');
    await user.selectOptions(screen.getByLabelText('Calculation 1'), 'sum');
    await user.selectOptions(screen.getByLabelText('of column'), 'units');
    await user.click(screen.getByRole('button', { name: 'Run analysis' }));

    await screen.findByRole('table');
    expect(container.querySelector('svg[data-chart="line"]')).not.toBeNull();
    const months = within(screen.getByRole('table')).getAllByRole('row').slice(1).map((r) => within(r).getAllByRole('cell')[0].textContent);
    expect(months).toEqual(['2023-12', '2024-01', '2024-02', '2024-03']);
  });

  it('starts from a pattern built on the real columns and disables patterns the file cannot support', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user, 'name,score\nann,1\nbob,5\nann,3\n', 'scores.csv');
    await user.click(tab('Build Analysis'));

    // no date column in this file
    const time = screen.getByRole('button', { name: /See how a value changes over time/ });
    expect(time).toBeDisabled();
    expect(within(time).getByText(/needs a date column/)).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /Compare a value across groups/ }));
    expect(screen.getByText('Average of score by name, sorted by avg(score) (highest first).')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Run analysis' }));
    const table = await screen.findByRole('table');
    const rows = within(table).getAllByRole('row').slice(1).map((row) => within(row).getAllByRole('cell').map((c) => c.textContent));
    expect(rows).toEqual([
      ['bob', '5'],
      ['ann', '2'],
    ]);
  });

  it('refuses impossible calculations in plain words and does not run them', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Build Analysis'));
    await user.selectOptions(screen.getByLabelText('Calculation 1'), 'avg');
    await user.selectOptions(screen.getByLabelText('of column'), 'order_id');
    expect(screen.getByRole('button', { name: 'Run analysis' })).toBeEnabled();
    // a text column cannot be summed: only numeric columns are even offered
    const options = within(screen.getByLabelText('of column')).getAllByRole('option').map((o) => o.textContent);
    expect(options).not.toContain('region');
    expect(options).toContain('units');
  });

  it('keeps export of results disabled until an analysis has been run', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Export'));
    expect(screen.getByRole('button', { name: /Download results/ })).toBeDisabled();
    expect(screen.getByRole('button', { name: /Download report/ })).toBeDisabled();
    expect(screen.getByRole('button', { name: /Download rows/ })).toBeEnabled();
    await user.click(tab('Results & Charts'));
    expect(screen.getByText('No results yet')).toBeInTheDocument();
  });

  it('shows an honest empty result when the filters match nothing', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Explore & Filter'));
    await user.selectOptions(screen.getByLabelText('Column', { selector: '#new-filter-column' }), 'units');
    await user.click(screen.getByRole('button', { name: 'Add filter' }));
    await user.type(screen.getByLabelText('Minimum'), '999');
    expect(await screen.findByText(/No rows match the filters/)).toBeInTheDocument();

    await buildSumByRegion(user);
    await user.click(screen.getByRole('button', { name: 'Run analysis' }));
    expect(await screen.findByText(/There are no groups to show/)).toBeInTheDocument();
    expect(screen.getByText(/No rows match the filters, so there is nothing to calculate/)).toBeInTheDocument();
  });
});

describe('what happens with unusable files', () => {
  it('explains an empty file', async () => {
    const user = userEvent.setup();
    render(<App />);
    await uploadCsv(user, '', 'empty.csv');
    expect(await screen.findByText(/This file is empty \(0 bytes\)/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Analyze This Dataset' })).not.toBeInTheDocument();
  });

  it('explains a file with a header and no data, and keeps the dataset that was already loaded', async () => {
    const user = userEvent.setup();
    render(<App />);
    await uploadCsv(user, SALES, 'sales.csv');
    await screen.findAllByRole('button', { name: 'Analyze This Dataset' });

    await user.upload(screen.getByLabelText(/Click to upload your CSV/), new File(['a,b,c\n'], 'header_only.csv', { type: 'text/csv' }));
    expect(await screen.findByText(/header row but no data rows/)).toBeInTheDocument();

    await user.click(nav('Analyze Dataset'));
    expect(await screen.findByRole('heading', { name: 'sales.csv' })).toBeInTheDocument();
  });

  it('explains a file that is too large before reading it', async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(nav('Import & Profile Data'));
    const big = new File(['a,b\n1,2\n'], 'big.csv', { type: 'text/csv' });
    Object.defineProperty(big, 'size', { value: MAX_FILE_BYTES + 1 });
    await user.upload(screen.getByLabelText(/Click to upload your CSV/), big);
    const message = await screen.findByText(/limited to 10\.0 MB/);
    expect(message.textContent).toContain('run Quaero locally');
    expect(screen.queryByRole('button', { name: 'Analyze This Dataset' })).not.toBeInTheDocument();
  });

  it('rejects files that are not text tables', async () => {
    const user = userEvent.setup({ applyAccept: false });
    render(<App />);
    await user.click(nav('Import & Profile Data'));
    const input = screen.getByLabelText(/Click to upload your CSV/);
    await user.upload(input, new File(['x'], 'book.xlsx', { type: 'application/vnd.ms-excel' }));
    expect(await screen.findByText(/Excel workbooks must be exported to CSV first/)).toBeInTheDocument();
  });
});

describe('switching datasets', () => {
  it('discards the previous analysis, filters and results and uses only the new data', async () => {
    const user = userEvent.setup();
    render(<App />);
    await openWorkspace(user);
    await user.click(tab('Explore & Filter'));
    await user.selectOptions(screen.getByLabelText('Column', { selector: '#new-filter-column' }), 'region');
    await user.click(screen.getByRole('button', { name: 'Add filter' }));
    await user.click(screen.getByRole('checkbox', { name: /North/ }));
    await buildSumByRegion(user);
    await user.click(screen.getByRole('button', { name: 'Run analysis' }));
    await screen.findByRole('table');

    // load a different file
    await uploadCsv(user, 'team,points\nred,10\nblue,4\nred,6\n', 'league.csv');
    await user.click((await screen.findAllByRole('button', { name: 'Analyze This Dataset' }))[0]);

    expect(screen.getByRole('heading', { name: 'league.csv' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Data Preview', selected: true })).toBeInTheDocument();
    await user.click(tab('Results & Charts'));
    expect(screen.getByText('No results yet')).toBeInTheDocument();

    await user.click(tab('Explore & Filter'));
    expect(screen.getByText(/No filters: every row is used/)).toBeInTheDocument();
    await user.click(tab('Build Analysis'));
    expect(screen.getByText('Number of rows.')).toBeInTheDocument();
    const columns = within(screen.getByLabelText('of column')).queryAllByRole('option').map((o) => o.textContent);
    expect(columns).not.toContain('region');
    expect(document.body.textContent).not.toContain('Widget');
  });

  it('a bundled sample behaves like any other dataset and is labelled as a sample', async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(nav('Import & Profile Data'));
    await user.click(screen.getByRole('button', { name: /Sample: release_impact/ }));
    await user.click((await screen.findAllByRole('button', { name: 'Analyze This Dataset' }))[0]);
    expect(screen.getByText(/bundled sample/)).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
